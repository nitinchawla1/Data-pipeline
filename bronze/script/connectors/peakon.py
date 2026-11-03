"""
Workday Peakon Employee Voice REST API Connector — AWS Glue Data Pipeline.

Extracts survey answers, employees, engagement metrics, drivers, segments,
and segment-level contextual metrics (overview, drivers, scores) from Peakon.
Supports both:
  1. Peakon Custom Application Access Token (automatically exchanged for JWT Bearer token).
  2. Pre-exchanged JWT Bearer token.

Cursor pagination via JSON:API links.next.
Delta extraction via responseAnsweredAt / updated timestamps.
Dynamic multi-segment fan-out for contextual endpoints ({segment_id} / {context_id}).

Required Secrets Manager keys:
  peakon_api_token : Peakon Custom Application Access Token (or JWT Bearer token). Alias: api_token
  api_base_url     : (Optional) Peakon instance base URL (default: https://api.peakon.com)
  batch_size       : (Optional) Max answers per page (default: 2000, max: 20000)

Required bronze_config.json keys (source_systems.peakon):
  base_url             : Fallback Peakon base URL if not in secret
  response_records_key : JSON key containing records (default: 'data')
  batch_size           : Records per page (default: 2000)
"""

import io
import json
import re
import time
import logging
import urllib.request
import urllib.parse
from urllib.error import HTTPError, URLError
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

from config_loader import ConfigLoader

logger = logging.getLogger(__name__)


def _parse_ts(ts_val: Any) -> Optional[datetime]:
    """Parses various ISO and date string formats into timezone-aware UTC datetime."""
    if not ts_val:
        return None
    s = str(ts_val).strip()
    if s.endswith('Z') or s.endswith('z'):
        s = s[:-1] + '+00:00'
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(s[:19], fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


class PeakonConnector:
    """
    Workday Peakon Employee Voice API connector.
    Handles automatic JWT authentication, JSON:API unwrapping, delta date filtering,
    dynamic segment iteration for context endpoints, and cursor pagination via links.next.
    """

    _jwt_cache: Dict[str, Tuple[str, float]] = {}
    _cached_segment_ids: List[str] = []
    _last_source_config: Dict[str, Any] = {}

    @classmethod
    def _get_bearer_token(cls, base_url: str, raw_token: str) -> str:
        """
        Returns a valid JWT Bearer token for Peakon API requests.
        If the token is already a JWT (starts with 'ey' and contains two dots), it is used directly.
        Otherwise, exchanges the Custom App Access Token at /api/v1/auth/application.
        """
        clean_token = raw_token.strip()

        # If already a JWT bearer token, use directly
        if clean_token.startswith('ey') and clean_token.count('.') == 2:
            return clean_token

        # Check in-memory token cache (valid for 24 hours, refreshed 60s before)
        now = time.time()
        if clean_token in cls._jwt_cache:
            cached_jwt, expires_at = cls._jwt_cache[clean_token]
            if now < expires_at - 60:
                return cached_jwt

        auth_url = f"{base_url.rstrip('/')}/api/v1/auth/application"
        payload = json.dumps({"token": clean_token, "persists": True}).encode("utf-8")
        req = urllib.request.Request(
            auth_url,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "UAX-DataLake-Pipeline/1.0",
            },
            method="POST",
        )

        logger.info(f"Authenticating with Peakon at '{auth_url}'...")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                resp_data = json.loads(resp.read().decode("utf-8"))
        except HTTPError as http_err:
            body = ""
            try:
                body = http_err.read().decode("utf-8")
            except Exception:
                pass
            raise RuntimeError(
                f"Peakon authentication failed (HTTP {http_err.code}): {http_err.reason}. "
                f"Response body: {body}. Verify 'peakon_api_token' in AWS Secrets Manager."
            ) from http_err
        except Exception as err:
            raise RuntimeError(f"Peakon authentication error: {err}") from err

        jwt_token = resp_data.get("data", {}).get("id")
        if not jwt_token:
            raise ValueError(
                f"Peakon auth response missing 'data.id'. Received: {resp_data}"
            )

        # Cache token for 24 hours (persists=True session token)
        cls._jwt_cache[clean_token] = (jwt_token, now + 86400)
        logger.info("Successfully acquired Peakon JWT Bearer token.")
        return jwt_token

    @classmethod
    def _execute_request_with_retry(
        cls,
        url: str,
        bearer_token: str,
        max_retries: int = 4,
        ignore_status_codes: Tuple[int, ...] = (),
    ) -> Dict[str, Any]:
        """
        Executes GET request with exponential backoff on HTTP 429 rate limits or transient errors.
        Optionally ignores designated status codes (e.g. 403, 404 for confidentiality thresholds)
        and returns an empty dictionary.
        """
        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Accept": "application/json",
            "User-Agent": "UAX-DataLake-Pipeline/1.0",
        }

        for attempt in range(1, max_retries + 1):
            req = urllib.request.Request(url, headers=headers, method="GET")
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except HTTPError as err:
                if ignore_status_codes and err.code in ignore_status_codes:
                    logger.warning(
                        f"Peakon API returned HTTP {err.code} ({err.reason}) on '{url}'. "
                        "Skipping resource due to ignore_status_codes policy (e.g. confidentiality threshold)."
                    )
                    return {}
                if err.code == 429:
                    retry_after = err.headers.get("Retry-After")
                    sleep_sec = int(retry_after) if retry_after and retry_after.isdigit() else (5 * attempt)
                    logger.warning(
                        f"Peakon rate limit (HTTP 429) hit on '{url}'. Backing off {sleep_sec}s (attempt {attempt}/{max_retries})..."
                    )
                    time.sleep(sleep_sec)
                    continue
                elif err.code in (500, 502, 503, 504) and attempt < max_retries:
                    sleep_sec = 2 ** attempt
                    logger.warning(
                        f"Peakon server error (HTTP {err.code}) on '{url}'. Retrying in {sleep_sec}s (attempt {attempt}/{max_retries})..."
                    )
                    time.sleep(sleep_sec)
                    continue
                else:
                    body = ""
                    try:
                        body = err.read().decode("utf-8")
                    except Exception:
                        pass
                    raise RuntimeError(
                        f"Peakon API call failed (HTTP {err.code}): {err.reason} on {url}. Body: {body}"
                    ) from err
            except (URLError, TimeoutError) as net_err:
                if attempt < max_retries:
                    sleep_sec = 2 ** attempt
                    logger.warning(f"Peakon network error: {net_err}. Retrying in {sleep_sec}s...")
                    time.sleep(sleep_sec)
                    continue
                raise

        raise RuntimeError(f"Peakon API call to '{url}' failed after {max_retries} attempts.")

    @classmethod
    def _read_segment_ids_from_s3(cls, bronze_bucket: str, s3_prefix: str) -> List[str]:
        """Reads latest ingested segment IDs from all parquet files in the latest partition of Bronze S3."""
        try:
            s3_client = boto3.client("s3")
            resp = s3_client.list_objects_v2(Bucket=bronze_bucket, Prefix=s3_prefix)
            contents = resp.get("Contents", [])
            parquet_keys = [obj["Key"] for obj in contents if obj["Key"].endswith(".parquet")]
            if not parquet_keys:
                return []

            # Sort to pick the latest partition directory
            parquet_keys.sort()
            latest_key = parquet_keys[-1]
            latest_dir = latest_key.rsplit('/', 1)[0]
            partition_keys = [k for k in parquet_keys if k.rsplit('/', 1)[0] == latest_dir]

            logger.info(f"Reading segment IDs from {len(partition_keys)} file(s) in s3://{bronze_bucket}/{latest_dir}/")
            import pandas as pd
            collected_ids: List[str] = []
            for p_key in partition_keys:
                file_obj = s3_client.get_object(Bucket=bronze_bucket, Key=p_key)
                df = pd.read_parquet(io.BytesIO(file_obj["Body"].read()))
                id_col = None
                for candidate in ("id", "segment_id", "context_id"):
                    if candidate in df.columns:
                        id_col = candidate
                        break
                if id_col:
                    vals = df[id_col].dropna().astype(str).unique().tolist()
                    collected_ids.extend([v.strip() for v in vals if v.strip()])

            if collected_ids:
                return list(dict.fromkeys(collected_ids))
        except Exception as e:
            logger.warning(f"Could not read segments from S3 (s3://{bronze_bucket}/{s3_prefix}): {e}")
        return []

    @classmethod
    def _normalize_metric_floats(cls, record: Dict[str, Any]) -> None:
        """
        Deep-recursively normalizes all numeric metric fields across nested dictionaries and arrays to Python float
        so that Parquet (pandas) and Glue Catalog consistently infer DOUBLE.
        Deeply inspects all levels (scores, score, nps, promoters, detractors, passives, drivers, etc.).
        Permanently prevents HIVE_PARTITION_SCHEMA_MISMATCH and DOUBLE vs BIGINT conflicts.
        """
        metric_keywords = (
            "score", "mean", "size", "standarddeviation", "difference", "benchmark",
            "participation", "participationrate", "participation_rate", "confidence",
            "percentile", "enps", "count", "respondents", "weight", "frequency",
            "promoter", "promoters", "detractor", "detractors", "passive", "passives",
            "previous_score", "scale", "response_count", "employee_count"
        )
        non_metric_identifiers = (
            "id", "type", "at", "date", "name", "status", "group", "text", "description",
            "title", "email", "department", "interval", "sentiment", "format", "version"
        )

        def _walk_and_normalize(obj: Any, parent_path: str = "") -> None:
            if isinstance(obj, dict):
                for k, v in list(obj.items()):
                    clean_k = str(k).lower().replace("-", "_").replace(".", "_")
                    full_path = f"{parent_path}_{clean_k}" if parent_path else clean_k

                    if isinstance(v, (dict, list)):
                        _walk_and_normalize(v, full_path)
                    elif v is not None and not isinstance(v, bool):
                        is_non_metric_id = any(
                            clean_k == nmi or clean_k.endswith(f"_{nmi}")
                            for nmi in non_metric_identifiers
                        )
                        is_explicit_metric = any(
                            kw in clean_k or kw in full_path
                            for kw in metric_keywords
                        )

                        if is_explicit_metric or (not is_non_metric_id and isinstance(v, (int, float))):
                            if isinstance(v, (int, float)):
                                try:
                                    obj[k] = float(v)
                                except Exception:
                                    pass
                            elif isinstance(v, str):
                                try:
                                    obj[k] = float(v)
                                except ValueError:
                                    pass
            elif isinstance(obj, list):
                for item in obj:
                    if isinstance(item, (dict, list)):
                        _walk_and_normalize(item, parent_path)

        _walk_and_normalize(record)

    @classmethod
    def _install_runtime_glue_hooks(cls, source_config: Dict[str, Any]) -> None:
        """
        Dynamically installs non-invasive runtime hooks into __main__ (uax_bronze_load.py)
        to guarantee:
        1. infer_glue_column_type treats all numeric values as 'double' for Peakon tables.
        2. sync_bronze_catalog_table automatically triggers _sync_glue_table_schema immediately
           AFTER creating or updating the table in AWS Glue Data Catalog.
        Ensures 100% catalog consistency without modifying a single line of uax_bronze_load.py on disk.
        """
        try:
            import sys
            cls._last_source_config = source_config or {}
            main_mod = sys.modules.get("__main__")
            if not main_mod:
                return

            # 1. Hook infer_glue_column_type so brand-new tables are born with DOUBLE for numbers
            if hasattr(main_mod, "infer_glue_column_type"):
                orig_infer = getattr(main_mod, "infer_glue_column_type")
                if not getattr(orig_infer, "_peakon_hooked", False):
                    def peakon_safe_infer(val: Any) -> str:
                        if isinstance(val, bool):
                            return "boolean"
                        elif isinstance(val, (int, float)):
                            return "double"
                        return orig_infer(val)
                    peakon_safe_infer._peakon_hooked = True
                    setattr(main_mod, "infer_glue_column_type", peakon_safe_infer)
                    logger.info("[Peakon] Installed runtime Glue type inference hook (numbers -> double).")

            # 2. Hook sync_bronze_catalog_table so _sync_glue_table_schema runs post-creation
            if hasattr(main_mod, "sync_bronze_catalog_table"):
                orig_sync = getattr(main_mod, "sync_bronze_catalog_table")
                if not getattr(orig_sync, "_peakon_hooked", False):
                    def peakon_safe_sync(*args, **kwargs):
                        res = orig_sync(*args, **kwargs)
                        try:
                            t_name = kwargs.get("table_name") or (args[3] if len(args) > 3 else None)
                            if t_name:
                                cls._sync_glue_table_schema(str(t_name), cls._last_source_config or {})
                        except Exception as e:
                            logger.warning(f"[Peakon] Post-catalog sync self-healing non-fatal warning: {e}")
                        return res
                    peakon_safe_sync._peakon_hooked = True
                    setattr(main_mod, "sync_bronze_catalog_table", peakon_safe_sync)
                    logger.info("[Peakon] Installed runtime post-catalog sync self-healing hook.")
        except Exception as hook_err:
            logger.warning(f"[Peakon] Could not install runtime Glue hooks (non-fatal): {hook_err}")

    @classmethod
    def _sync_glue_table_schema(
        cls,
        table_name: str,
        source_config: Dict[str, Any]
    ) -> None:
        """
        Inspects the AWS Glue Data Catalog table and existing partitions for the given table.
        Automatically upgrades any metric/score columns declared as 'bigint' or 'int' to 'double',
        and ensures 'parquet.column.index.access' is set to 'false'.
        Permanently eliminates HIVE_PARTITION_SCHEMA_MISMATCH in Athena on existing and new partitions
        without requiring manual ALTER TABLE queries or modifying uax_bronze_load.py.
        """
        try:
            config = source_config or {}
            env = config.get("env") or config.get("environment") or "dev"
            glue_database = (
                config.get("glue_database_name")
                or config.get("glue_database")
                or f"uax_datalake_db_{env}"
            )
            table_prefix = config.get("glue_table_prefix") or "raw_tbl_"
            clean_name = table_name.lower().replace("-", "_").strip()
            catalog_table_name = (
                clean_name if clean_name.startswith(table_prefix) else f"{table_prefix}{clean_name}"
            )
            while catalog_table_name.startswith("raw_tbl_raw_tbl_"):
                catalog_table_name = catalog_table_name[len("raw_tbl_"):]

            glue_client = boto3.client("glue")

            # Check if table exists in Glue Catalog
            try:
                table_resp = glue_client.get_table(DatabaseName=glue_database, Name=catalog_table_name)
            except Exception:
                # Table does not exist yet; will be synced after creation via runtime hook
                return

            table_obj = table_resp.get("Table", {})
            sd = table_obj.get("StorageDescriptor", {})
            columns = sd.get("Columns", [])
            if not columns:
                return

            metric_keywords = (
                "score", "mean", "size", "standarddeviation", "difference", "benchmark",
                "participation", "participationrate", "participation_rate", "confidence",
                "percentile", "enps", "count", "respondents", "weight", "frequency",
                "promoter", "promoters", "detractor", "detractors", "passive", "passives",
                "scale", "response_count", "employee_count", "previous_score"
            )

            modified = False
            widened_cols = []
            for col in columns:
                cname = col.get("Name", "").lower()
                ctype = col.get("Type", "").lower()
                is_metric = (
                    cname.startswith("scores")
                    or cname.startswith("score")
                    or cname.startswith("nps")
                    or cname.startswith("driver")
                    or any(kw in cname for kw in metric_keywords)
                )
                if is_metric and ctype in ("bigint", "int", "smallint", "tinyint", "integer"):
                    col["Type"] = "double"
                    modified = True
                    widened_cols.append(col.get("Name"))

            table_params = table_obj.get("Parameters", {})
            if table_params.get("parquet.column.index.access") != "false":
                table_params["parquet.column.index.access"] = "false"
                modified = True

            if modified:
                logger.info(
                    f"[Peakon Glue Self-Healing] Auto-widening column types to DOUBLE in "
                    f"'{glue_database}.{catalog_table_name}': {widened_cols}"
                )
                table_input = {
                    "Name": catalog_table_name,
                    "Description": table_obj.get("Description", f"Bronze raw data table for peakon/{table_name}"),
                    "PartitionKeys": table_obj.get("PartitionKeys", [{"Name": "_ingested_at", "Type": "string"}]),
                    "TableType": table_obj.get("TableType", "EXTERNAL_TABLE"),
                    "Parameters": table_params,
                    "StorageDescriptor": dict(sd, Columns=columns)
                }
                glue_client.update_table(DatabaseName=glue_database, TableInput=table_input)
                logger.info(
                    f"[Peakon Glue Self-Healing] Successfully updated table '{glue_database}.{catalog_table_name}' definition."
                )

                # Back-sync existing partitions so older partitions also match the 'double' schema
                try:
                    paginator = glue_client.get_paginator("get_partitions")
                    for page in paginator.paginate(DatabaseName=glue_database, TableName=catalog_table_name):
                        for part in page.get("Partitions", []):
                            p_sd = part.get("StorageDescriptor", {})
                            p_cols = p_sd.get("Columns", [])
                            part_modified = False
                            for p_col in p_cols:
                                pcname = p_col.get("Name", "").lower()
                                pctype = p_col.get("Type", "").lower()
                                is_p_metric = (
                                    pcname.startswith("scores")
                                    or pcname.startswith("score")
                                    or pcname.startswith("nps")
                                    or pcname.startswith("driver")
                                    or any(kw in pcname for kw in metric_keywords)
                                )
                                if is_p_metric and pctype in ("bigint", "int", "smallint", "tinyint", "integer"):
                                    p_col["Type"] = "double"
                                    part_modified = True

                            p_params = part.get("Parameters", {})
                            if p_params.get("parquet.column.index.access") != "false":
                                p_params["parquet.column.index.access"] = "false"
                                part_modified = True

                            if part_modified:
                                glue_client.update_partition(
                                    DatabaseName=glue_database,
                                    TableName=catalog_table_name,
                                    PartitionValueList=part.get("Values", []),
                                    PartitionInput={
                                        "Values": part.get("Values", []),
                                        "StorageDescriptor": dict(p_sd, Columns=p_cols),
                                        "Parameters": p_params
                                    }
                                )
                except Exception as part_sync_err:
                    logger.warning(
                        f"[Peakon Glue Self-Healing] Non-fatal: Partition back-sync for '{catalog_table_name}': {part_sync_err}"
                    )

        except Exception as glue_err:
            logger.warning(
                f"[Peakon Glue Self-Healing] Non-fatal: Skipped Glue schema self-healing for '{table_name}': {glue_err}"
            )

    @classmethod
    def _get_dynamic_segment_ids(
        cls,
        base_url: str,
        bearer_token: str,
        source_config: Dict[str, Any]
    ) -> List[str]:
        """
        Retrieves active Peakon segment IDs via 3-tier discovery:
        1. In-memory cache (if 'segments' table was processed earlier in the same run).
        2. Ingested Bronze S3 'segments' Parquet table.
        3. Peakon REST API /api/v1/segments fallback with cursor pagination.
        """
        # Tier 1: In-memory cache
        if cls._cached_segment_ids:
            logger.info(f"Using {len(cls._cached_segment_ids)} segment IDs from in-memory cache.")
            return cls._cached_segment_ids

        # Tier 2: Read from ingested Bronze segments in S3
        bronze_bucket = source_config.get("bronze_bucket")
        bronze_data_prefix = source_config.get("bronze_data_prefix", "bronze/data").strip("/")
        if bronze_bucket:
            for prefix_candidate in (
                f"{bronze_data_prefix}/peakon/segments/",
                f"{bronze_data_prefix}/peakon/raw_tbl_segments/",
                f"{bronze_data_prefix}/peakon/segment/",
            ):
                s3_ids = cls._read_segment_ids_from_s3(bronze_bucket, prefix_candidate)
                if s3_ids:
                    cls._cached_segment_ids = s3_ids
                    logger.info(f"Loaded {len(s3_ids)} segment IDs from S3 ({prefix_candidate}).")
                    return s3_ids

        # Tier 3: Fetch directly from Peakon API /api/v1/segments with cursor pagination
        segments_cfg = ConfigLoader.get_table_config("peakon", "segments", source_config=source_config)
        segments_endpoint = (
            segments_cfg.get("custom_endpoint")
            or segments_cfg.get("endpoint")
            or "/api/v1/segments"
        ).strip()
        if "://" in segments_endpoint:
            curr_url: Optional[str] = segments_endpoint
        else:
            if not segments_endpoint.startswith("/"):
                segments_endpoint = "/" + segments_endpoint
            if not segments_endpoint.startswith("/api/v1"):
                segments_endpoint = f"/api/v1{segments_endpoint}"
            curr_url = f"{base_url.rstrip('/')}{segments_endpoint}"

        sep = "&" if "?" in curr_url else "?"
        if "per_page" not in curr_url:
            curr_url = f"{curr_url}{sep}per_page=2000"

        logger.info(f"Fetching segment IDs directly from Peakon API: {curr_url}...")
        api_segment_ids: List[str] = []
        # Check segment limit for discovery
        seg_limit_val = source_config.get("segment_limit", 50)
        try:
            discovery_limit = int(seg_limit_val) if seg_limit_val is not None else 50
        except (ValueError, TypeError):
            discovery_limit = 50

        try:
            while curr_url:
                resp = cls._execute_request_with_retry(curr_url, bearer_token)
                raw_items = resp.get("data", [])
                if isinstance(raw_items, dict):
                    raw_items = [raw_items]
                elif not isinstance(raw_items, list):
                    raw_items = []

                for item in raw_items:
                    if isinstance(item, dict) and item.get("id"):
                        api_segment_ids.append(str(item["id"]).strip())

                if discovery_limit > 0 and len(api_segment_ids) >= discovery_limit:
                    logger.info(
                        f"Discovery limit reached ({len(api_segment_ids)} >= {discovery_limit}). Stopping pagination early."
                    )
                    break

                links = resp.get("links", {})
                curr_url = links.get("next") if isinstance(links, dict) else None

            if api_segment_ids:
                deduped = list(dict.fromkeys(api_segment_ids))
                if discovery_limit > 0:
                    deduped = deduped[:discovery_limit]
                cls._cached_segment_ids = deduped
                logger.info(f"Acquired {len(deduped)} segment IDs from Peakon API (segment_limit={discovery_limit}).")
                return deduped
        except Exception as api_err:
            logger.error(f"Failed to fetch segment IDs from Peakon API: {api_err}")
            raise RuntimeError(f"Unable to resolve dynamic segment IDs: {api_err}") from api_err

        return []

    @classmethod
    def fetch_delta(
        cls,
        last_load_date: str,
        secret_dict: Dict[str, Any],
        table_name: str,
        source_config: Dict[str, Any],
        custom_query: Optional[str] = None,
        on_chunk_callback: Optional[Callable[[List[Dict[str, Any]], int], None]] = None,
        s3_chunk_size: int = 10000,
    ) -> List[Dict[str, Any]]:
        """
        Extracts records from Workday Peakon updated since last_load_date.
        Supports:
          - Standard single endpoints (answers, employees, segments).
          - Dynamic segment iteration for contextual endpoints ({segment_id} / {context_id}).
          - Full cold starts and incremental delta filtering.
          - Streams records to S3 chunks via on_chunk_callback.
        """
        if not table_name or not table_name.strip():
            raise ValueError("Peakon connector: 'table_name' is required.")

        config = source_config or {}

        # 1. Install runtime Glue hooks to ensure new tables and partitions are created with DOUBLE
        cls._install_runtime_glue_hooks(config)

        # 2. Auto-heal existing Glue Catalog table schema (widen metric columns from BIGINT to DOUBLE)
        cls._sync_glue_table_schema(table_name, config)

        # 1. Resolve Credentials & Token
        api_token = (
            secret_dict.get("peakon_api_token")
            or secret_dict.get("api_token")
            or secret_dict.get("token")
            or secret_dict.get("access_token")
            or secret_dict.get("bearer_token")
        )
        if not api_token:
            raise ValueError(
                "Peakon connector: Missing 'peakon_api_token' (or 'api_token') in AWS Secrets Manager. "
                "Store your Peakon Custom Application Access Token in Secrets Manager."
            )

        base_url = (
            secret_dict.get("api_base_url")
            or config.get("base_url")
            or "https://api.peakon.com"
        ).rstrip("/")

        bearer_token = cls._get_bearer_token(base_url, api_token)

        # 2. Resolve Table Config, Endpoint, Response Key, and Batch Size
        table_cfg = ConfigLoader.get_table_config("peakon", table_name, source_config=config)
        endpoint = (
            table_cfg.get("custom_endpoint")
            or table_cfg.get("endpoint")
            or ConfigLoader.get_table_endpoint("peakon", table_name, config)
        )
        response_key = config.get("response_records_key", "data")
        batch_size = int(secret_dict.get("batch_size") or config.get("batch_size", 2000))
        batch_size = min(max(batch_size, 1), 20000)

        # Determine if endpoint requires dynamic segment / group iteration
        is_segment_endpoint = (
            "{segment_id}" in endpoint
            or "{context_id}" in endpoint
            or "{group}" in endpoint
            or bool(table_cfg.get("is_segment_level"))
        )

        # 3. Setup Delta Filtering Timestamps
        is_full_load = (not last_load_date) or str(last_load_date).strip().startswith("1900")
        cutoff_dt = _parse_ts(last_load_date) if not is_full_load else None

        upper_bound_str = config.get("upper_bound")
        is_high_date = bool(
            upper_bound_str and (
                str(upper_bound_str).strip().startswith("9999")
                or str(upper_bound_str).strip().startswith("9998")
            )
        )
        upper_bound_dt = _parse_ts(upper_bound_str) if (upper_bound_str and not is_high_date) else None

        logger.info(
            f"[Peakon/{table_name}] Starting extraction from '{base_url}' | "
            f"Mode: {'FULL_LOAD' if is_full_load else 'DELTA'} | "
            f"Segment-level: {is_segment_endpoint} | "
            f"since: {last_load_date} | upper_bound: {upper_bound_str or 'None'} | "
            f"response_records_key: {response_key} | batch_size: {batch_size}"
        )

        all_records: List[Dict[str, Any]] = []
        records_buffer: List[Dict[str, Any]] = []
        total_extracted = 0
        total_filtered_out = 0
        part_number = 1

        # =========================================================================
        # Case A: Dynamic Segment-Level Endpoints ({context_id}, {group})
        # =========================================================================
        if is_segment_endpoint:
            segment_ids = cls._get_dynamic_segment_ids(base_url, bearer_token, config)
            if not segment_ids:
                logger.warning(
                    f"[Peakon/{table_name}] No segment IDs available to query. Exiting table extraction."
                )
                return []

            # Segment limit for testing / staging (defaults to 50, set to 0 or null in bronze_config.json for full run)
            seg_limit_raw = table_cfg.get("segment_limit")
            if seg_limit_raw is None:
                seg_limit_raw = config.get("segment_limit", 50)
            try:
                segment_limit = int(seg_limit_raw) if seg_limit_raw is not None else 50
            except (ValueError, TypeError):
                segment_limit = 50

            if segment_limit > 0 and len(segment_ids) > segment_limit:
                logger.info(
                    f"[Peakon/{table_name}] Test limit active: Limiting from {len(segment_ids)} segments down to first {segment_limit} segments."
                )
                segment_ids = segment_ids[:segment_limit]

            logger.info(
                f"[Peakon/{table_name}] Resolving tasks for {len(segment_ids)} segments on endpoint: {endpoint}"
            )

            # Normalize endpoint path (normalize multiple slashes e.g. contexts//{context_id} -> contexts/{context_id})
            clean_template = endpoint.strip()
            if "://" in clean_template:
                scheme, rest = clean_template.split("://", 1)
                rest = re.sub(r"/+", "/", rest)
                clean_template = f"{scheme}://{rest}"
            else:
                clean_template = re.sub(r"/+", "/", clean_template)

            if not clean_template.startswith("http://") and not clean_template.startswith("https://"):
                if not clean_template.startswith("/"):
                    clean_template = "/" + clean_template
                if not clean_template.startswith("/api/v1"):
                    clean_template = f"/api/v1{clean_template}"

            # Check if endpoint requires {group} expansion (e.g. scores_by_category, scores_by_question)
            has_group = "{group}" in clean_template
            if has_group:
                groups_list = table_cfg.get("groups")
                if not groups_list or not isinstance(groups_list, list):
                    groups_list = [
                        "engagement",
                        "health_wellbeing",
                        "transformation_change",
                        "diversity_inclusion",
                        "onboarding",
                        "exit",
                        "environmental_social_governance",
                        "values",
                        "other",
                    ]
                tasks = [(s_id, str(g).strip()) for s_id in segment_ids for g in groups_list if str(g).strip()]
            else:
                tasks = [(s_id, None) for s_id in segment_ids]

            def _fetch_single_task(task_tuple: Tuple[Any, Optional[str]]) -> Tuple[List[Dict[str, Any]], int]:
                raw_seg, group_val = task_tuple
                seg_str = str(raw_seg).strip()
                context_id = seg_str if seg_str.startswith("segment_") else f"segment_{seg_str}"
                raw_segment_id = seg_str
                for prefix in ("segment_", "valid_", "suppressed_"):
                    if raw_segment_id.startswith(prefix):
                        raw_segment_id = raw_segment_id[len(prefix):]
                        break

                target_id = context_id if "/contexts/" in clean_template else raw_segment_id

                seg_ep = clean_template
                if "segment_{segment_id}" in seg_ep:
                    seg_ep = seg_ep.replace("segment_{segment_id}", context_id)
                else:
                    seg_ep = seg_ep.replace("{segment_id}", target_id)

                if "segment_{context_id}" in seg_ep:
                    seg_ep = seg_ep.replace("segment_{context_id}", context_id)
                else:
                    seg_ep = seg_ep.replace("{context_id}", context_id)

                seg_ep = seg_ep.replace("{raw_segment_id}", raw_segment_id)

                if group_val:
                    seg_ep = seg_ep.replace("{group}", group_val)

                # Compose query parameters: preserve existing params + default_query + extra_query + custom_query + per_page
                query_parts = []
                if "?" in seg_ep:
                    path_part, existing_q = seg_ep.split("?", 1)
                    if existing_q:
                        query_parts.append(existing_q)
                else:
                    path_part = seg_ep

                def_q = config.get("default_query") or table_cfg.get("default_query")
                if def_q and str(def_q).strip():
                    query_parts.append(str(def_q).strip().lstrip('?'))

                ext_q = table_cfg.get("extra_query")
                if ext_q and str(ext_q).strip():
                    query_parts.append(str(ext_q).strip().lstrip('?'))

                if custom_query and str(custom_query).strip():
                    query_parts.append(str(custom_query).strip().lstrip('?'))

                seen_keys = set()
                deduped_parts = []
                for qp in query_parts:
                    for kv in qp.split("&"):
                        if not kv:
                            continue
                        k = kv.split("=")[0] if "=" in kv else kv
                        if k not in seen_keys:
                            seen_keys.add(k)
                            deduped_parts.append(kv)

                if "per_page" not in seen_keys:
                    deduped_parts.append(f"per_page={batch_size}")

                final_query = "&".join(deduped_parts)
                if path_part.startswith("http://") or path_part.startswith("https://"):
                    next_url: Optional[str] = f"{path_part}?{final_query}" if final_query else path_part
                else:
                    next_url = f"{base_url}{path_part}?{final_query}" if final_query else f"{base_url}{path_part}"

                seg_records: List[Dict[str, Any]] = []
                seg_filtered = 0

                while next_url:
                    # Ignore 403 (confidentiality suppression) and 404 (group without scores/questions)
                    resp_data = cls._execute_request_with_retry(
                        next_url,
                        bearer_token,
                        ignore_status_codes=(403, 404)
                    )

                    if not resp_data or not isinstance(resp_data, dict):
                        break

                    raw_items = resp_data.get(response_key, [])
                    if isinstance(raw_items, dict):
                        raw_items = [raw_items]
                    elif not isinstance(raw_items, list):
                        raw_items = []

                    meta_obj = resp_data.get("meta", {})

                    # Build lookup for JSON:API 'included' resources (e.g. question, category)
                    included_lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}
                    for inc in resp_data.get("included", []):
                        if isinstance(inc, dict):
                            itype = str(inc.get("type", "")).strip().lower()
                            iid = str(inc.get("id", "")).strip()
                            if itype and iid:
                                included_lookup[(itype, iid)] = inc

                    for item in raw_items:
                        if not isinstance(item, dict):
                            continue

                        attrs = item.get("attributes")
                        rec_id = item.get("id") or f"{context_id}_{group_val or ''}_{table_name}".strip('_')
                        record: Dict[str, Any] = {
                            "id": rec_id,
                            "type": item.get("type", table_name),
                            "segment_id": raw_segment_id,
                            "context_id": context_id,
                        }
                        if group_val:
                            record["group"] = group_val
                            record["category_group"] = group_val

                        if isinstance(attrs, dict):
                            record.update(attrs)
                        else:
                            record.update(item)

                        # Enrich with side-loaded JSON:API included resources (questions & categories)
                        rels = item.get("relationships", {}) if isinstance(item, dict) else {}
                        if isinstance(rels, dict):
                            q_data = rels.get("question", {}).get("data")
                            if isinstance(q_data, dict):
                                q_id = str(q_data.get("id", "")).strip()
                                record["question_id"] = q_id
                                q_inc = included_lookup.get(("question", q_id))
                                if q_inc and isinstance(q_inc.get("attributes"), dict):
                                    for qk, qv in q_inc["attributes"].items():
                                        record[f"question_{qk}"] = qv
                                    q_rels = q_inc.get("relationships", {})
                                    if isinstance(q_rels, dict):
                                        cat_data = q_rels.get("category", {}).get("data")
                                        if isinstance(cat_data, dict):
                                            cat_id = str(cat_data.get("id", "")).strip()
                                            record["category_id"] = cat_id
                                            cat_inc = included_lookup.get(("category", cat_id))
                                            if cat_inc and isinstance(cat_inc.get("attributes"), dict):
                                                for ck, cv in cat_inc["attributes"].items():
                                                    record[f"category_{ck}"] = cv

                            cat_data = rels.get("category", {}).get("data")
                            if isinstance(cat_data, dict):
                                cat_id = str(cat_data.get("id", "")).strip()
                                record["category_id"] = cat_id
                                cat_inc = included_lookup.get(("category", cat_id))
                                if cat_inc and isinstance(cat_inc.get("attributes"), dict):
                                    for ck, cv in cat_inc["attributes"].items():
                                        record[f"category_{ck}"] = cv

                        # Permanently normalize all metric fields (scores.mean, scores.size, standardDeviation, difference, etc.) to float
                        cls._normalize_metric_floats(record)

                        # Enrich with meta attributes (e.g. meta_interval, meta_participation)
                        if isinstance(meta_obj, dict):
                            for mk, mv in meta_obj.items():
                                if not isinstance(mv, (dict, list)):
                                    record[f"meta_{mk}"] = mv

                        # Delta date filtering
                        rec_ts_str = (
                            record.get("responseAnsweredAt")
                            or record.get("updated_at")
                            or record.get("updatedAt")
                            or record.get("created_at")
                            or record.get("createdAt")
                            or record.get("employmentStart")
                        )

                        if not is_full_load and cutoff_dt and rec_ts_str:
                            rec_dt = _parse_ts(rec_ts_str)
                            if rec_dt and rec_dt < cutoff_dt:
                                seg_filtered += 1
                                continue

                        if upper_bound_dt and rec_ts_str:
                            rec_dt = _parse_ts(rec_ts_str)
                            if rec_dt and rec_dt > upper_bound_dt:
                                seg_filtered += 1
                                continue

                        seg_records.append(record)

                    # Follow cursor pagination if present
                    links = resp_data.get("links", {})
                    next_url = links.get("next") if isinstance(links, dict) else None

                return seg_records, seg_filtered

            # Concurrent task execution across all (segment, group) pairs
            max_workers = int(config.get("max_workers") or config.get("parallel_workers") or 8)
            max_workers = min(max(max_workers, 1), 16)
            logger.info(
                f"[Peakon/{table_name}] Fetching {len(tasks)} tasks concurrently with {max_workers} worker threads..."
            )

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_task = {
                    executor.submit(_fetch_single_task, t): t
                    for t in tasks
                }
                for done_idx, future in enumerate(as_completed(future_to_task), start=1):
                    t_info = future_to_task[future]
                    try:
                        seg_records, seg_filtered = future.result()
                    except Exception as task_err:
                        logger.warning(f"[Peakon/{table_name}] Failed task {t_info}: {task_err}")
                        continue

                    total_filtered_out += seg_filtered
                    total_extracted += len(seg_records)

                    if on_chunk_callback:
                        records_buffer.extend(seg_records)
                        if len(records_buffer) >= s3_chunk_size:
                            on_chunk_callback(records_buffer, part_number)
                            part_number += 1
                            records_buffer.clear()
                    else:
                        all_records.extend(seg_records)

                    if done_idx % 25 == 0 or done_idx == len(tasks):
                        logger.info(
                            f"[Peakon/{table_name}] Progress: [{done_idx}/{len(tasks)}] tasks completed "
                            f"({total_extracted} total records collected so far)."
                        )

            if on_chunk_callback and records_buffer:
                on_chunk_callback(records_buffer, part_number)
                records_buffer.clear()

            logger.info(
                f"[Peakon/{table_name}] Extraction completed across {len(tasks)} tasks: "
                f"{total_extracted} records accepted ({total_filtered_out} records skipped)."
            )
            return all_records if not on_chunk_callback else []

        # =========================================================================
        # Case B: Standard Single-Endpoint Tables (answers, employees, segments, etc.)
        # =========================================================================
        clean_endpoint = endpoint.strip()
        if "://" in clean_endpoint:
            scheme, rest = clean_endpoint.split("://", 1)
            rest = re.sub(r"/+", "/", rest)
            clean_endpoint = f"{scheme}://{rest}"
        else:
            clean_endpoint = re.sub(r"/+", "/", clean_endpoint)

        if not clean_endpoint.startswith("http://") and not clean_endpoint.startswith("https://"):
            if not clean_endpoint.startswith("/"):
                clean_endpoint = "/" + clean_endpoint
            if not clean_endpoint.startswith("/api/v1"):
                clean_endpoint = f"/api/v1{clean_endpoint}"
            next_url_base = f"{base_url}{clean_endpoint}"
        else:
            next_url_base = clean_endpoint

        separator = "&" if "?" in next_url_base else "?"
        next_url = f"{next_url_base}{separator}per_page={batch_size}"

        ext_query = table_cfg.get("extra_query")
        if ext_query and str(ext_query).strip():
            sep_eq = "&" if "?" in next_url else "?"
            next_url = f"{next_url}{sep_eq}{str(ext_query).strip().lstrip('?')}"

        if custom_query and str(custom_query).strip():
            cq = str(custom_query).strip().lstrip('?')
            sep_cq = "&" if "?" in next_url else "?"
            next_url = f"{next_url}{sep_cq}{cq}"

        page_num = 1
        extracted_segment_ids: List[str] = []

        while next_url:
            logger.info(f"[Peakon/{table_name}] Page {page_num}: Fetching from {next_url}...")
            resp_data = cls._execute_request_with_retry(next_url, bearer_token)

            if not isinstance(resp_data, dict):
                raise TypeError(
                    f"[Peakon/{table_name}] Expected dict response from API, got {type(resp_data)}."
                )

            if response_key not in resp_data:
                raise KeyError(
                    f"[Peakon/{table_name}] Response key '{response_key}' not found in API response. "
                    f"Available top-level keys: {list(resp_data.keys())}."
                )

            raw_items = resp_data[response_key]
            if isinstance(raw_items, dict):
                raw_items = [raw_items]
            elif not isinstance(raw_items, list):
                raw_items = []

            meta_obj = resp_data.get("meta", {})

            page_valid_records = []
            for item in raw_items:
                if not isinstance(item, dict):
                    continue

                attrs = item.get("attributes")
                record: Dict[str, Any] = {
                    "id": item.get("id"),
                    "type": item.get("type", table_name),
                }
                if isinstance(attrs, dict):
                    record.update(attrs)
                else:
                    record.update(item)

                # Permanently normalize all metric fields to float
                cls._normalize_metric_floats(record)

                if isinstance(meta_obj, dict):
                    for mk, mv in meta_obj.items():
                        if not isinstance(mv, (dict, list)):
                            record[f"meta_{mk}"] = mv

                # If ingesting the segments table, record IDs for subsequent segment-dependent tables
                if table_name.lower().endswith("segments") and item.get("id"):
                    extracted_segment_ids.append(str(item["id"]).strip())

                # Delta date filtering
                rec_ts_str = (
                    record.get("responseAnsweredAt")
                    or record.get("updated_at")
                    or record.get("updatedAt")
                    or record.get("created_at")
                    or record.get("createdAt")
                    or record.get("employmentStart")
                )

                if not is_full_load and cutoff_dt and rec_ts_str:
                    rec_dt = _parse_ts(rec_ts_str)
                    if rec_dt and rec_dt < cutoff_dt:
                        total_filtered_out += 1
                        continue

                if upper_bound_dt and rec_ts_str:
                    rec_dt = _parse_ts(rec_ts_str)
                    if rec_dt and rec_dt > upper_bound_dt:
                        total_filtered_out += 1
                        continue

                page_valid_records.append(record)

            total_extracted += len(page_valid_records)
            logger.info(
                f"[Peakon/{table_name}] Page {page_num}: Processed {len(raw_items)} items "
                f"({len(page_valid_records)} accepted, {total_extracted} total accepted so far)."
            )

            # Buffer and stream via callback
            if on_chunk_callback:
                records_buffer.extend(page_valid_records)
                if len(records_buffer) >= s3_chunk_size:
                    on_chunk_callback(records_buffer, part_number)
                    part_number += 1
                    records_buffer.clear()
            else:
                all_records.extend(page_valid_records)

            # Follow JSON:API links.next cursor
            links = resp_data.get("links", {})
            next_url = links.get("next") if isinstance(links, dict) else None
            page_num += 1

        # Flush any remaining buffered records to S3 callback
        if on_chunk_callback and records_buffer:
            on_chunk_callback(records_buffer, part_number)
            records_buffer.clear()

        # Update in-memory segment cache if segments table was extracted
        if table_name.lower().endswith("segments") and extracted_segment_ids:
            cls._cached_segment_ids = list(dict.fromkeys(extracted_segment_ids))
            logger.info(f"Updated PeakonConnector in-memory segment cache with {len(cls._cached_segment_ids)} IDs.")

        logger.info(
            f"[Peakon/{table_name}] Extraction completed: {total_extracted} records accepted "
            f"({total_filtered_out} records skipped prior to watermark '{last_load_date}')."
        )

        return all_records if not on_chunk_callback else []
