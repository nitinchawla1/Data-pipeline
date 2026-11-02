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
import time
import logging
import urllib.request
import urllib.parse
from urllib.error import HTTPError, URLError
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

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
        logger.info(f"Fetching segment IDs directly from Peakon API: {base_url}/api/v1/segments...")
        curr_url: Optional[str] = f"{base_url.rstrip('/')}/api/v1/segments?per_page=2000"
        api_segment_ids: List[str] = []
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

                links = resp.get("links", {})
                curr_url = links.get("next") if isinstance(links, dict) else None

            if api_segment_ids:
                deduped = list(dict.fromkeys(api_segment_ids))
                cls._cached_segment_ids = deduped
                logger.info(f"Acquired {len(deduped)} segment IDs from Peakon API.")
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

        # 2. Resolve Table Endpoint, Response Key, and Batch Size
        endpoint = ConfigLoader.get_table_endpoint("peakon", table_name, config)
        response_key = config.get("response_records_key", "data")
        batch_size = int(secret_dict.get("batch_size") or config.get("batch_size", 2000))
        batch_size = min(max(batch_size, 1), 20000)

        # Determine if endpoint requires dynamic segment iteration
        table_cfg = ConfigLoader.get_table_config("peakon", table_name, source_config=config)
        is_segment_endpoint = (
            "{segment_id}" in endpoint
            or "{context_id}" in endpoint
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
        # Case A: Dynamic Segment-Level Endpoints
        # =========================================================================
        if is_segment_endpoint:
            segment_ids = cls._get_dynamic_segment_ids(base_url, bearer_token, config)
            if not segment_ids:
                logger.warning(
                    f"[Peakon/{table_name}] No segment IDs available to query. Exiting table extraction."
                )
                return []

            logger.info(
                f"[Peakon/{table_name}] Processing {len(segment_ids)} segments for endpoint template: {endpoint}"
            )

            # Normalize endpoint path (prevent duplicate /api/v1 or missing leading slash)
            clean_template = endpoint.strip()
            if not clean_template.startswith("http://") and not clean_template.startswith("https://"):
                if not clean_template.startswith("/"):
                    clean_template = "/" + clean_template
                if not clean_template.startswith("/api/v1"):
                    clean_template = f"/api/v1{clean_template}"

            for seg_idx, raw_seg in enumerate(segment_ids, start=1):
                seg_str = str(raw_seg).strip()
                # Context ID format in Peakon contexts endpoint: 'segment_<id>'
                context_id = seg_str if seg_str.startswith("segment_") else f"segment_{seg_str}"
                raw_segment_id = seg_str[len("segment_"):] if seg_str.startswith("segment_") else seg_str

                # Peakon context endpoints require context_id ('segment_<id>')
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

                sep = "&" if "?" in seg_ep else "?"
                if seg_ep.startswith("http://") or seg_ep.startswith("https://"):
                    next_url = f"{seg_ep}{sep}per_page={batch_size}"
                else:
                    next_url = f"{base_url}{seg_ep}{sep}per_page={batch_size}"

                if custom_query and str(custom_query).strip():
                    cq = str(custom_query).strip()
                    sep_cq = "&" if "?" in next_url else "?"
                    next_url = f"{next_url}{sep_cq}{cq}"

                seg_records_count = 0
                while next_url:
                    # Ignore 403 / 404 (confidentiality anonymity threshold or empty segment suppression)
                    resp_data = cls._execute_request_with_retry(
                        next_url,
                        bearer_token,
                        ignore_status_codes=(403, 404)
                    )

                    if not resp_data or not isinstance(resp_data, dict):
                        # Empty response due to 403 / 404 suppression
                        break

                    raw_items = resp_data.get(response_key, [])
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
                            "id": item.get("id") or f"{context_id}_{table_name}",
                            "type": item.get("type", table_name),
                            "segment_id": raw_segment_id,
                            "context_id": context_id,
                        }
                        if isinstance(attrs, dict):
                            record.update(attrs)
                        else:
                            record.update(item)

                        # Enrich with meta attributes (e.g. meta_interval, meta_participation_rate)
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
                                total_filtered_out += 1
                                continue

                        if upper_bound_dt and rec_ts_str:
                            rec_dt = _parse_ts(rec_ts_str)
                            if rec_dt and rec_dt > upper_bound_dt:
                                total_filtered_out += 1
                                continue

                        page_valid_records.append(record)

                    seg_records_count += len(page_valid_records)
                    total_extracted += len(page_valid_records)

                    if on_chunk_callback:
                        records_buffer.extend(page_valid_records)
                        if len(records_buffer) >= s3_chunk_size:
                            on_chunk_callback(records_buffer, part_number)
                            part_number += 1
                            records_buffer.clear()
                    else:
                        all_records.extend(page_valid_records)

                    # Follow cursor pagination if present
                    links = resp_data.get("links", {})
                    next_url = links.get("next") if isinstance(links, dict) else None

                if seg_idx % 25 == 0 or seg_idx == len(segment_ids):
                    logger.info(
                        f"[Peakon/{table_name}] Progress: [{seg_idx}/{len(segment_ids)}] segments processed "
                        f"({total_extracted} total records accepted so far)."
                    )

            if on_chunk_callback and records_buffer:
                on_chunk_callback(records_buffer, part_number)
                records_buffer.clear()

            logger.info(
                f"[Peakon/{table_name}] Extraction completed across {len(segment_ids)} segments: "
                f"{total_extracted} records accepted ({total_filtered_out} records skipped)."
            )
            return all_records if not on_chunk_callback else []

        # =========================================================================
        # Case B: Standard Single-Endpoint Tables (answers, employees, segments, etc.)
        # =========================================================================
        clean_endpoint = endpoint.strip()
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

        if custom_query and str(custom_query).strip():
            cq = str(custom_query).strip()
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
