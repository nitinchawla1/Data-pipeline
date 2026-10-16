"""
ServiceNow REST API Connector — AWS Glue Data Pipeline.

Required Secrets Manager keys:
  auth_type    : 'basic' or 'oauth'
  api_base_url : ServiceNow instance URL (e.g. https://mycompany.service-now.com)
  username     : (when auth_type=basic) ServiceNow username
  password     : (when auth_type=basic) ServiceNow password
  token_url    : (when auth_type=oauth) OAuth token endpoint
  client_id    : (when auth_type=oauth) Client identifier
  client_secret: (when auth_type=oauth) Client secret
  batch_size   : Records per page (default in config)

Required bronze_config.json keys (source_systems.servicenow):
  base_url             : Fallback instance URL if not in secret
  response_records_key : JSON key containing records (typically 'result')
  batch_size           : Records per page
"""

import logging
from typing import Any, Callable, Dict, List, Optional

from .http_client import HTTPClient
from config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class ServiceNowConnector:
    """
    ServiceNow Table API connector. Supports delta extraction with offset pagination.
    Streams large datasets via chunked S3 callbacks.
    """

    @staticmethod
    def fetch_delta(
        last_load_date: str,
        secret_dict: Dict[str, Any],
        table_name: str,
        source_config: Dict[str, Any],
        custom_query: Optional[str] = None,
        on_chunk_callback: Optional[Callable[[List[Dict[str, Any]], int], None]] = None,
        s3_chunk_size: int = 10000,
    ) -> List[Dict[str, Any]]:
        """
        Extracts rows from a ServiceNow table updated since last_load_date.

        Args:
            last_load_date:    Watermark timestamp — inclusive start.
            secret_dict:       Credentials from Secrets Manager.
            table_name:        ServiceNow table name (e.g. 'incident').
            source_config:     Source config block from bronze_config.json.
            custom_query:      Optional sysparm_query override from CLI.
            on_chunk_callback: Called with (records, part_number) per chunk.
            s3_chunk_size:     Records buffered before flushing via callback.
        """
        if not table_name or not table_name.strip():
            raise ValueError("ServiceNow connector: 'table_name' is required.")
        if not last_load_date or not last_load_date.strip():
            raise ValueError(
                f"ServiceNow connector: 'last_load_date' is required for table '{table_name}'."
            )

        config = source_config or {}

        base_url = secret_dict.get('api_base_url') or config.get('base_url')
        if not base_url:
            raise ValueError(
                f"ServiceNow connector for '{table_name}': Instance URL is not configured. "
                "Set 'api_base_url' in Secrets Manager, or 'base_url' in "
                "bronze_config.json (source_systems.servicenow.base_url)."
            )
        base_url = str(base_url).strip().rstrip('/')

        endpoint     = ConfigLoader.get_table_endpoint('servicenow', table_name, config)
        query_filter = ConfigLoader.get_table_query_filter(
            'servicenow', table_name, last_load_date, custom_query, config,
            upper_bound=config.get('upper_bound')
        )

        response_key = config.get('response_records_key')
        if not response_key:
            raise ValueError(
                f"ServiceNow connector for '{table_name}': 'response_records_key' is not set. "
                "Add 'response_records_key' (typically 'result') to "
                "bronze_config.json (source_systems.servicenow.response_records_key)."
            )

        batch_size = secret_dict.get('batch_size') or config.get('batch_size')
        if not batch_size:
            raise ValueError(
                f"ServiceNow connector for '{table_name}': 'batch_size' is not configured. "
                "Set 'batch_size' in Secrets Manager or in bronze_config.json "
                "(source_systems.servicenow.batch_size)."
            )
        limit = int(batch_size)

        all_records:    List[Dict[str, Any]] = []
        records_buffer: List[Dict[str, Any]] = []
        total = 0
        part  = 1
        offset = 0

        logger.info(
            f"[ServiceNow/{table_name}] Starting extraction from '{base_url}' | "
            f"filter: {query_filter} | page_size: {limit}"
        )

        while True:
            api_url = (
                f"{base_url}{endpoint}?"
                f"sysparm_query={query_filter}^ORDERBYsys_updated_on"
                f"&sysparm_limit={limit}&sysparm_offset={offset}"
            )
            response = HTTPClient.get(url=api_url, secret_dict=secret_dict)

            if not isinstance(response, dict) or response_key not in response:
                raise KeyError(
                    f"[ServiceNow/{table_name}] Response key '{response_key}' not found. "
                    f"Available keys: {list(response.keys()) if isinstance(response, dict) else type(response)}. "
                    "Update 'response_records_key' in bronze_config.json."
                )

            batch = response[response_key]
            if not isinstance(batch, list):
                raise TypeError(
                    f"[ServiceNow/{table_name}] Expected list at '{response_key}', got {type(batch)}."
                )

            total += len(batch)
            logger.info(f"[ServiceNow/{table_name}] offset={offset}: {len(batch)} records | total: {total}")

            if on_chunk_callback:
                records_buffer.extend(batch)
                if len(records_buffer) >= s3_chunk_size:
                    on_chunk_callback(records_buffer, part)
                    records_buffer = []
                    part += 1
            else:
                all_records.extend(batch)

            if len(batch) < limit:
                break
            offset += limit

        if on_chunk_callback and records_buffer:
            on_chunk_callback(records_buffer, part)

        logger.info(f"[ServiceNow/{table_name}] Done. Total: {total}")
        return all_records if not on_chunk_callback else []








# NEW: Peakon REST API connector module
"""
NEW: Peakon REST API Connector — AWS Glue Data Pipeline.

Handles delta extraction and pagination for Workday Peakon Employee Voice API.

Required Secrets Manager keys:
  api_token    : Peakon API Bearer token
  api_base_url : (Optional) Peakon instance base URL (default: https://api.peakon.com)
  batch_size   : (Optional) Records per page (default: 1000)

Required bronze_config.json keys (source_systems.peakon):
  base_url             : Fallback instance URL (https://api.peakon.com)
  api_endpoint_template: Template for endpoint (e.g. /v1/{table_name})
  batch_size           : Records per page (default: 1000)
"""

import logging
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlencode

from .http_client import HTTPClient
from config_loader import ConfigLoader

logger = logging.getLogger(__name__)


# NEW: PeakonConnector implementation
class PeakonConnector:
    """
    NEW: Peakon REST API Connector.
    Extracts engagement scores, survey responses, and feedback from Peakon API.
    Supports incremental delta extraction based on last_load_date.
    """

    @staticmethod
    def fetch_delta(
        last_load_date: str,
        secret_dict: Dict[str, Any],
        table_name: str,
        source_config: Dict[str, Any],
        custom_query: Optional[str] = None,
        on_chunk_callback: Optional[Callable[[List[Dict[str, Any]], int], None]] = None,
        s3_chunk_size: int = 10000,
    ) -> List[Dict[str, Any]]:
        """
        NEW: Extracts rows from Peakon REST API updated since last_load_date.

        Args:
            last_load_date:    Watermark timestamp — inclusive start.
            secret_dict:       Credentials from AWS Secrets Manager.
            table_name:        Peakon endpoint entity (e.g. 'surveys', 'engagement_scores').
            source_config:     Source config block from bronze_config.json.
            custom_query:      Optional query override.
            on_chunk_callback: Called with (records, part_number) per chunk.
            s3_chunk_size:     Records buffered before flushing via callback.
        """
        # NEW: Input validation
        if not table_name or not table_name.strip():
            raise ValueError("Peakon connector: 'table_name' is required.")
        if not last_load_date or not last_load_date.strip():
            raise ValueError(
                f"Peakon connector: 'last_load_date' is required for table '{table_name}'."
            )

        config = source_config or {}

        # NEW: Resolve base URL and authentication credentials
        base_url = secret_dict.get('api_base_url') or config.get('base_url', 'https://api.peakon.com')
        base_url = str(base_url).strip().rstrip('/')

        api_token = secret_dict.get('api_token') or secret_dict.get('token')
        if not api_token:
            raise ValueError("Peakon connector: 'api_token' is missing from secret dictionary.")

        # NEW: Resolve endpoint template using ConfigLoader
        endpoint = ConfigLoader.get_table_endpoint('peakon', table_name, config)
        if not endpoint.startswith('/'):
            endpoint = f"/{endpoint}"
        url = f"{base_url}{endpoint}"

        # NEW: Configure HTTP client and auth headers
        client = HTTPClient(max_retries=config.get('max_retries', 3))
        headers = {
            'Authorization': f'Bearer {api_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

        batch_size = int(secret_dict.get('batch_size') or config.get('batch_size', 1000))
        page = 1
        all_records: List[Dict[str, Any]] = []
        chunk_buffer: List[Dict[str, Any]] = []
        part_number = 1

        logger.info(
            f"[NEW] Peakon connector: Starting delta fetch for '{table_name}' from {last_load_date} (batch size: {batch_size})"
        )

        while True:
            # NEW: Build query parameters with pagination and delta watermark filter
            params: Dict[str, Any] = {
                'page': page,
                'limit': batch_size,
                'updated_after': last_load_date
            }
            if custom_query:
                params['query'] = custom_query

            query_string = urlencode(params)
            request_url = f"{url}?{query_string}"

            logger.info(f"[NEW] Peakon connector: Requesting page {page} from {request_url}")
            response = client.get(request_url, headers=headers)
            if response.status_code != 200:
                raise RuntimeError(
                    f"Peakon API call failed (HTTP {response.status_code}): {response.text}"
                )

            data = response.json() if hasattr(response, 'json') else {}
            # NEW: Normalize record payload from response envelopes
            records = []
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                records = data.get('data') or data.get('results') or data.get('records') or []

            if not records:
                logger.info(f"[NEW] Peakon connector: No more records returned on page {page}.")
                break

            logger.info(f"[NEW] Peakon connector: Retrieved {len(records)} records from page {page}")

            if on_chunk_callback:
                chunk_buffer.extend(records)
                if len(chunk_buffer) >= s3_chunk_size:
                    logger.info(
                        f"[NEW] Peakon connector: Flushing {len(chunk_buffer)} records (part {part_number})"
                    )
                    on_chunk_callback(chunk_buffer, part_number)
                    part_number += 1
                    chunk_buffer = []
            else:
                all_records.extend(records)

            if len(records) < batch_size:
                logger.info("[NEW] Peakon connector: Final page reached based on page size threshold.")
                break

            page += 1

        # NEW: Flush remaining buffered records
        if on_chunk_callback and chunk_buffer:
            logger.info(
                f"[NEW] Peakon connector: Final flush of {len(chunk_buffer)} records (part {part_number})"
            )
            on_chunk_callback(chunk_buffer, part_number)

        return all_records if not on_chunk_callback else []

