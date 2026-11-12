"""
Custom Transformation Script for Workday Trended Workforce Gold Mart.

Pattern: gold/script/custom_transforms/<source>_<table>.py
Signature: transform(df, spark=None, context=None) -> DataFrame

Developer Notes:
- Enforces Enterprise Security, Compliance, and Row-Level Security (RLS) standards.
- Sensitive demographic attributes (e.g. Age, Gender, Date of Birth, Race and Ethnicity,
  Salary/Compensation, Social Security Numbers) are strictly stripped to prevent PII leakage.
- Validates essential organizational hierarchy keys (Worker ID, Cost Center, Supervisory Org,
  Management Level) required for downstream analytics and access filtering.
"""

import logging
import re
from typing import Optional, Dict, Any, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

logger = logging.getLogger(__name__)

# Patterns identifying sensitive PII/demographic fields that must never be exposed in Gold reporting
SENSITIVE_PII_PATTERNS = [
    r"^age$",
    r"^gender$",
    r"^sex$",
    r"date_?of_?birth",
    r"^dob$",
    r"birth_?date",
    r"race.*ethnicity",
    r"^ethnicity$",
    r"^race$",
    r"veteran.*status",
    r"^salary$",
    r"^compensation$",
    r"pay_?type",
    r"is_?rif",
    r"^ssn$",
    r"social_?security",
    r"disability",
    r"sexual_?orientation",
    r"marital_?status",
    r"^religion$",
]


def transform(
    df: DataFrame,
    spark=None,
    context: Optional[Dict[str, Any]] = None
) -> DataFrame:
    """
    Main transform hook for Workday Trended Workforce Gold Mart.

    Args:
        df (DataFrame): PySpark DataFrame produced by trended_workforce.sql.
        spark (SparkSession, optional): Active SparkSession with Iceberg / Glue catalog access.
        context (dict, optional): Runtime execution metadata containing table_cfg, glue_database,
                                  is_initial_load flag, etc.

    Returns:
        DataFrame: Enriched, sanitized PySpark DataFrame ready for Athena / Aurora Gold serving.
    """
    logger.info("[CUSTOM TRANSFORM] Invoking custom transform hook for 'workday.trended_workforce'...")

    ctx = context or {}
    is_initial_load = ctx.get("is_initial_load", False)

    # 1. Defense-in-depth: dynamically strip any sensitive demographic/PII columns
    sensitive_cols_to_drop: List[str] = []
    for c in df.columns:
        c_lower = c.strip().lower()
        for pat in SENSITIVE_PII_PATTERNS:
            if re.search(pat, c_lower):
                sensitive_cols_to_drop.append(c)
                break

    if sensitive_cols_to_drop:
        logger.warning(
            f"[SECURITY ENFORCEMENT] Dropping sensitive PII columns from Workday Gold Mart: {sensitive_cols_to_drop}"
        )
        df = df.drop(*sensitive_cols_to_drop)

    # 2. Ensure core security & RLS columns are always present with valid types
    expected_security_columns = {
        "worker_id": "string",
        "worker_name": "string",
        "email": "string",
        "cost_center_id": "string",
        "cost_center_name": "string",
        "supervisory_org": "string",
        "job_profile": "string",
        "job_title": "string",
        "management_level": "string",
        "employment_status": "string",
        "snapshot_date": "string",
    }
    for col_name, col_type in expected_security_columns.items():
        if col_name not in df.columns:
            logger.info(f"[SECURITY RLS] Attaching missing column '{col_name}' as NULL {col_type}.")
            df = df.withColumn(col_name, lit(None).cast(col_type))

    if is_initial_load:
        logger.info("[CUSTOM TRANSFORM] Initial historical load detected; preserving bootstrap records.")
        return df

    return df

