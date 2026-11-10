"""
Custom Transformation Script for Peakon Segments Gold Mart.

Pattern: gold/script/custom_transforms/<source>_<table>.py
Signature: transform(df, spark=None, context=None) -> DataFrame

Developer Notes:
- Curates Peakon Segments and their relationship links to attributes for Row-Level Security (RLS).
- Preserves all incoming segment metadata and relationship attributes.
"""

import logging
from typing import Optional, Dict, Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

logger = logging.getLogger(__name__)


def transform(
    df: DataFrame,
    spark=None,
    context: Optional[Dict[str, Any]] = None
) -> DataFrame:
    """
    Main transform hook for Peakon Segments Gold Mart.

    Args:
        df (DataFrame): PySpark DataFrame produced by segments.sql.
        spark (SparkSession, optional): Active SparkSession with Iceberg / Glue catalog access.
        context (dict, optional): Runtime execution metadata containing table_cfg, glue_database,
                                  is_initial_load flag, etc.

    Returns:
        DataFrame: Conformed PySpark DataFrame ready for Athena / Aurora Gold serving.
    """
    logger.info("[CUSTOM TRANSFORM] Invoking custom transform hook for 'peakon.segments'...")

    ctx = context or {}
    is_initial_load = ctx.get("is_initial_load", False)

    # Ensure key RLS relationship attributes are present
    expected_cols = {
        "attribute_id": "string",
        "attribute_type": "string",
        "description": "string",
    }
    for col_name, col_type in expected_cols.items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast(col_type))

    if is_initial_load:
        logger.info("[CUSTOM TRANSFORM] Initial historical load detected; preserving bootstrap records.")
        return df

    return df

