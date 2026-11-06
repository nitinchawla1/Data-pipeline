"""
Custom Transformation Script for Peakon Question Scores Gold Mart.

Pattern: gold/script/custom_transforms/<source>_<table>.py
Signature: transform(df, spark=None, context=None) -> DataFrame

Developer Notes:
- Developers can handle custom post-SQL transformations, external API / LLM sentiment calls,
  question theme categorizations, and benchmarking logic directly within this script.
- Strict Precedence Hierarchy:
    Custom Transform (df.withColumn) > SQL Query Column > Config Default (NULL)
- Initial Load Awareness: When context.get('is_initial_load') is True, historical pre-enriched
  records are preserved directly.
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
    Main transform hook for Peakon Question Scores Gold Mart.

    Args:
        df (DataFrame): PySpark DataFrame produced by question_scores.sql.
        spark (SparkSession, optional): Active SparkSession with Iceberg / Glue catalog access.
        context (dict, optional): Runtime execution metadata containing table_cfg, glue_database,
                                  is_initial_load flag, etc.

    Returns:
        DataFrame: Enriched PySpark DataFrame ready for Athena / Aurora Gold serving.
    """
    logger.info("[CUSTOM TRANSFORM] Invoking custom transform hook for 'peakon.question_scores'...")

    ctx = context or {}
    is_initial_load = ctx.get("is_initial_load", False)

    # Attach configured external columns with NULL default if not already present
    external_cols = ["sentiment_label", "key_theme"]
    for col_name in external_cols:
        if col_name not in df.columns:
            logger.info(f"[CUSTOM TRANSFORM] Attaching external column '{col_name}' as NULL string.")
            df = df.withColumn(col_name, lit(None).cast("string"))

    if is_initial_load:
        logger.info("[CUSTOM TRANSFORM] Initial historical load detected; preserving bootstrap records.")
        return df

    # Extensible Hook: Developers can add LLM-based theme clustering or custom score indexing here
    return df

