"""
Custom Transformation Script for Workday Trended Workforce Gold Mart.

Pattern: gold/script/custom_transforms/<source>_<table>.py
Signature: transform(df, spark=None, context=None) -> DataFrame

Source Report: Trended_Workforce_Snapshots_for_Data_Lake
  - Employee Identification: Employee_ID, Worker_Name, Primary_Work_Email, Position_ID
  - Cost Center Hierarchy: Cost_Center_ID, Cost_Center, Cost_Center_WID, Cost_Center_Level_01,
                           Cost_Center_Level_01_WID, Cost_Center_Level_02, Cost_Center_Level_02_WID,
                           Cost_Center_Level_03
  - Supervisory Org Hierarchy: Supervisory_Organization, Supervisory_Organization_WID,
                               Supervisory_Organization_ID, Level_01_from_the_Top, Level_01_from_the_Top_WID,
                               Level_02_from_the_Top, Level_02_from_the_Top_WID, Level_03_from_the_Top,
                               Level_03_from_the_Top_WID
  - Job & Role Classifications: Job_Profile, Job_Family, Job_Family_Group, Management_Level_ID,
                                Management_Level, Compensation_Grade, Current_Rating
  - Employment & Work Arrangement: Worker_Type, Worker_Status, Employee_Type, Time_Type,
                                   Hybrid_Work_Arrangement, Number_of_Direct_Employees, Tenure_Category
  - Location Details: Location, Location_Country, Location_State, Primary_Home_State
  - Snapshot Audit: Record_Date

Developer Notes:
- Standardizes all incoming column names (case-insensitive / snake_case mapping).
- Enforces Enterprise Privacy & Compliance: dynamically scrubs any sensitive demographic PII
  (e.g., Age, Gender, Date of Birth, Race/Ethnicity, SSN, Disability, exact Salary).
- Initial Load Awareness: Preserves historical bootstrap records when is_initial_load is True.
"""

import logging
import re
from typing import Optional, Dict, Any, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

logger = logging.getLogger(__name__)

# Authoritative columns contract for Workday Trended Workforce Snapshots
WORKDAY_TRENDED_WORKFORCE_COLUMNS: Dict[str, str] = {
    # 1. Employee Identification
    "employee_id": "string",
    "worker_name": "string",
    "primary_work_email": "string",
    "position_id": "string",

    # 2. Supervisory Organization Hierarchy
    "supervisory_organization": "string",
    "supervisory_organization_wid": "string",
    "supervisory_organization_id": "string",
    "level_01_from_the_top": "string",
    "level_01_from_the_top_wid": "string",
    "level_02_from_the_top": "string",
    "level_02_from_the_top_wid": "string",
    "level_03_from_the_top": "string",
    "level_03_from_the_top_wid": "string",

    # 3. Cost Center Hierarchy
    "cost_center_id": "string",
    "cost_center": "string",
    "cost_center_wid": "string",
    "cost_center_level_01": "string",
    "cost_center_level_01_wid": "string",
    "cost_center_level_02": "string",
    "cost_center_level_02_wid": "string",
    "cost_center_level_03": "string",

    # 4. Worker & Employment Status
    "worker_type": "string",
    "worker_status": "string",
    "employee_type": "string",
    "time_type": "string",
    "hybrid_work_arrangement": "string",
    "number_of_direct_employees": "int",
    "tenure_category": "string",

    # 5. Job & Management Classifications
    "job_profile": "string",
    "job_family": "string",
    "job_family_group": "string",
    "management_level_id": "string",
    "management_level": "string",
    "compensation_grade": "string",
    "current_rating": "string",

    # 6. Location Details
    "location": "string",
    "location_country": "string",
    "location_state": "string",
    "primary_home_state": "string",

    # 7. Snapshot Grain
    "record_date": "string",
}

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
    r"exact_?compensation",
    r"^base_?pay$",
    r"pay_?rate",
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

    # 1. Normalize Column Names to lowercase snake_case
    existing_cols = df.columns
    col_mapping = {}
    for c in existing_cols:
        norm_c = c.strip().lower().replace(" ", "_")
        col_mapping[c] = norm_c

    # Rename if columns have mixed casing or spaces
    for orig_c, target_c in col_mapping.items():
        if orig_c != target_c and orig_c in df.columns:
            df = df.withColumnRenamed(orig_c, target_c)

    # 2. Defense-in-depth: dynamically strip any sensitive demographic/PII columns
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

    # 3. Ensure all 40 authoritative columns from the Workday report are present
    for target_col, col_type in WORKDAY_TRENDED_WORKFORCE_COLUMNS.items():
        if target_col not in df.columns:
            logger.info(f"[SCHEMA ASSURANCE] Attaching contract column '{target_col}' as NULL {col_type}.")
            df = df.withColumn(target_col, lit(None).cast(col_type))
        else:
            # Enforce proper casting (e.g. number_of_direct_employees as integer)
            if col_type == "int":
                df = df.withColumn(target_col, col(target_col).cast("int"))

    if is_initial_load:
        logger.info("[CUSTOM TRANSFORM] Initial historical load detected; preserving bootstrap records.")
        return df

    return df
