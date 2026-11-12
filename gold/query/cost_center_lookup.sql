-- ==============================================================================
-- Gold Mart Query: cost_center_lookup.sql
-- Source System: workday
-- Hierarchy: s3://<bucket>/gold/query/workday/cost_center_lookup.sql
-- Produces:
--   - Athena Physical Table : gold_workday_cost_center_lookup (Iceberg Table)
--   - Downstream MySQL Mart : hr_gold.workday_cost_center_lookup (Zero-Downtime Physical Table)
-- S3 Storage: s3://<bucket>/gold/data/workday/cost_center_lookup/
--
-- Target: hr_gold.workday_cost_center_lookup
-- Description: Conformed Cost Center hierarchy lookup mart capturing managers,
--              reporting levels (Levels 1 through 6), and organizational rollups.
-- ==============================================================================

select
    cast(coalesce(cost_center_id, cost_center, id) as string)            as cost_center_id,
    cast(cost_center_name as string)                                     as cost_center_name,
    cast(cost_center_code as string)                                     as cost_center_code,
    cast(manager_id as string)                                           as manager_id,
    cast(manager_name as string)                                         as manager_name,
    cast(manager_email as string)                                        as manager_email,
    cast(hierarchy_level_1 as string)                                    as hierarchy_level_1,
    cast(hierarchy_level_2 as string)                                    as hierarchy_level_2,
    cast(hierarchy_level_3 as string)                                    as hierarchy_level_3,
    cast(hierarchy_level_4 as string)                                    as hierarchy_level_4,
    cast(hierarchy_level_5 as string)                                    as hierarchy_level_5,
    cast(hierarchy_level_6 as string)                                    as hierarchy_level_6,
    cast(company_code as string)                                         as company_code,
    cast(is_active as string)                                            as is_active,
    _inserted_at                                                         as loaded_at,
    current_timestamp                                                    as _data_as_of
from tbl_cost_center_manager_and_levels_lookup_for_data_lake
where _is_current = 'Y'
    and coalesce(_is_deleted, 'N') != 'Y';

