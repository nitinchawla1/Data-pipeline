-- ==============================================================================
-- Gold Mart Query: positions_status.sql
-- Source System: workday
-- Hierarchy: s3://<bucket>/gold/query/workday/positions_status.sql
-- Produces:
--   - Athena Physical Table : gold_workday_positions_status (Iceberg Table)
--   - Downstream MySQL Mart : hr_gold.workday_positions_status (Zero-Downtime Physical Table)
-- S3 Storage: s3://<bucket>/gold/data/workday/positions_status/
--
-- Target: hr_gold.workday_positions_status
-- Description: Headcount and position status mart tracking filled, open, frozen,
--              and closed positions with effective dates, job profiles, and organizational units.
-- ==============================================================================

select
    cast(coalesce(position_id, id) as string)                            as position_id,
    cast(position_title as string)                                       as position_title,
    cast(position_status as string)                                      as position_status,
    cast(is_filled as string)                                            as is_filled,
    cast(worker_id as string)                                            as current_worker_id,
    cast(worker_name as string)                                          as current_worker_name,
    cast(job_profile as string)                                          as job_profile,
    cast(job_family as string)                                           as job_family,
    cast(cost_center_id as string)                                       as cost_center_id,
    cast(cost_center_name as string)                                     as cost_center_name,
    cast(supervisory_org as string)                                      as supervisory_org,
    cast(location as string)                                             as location,
    cast(time_type as string)                                            as time_type,
    cast(worker_type as string)                                          as worker_type,
    cast(availability_date as string)                                    as availability_date,
    cast(effective_date as string)                                       as effective_date,
    cast(close_date as string)                                           as close_date,
    _inserted_at                                                         as loaded_at,
    current_timestamp                                                    as _data_as_of
from tbl_positions_status_for_data_lake
where _is_current = 'Y'
    and coalesce(_is_deleted, 'N') != 'Y';

