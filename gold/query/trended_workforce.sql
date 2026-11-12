-- ==============================================================================
-- Gold Mart Query: trended_workforce.sql
-- Source System: workday
-- Hierarchy: s3://<bucket>/gold/query/workday/trended_workforce.sql
-- Produces:
--   - Athena Physical Table : gold_workday_trended_workforce (Iceberg Table)
--   - Downstream MySQL Mart : hr_gold.workday_trended_workforce (Zero-Downtime Physical Table)
-- S3 Storage: s3://<bucket>/gold/data/workday/trended_workforce/
--
-- Target: hr_gold.workday_trended_workforce
-- Description: Conformed trended workforce snapshot mart capturing active and historical
--              worker records, organizational hierarchy (Cost Center, Supervisory Org),
--              job classifications, and employment lifecycle status for analytics and RLS.
-- ==============================================================================

select
    cast(coalesce(worker_id, employee_id, id) as string)                 as worker_id,
    cast(coalesce(name, concat_ws(' ', firstname, lastname)) as string)   as worker_name,
    cast(firstname as string)                                            as first_name,
    cast(lastname as string)                                             as last_name,
    cast(email as string)                                                as email,
    cast(coalesce(cost_center_id, cost_center) as string)                as cost_center_id,
    cast(cost_center_name as string)                                     as cost_center_name,
    cast(supervisory_org as string)                                      as supervisory_org,
    cast(supervisory_org_id as string)                                   as supervisory_org_id,
    cast(manager_id as string)                                           as manager_id,
    cast(manager_name as string)                                         as manager_name,
    cast(job_profile as string)                                          as job_profile,
    cast(job_title as string)                                            as job_title,
    cast(job_family as string)                                           as job_family,
    cast(management_level as string)                                     as management_level,
    cast(department as string)                                           as department,
    cast(business_unit as string)                                        as business_unit,
    cast(location as string)                                             as location,
    cast(work_city as string)                                            as work_city,
    cast(work_state as string)                                           as work_state,
    cast(work_country as string)                                         as work_country,
    cast(worker_type as string)                                          as worker_type,
    cast(time_type as string)                                            as time_type,
    cast(employment_status as string)                                    as employment_status,
    cast(hire_date as string)                                            as hire_date,
    cast(original_hire_date as string)                                   as original_hire_date,
    cast(continuous_service_date as string)                              as continuous_service_date,
    cast(termination_date as string)                                     as termination_date,
    cast(fte as double)                                                  as fte,
    cast(headcount as int)                                               as headcount,
    cast(coalesce(snapshot_date, effective_date, current_date()) as string) as snapshot_date,
    _inserted_at                                                         as loaded_at,
    current_timestamp                                                    as _data_as_of
from tbl_trended_workforce_for_data_lake_pulls
where _is_current = 'Y'
    and coalesce(_is_deleted, 'N') != 'Y';

