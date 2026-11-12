-- ==============================================================================
-- Gold Mart Query: trended_workforce_bp_reason.sql
-- Source System: workday
-- Hierarchy: s3://<bucket>/gold/query/workday/trended_workforce_bp_reason.sql
-- Produces:
--   - Athena Physical Table : gold_workday_trended_workforce_bp_reason (Iceberg Table)
--   - Downstream MySQL Mart : hr_gold.workday_trended_workforce_bp_reason (Zero-Downtime Physical Table)
-- S3 Storage: s3://<bucket>/gold/data/workday/trended_workforce_bp_reason/
--
-- Target: hr_gold.workday_trended_workforce_bp_reason
-- Description: Business Process (BP) reason tracking mart capturing workforce movement
--              events (hires, terminations, promotions, transfers, job changes) with
--              effective dates, categories, and business reasons.
-- ==============================================================================

select
    cast(coalesce(worker_id, employee_id, id) as string)                 as worker_id,
    cast(coalesce(event_id, bp_event_id, id) as string)                  as event_id,
    cast(business_process_type as string)                                as business_process_type,
    cast(business_process_reason as string)                              as business_process_reason,
    cast(business_process_category as string)                            as business_process_category,
    cast(effective_date as string)                                       as effective_date,
    cast(initiation_date as string)                                      as initiation_date,
    cast(completion_date as string)                                      as completion_date,
    cast(status as string)                                               as event_status,
    cast(initiated_by as string)                                         as initiated_by,
    cast(prior_job_profile as string)                                    as prior_job_profile,
    cast(proposed_job_profile as string)                                 as proposed_job_profile,
    cast(prior_cost_center as string)                                    as prior_cost_center,
    cast(proposed_cost_center as string)                                 as proposed_cost_center,
    cast(prior_supervisory_org as string)                                as prior_supervisory_org,
    cast(proposed_supervisory_org as string)                             as proposed_supervisory_org,
    _inserted_at                                                         as loaded_at,
    current_timestamp                                                    as _data_as_of
from tbl_trended_workforce_bp_reason_for_data_lake_pulls
where _is_current = 'Y'
    and coalesce(_is_deleted, 'N') != 'Y';

