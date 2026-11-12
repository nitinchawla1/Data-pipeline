-- ==============================================================================
-- Gold Mart Query: positions_awaiting_approval.sql
-- Source System: workday
-- Hierarchy: s3://<bucket>/gold/query/workday/positions_awaiting_approval.sql
-- Produces:
--   - Athena Physical Table : gold_workday_positions_awaiting_approval (Iceberg Table)
--   - Downstream MySQL Mart : hr_gold.workday_positions_awaiting_approval (Zero-Downtime Physical Table)
-- S3 Storage: s3://<bucket>/gold/data/workday/positions_awaiting_approval/
--
-- Target: hr_gold.workday_positions_awaiting_approval
-- Description: Pending position requisitions and job openings awaiting management / HR
--              business process approval, including aging days, hiring managers, and cost centers.
-- ==============================================================================

select
    cast(coalesce(position_id, id) as string)                            as position_id,
    cast(requisition_id as string)                                       as requisition_id,
    cast(position_title as string)                                       as position_title,
    cast(job_profile as string)                                          as job_profile,
    cast(job_family as string)                                           as job_family,
    cast(cost_center_id as string)                                       as cost_center_id,
    cast(cost_center_name as string)                                     as cost_center_name,
    cast(supervisory_org as string)                                      as supervisory_org,
    cast(hiring_manager_id as string)                                    as hiring_manager_id,
    cast(hiring_manager_name as string)                                  as hiring_manager_name,
    cast(recruiter_id as string)                                         as recruiter_id,
    cast(recruiter_name as string)                                       as recruiter_name,
    cast(current_approver_id as string)                                  as current_approver_id,
    cast(current_approver_name as string)                                as current_approver_name,
    cast(approval_step as string)                                        as approval_step,
    cast(submission_date as string)                                      as submission_date,
    cast(days_awaiting_approval as int)                                  as days_awaiting_approval,
    cast(approval_status as string)                                      as approval_status,
    _inserted_at                                                         as loaded_at,
    current_timestamp                                                    as _data_as_of
from tbl_positions_awaiting_approval_for_data_lake
where _is_current = 'Y'
    and coalesce(_is_deleted, 'N') != 'Y';

