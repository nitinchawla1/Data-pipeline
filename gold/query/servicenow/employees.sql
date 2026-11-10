-- ==============================================================================
-- Gold Mart Query: employees.sql
-- Source System: peakon
-- Hierarchy: s3://<bucket>/gold/query/peakon/employees.sql
-- Produces:
--   - Athena Physical Table : gold_peakon_employees (Iceberg Table)
--   - Downstream MySQL Mart : hr_gold.peakon_employees (Zero-Downtime Physical Table)
-- S3 Storage: s3://<bucket>/gold/data/peakon/employees/
--
-- Target: hr_gold.peakon_employees
-- Description: Conformed Peakon employees mart with Workday ID, Cost Center,
--              Supervisory Organization hierarchy, and organizational attributes
--              for enterprise Row-Level Security (RLS) and contextual reporting.
--              Sensitive demographic attributes (Age, Gender, Date of Birth,
--              Race and Ethnicity, Veteran Status, RIF flags, Pay Type) are
--              strictly excluded in accordance with privacy and compliance policies.
-- ==============================================================================

select
    cast(id as string)                                                   as employee_id,
    cast(coalesce(name, concat_ws(' ', firstname, lastname)) as string)    as employee_name,
    cast(firstname as string)                                            as first_name,
    cast(lastname as string)                                             as last_name,
    cast(identifier as string)                                           as email,
    cast(external as string)                                             as workday_id,
    cast(`manager email` as string)                                      as manager_email,
    cast(`cost center id` as string)                                     as cost_center_id,
    cast(`cost center name` as string)                                   as cost_center_name,
    cast(`supervisory org name` as string)                               as supervisory_org_name,
    cast(supervisoryorgfromtop3 as string)                                as supervisory_org_level_3,
    cast(supervisoryorgfromtop4 as string)                                as supervisory_org_level_4,
    cast(supervisoryorgfromtop5 as string)                                as supervisory_org_level_5,
    cast(supervisoryorgfromtop6 as string)                                as supervisory_org_level_6,
    cast(supervisoryorgfromtop7 as string)                                as supervisory_org_level_7,
    cast(supervisoryorgfromtop8 as string)                                as supervisory_org_level_8,
    cast(supervisoryorgfromtop9 as string)                                as supervisory_org_level_9,
    cast(supervisoryorgfromtop10 as string)                               as supervisory_org_level_10,
    cast(supervisoryorgfromtop11 as string)                               as supervisory_org_level_11,
    cast(supervisoryorgfromtop12 as string)                               as supervisory_org_level_12,
    cast(department as string)                                           as department,
    cast(`job title` as string)                                          as job_title,
    cast(`job profile` as string)                                        as job_profile,
    cast(`job family` as string)                                         as job_family,
    cast(`job family group` as string)                                   as job_family_group,
    cast(`management level` as string)                                   as management_level,
    cast(`management level code` as string)                              as management_level_code,
    cast(`management level group` as string)                             as management_level_group,
    cast(is_hr as string)                                                as is_hr,
    cast(`primary hr poc` as string)                                     as primary_hr_poc,
    cast(office as string)                                               as office,
    cast(`work city` as string)                                          as work_city,
    cast(`work state` as string)                                         as work_state,
    cast(`work country` as string)                                       as work_country,
    cast(`work region` as string)                                        as work_region,
    cast(locale as string)                                               as locale,
    cast(`time type` as string)                                          as time_type,
    cast(`worker sub-type` as string)                                    as worker_sub_type,
    cast(`hybrid category` as string)                                    as hybrid_category,
    cast(employmentstatus as string)                                     as employment_status,
    cast(`start date` as string)                                         as start_date,
    cast(`continuous service date` as string)                            as continuous_service_date,
    cast(`on leave` as string)                                           as on_leave,
    cast(`separation date` as string)                                    as separation_date,
    cast(`separation reason` as string)                                  as separation_reason,
    cast(peakon_included as string)                                      as peakon_included,
    cast(peakon_excluded as string)                                      as peakon_excluded,
    cast(`sample population` as string)                                  as sample_population,
    cast(`engagement groups` as string)                                  as engagement_groups,
    _inserted_at                                                         as loaded_at,
    current_timestamp                                                    as _data_as_of
from tbl_employees
where _is_current = 'Y'
    and coalesce(_is_deleted, 'N') != 'Y';
