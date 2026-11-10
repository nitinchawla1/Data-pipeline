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
-- Description: Conformed Peakon employees mart with Workday, Cost Center, Sup Org,
--              and relationship IDs for enterprise Row-Level Security (RLS).
--              Sensitive demographic attributes (Age, Gender, DOB, Ethnicity, etc.)
--              are strictly excluded for compliance and privacy.
-- ==============================================================================

select
    cast(id as string)                                                      as employee_id,
    cast(coalesce(name, email, id) as string)                               as employee_name,
    cast(email as string)                                                   as email,
    cast(coalesce(workday_id, external_id) as string)                       as workday_id,
    cast(coalesce(cost_center, 'Unassigned') as string)                     as cost_center,
    cast(coalesce(supervisory_organization, 'Unassigned') as string)        as supervisory_organization,
    cast(department as string)                                              as department,
    cast(location as string)                                                as location,
    cast(rel_manager_id as string)                                          as manager_id,
    cast(rel_attribute_ids as string)                                       as attribute_ids,
    cast(rel_attribute_type as string)                                      as attribute_type,
    _ingested_at                                                            as loaded_at,
    current_timestamp                                                       as _data_as_of
from tbl_employees
where _is_current = 'Y'
    and coalesce(_is_deleted, 'N') != 'Y';

