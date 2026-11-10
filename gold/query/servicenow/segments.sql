-- ==============================================================================
-- Gold Mart Query: segments.sql
-- Source System: peakon
-- Hierarchy: s3://<bucket>/gold/query/peakon/segments.sql
-- Produces:
--   - Athena Physical Table : gold_peakon_segments (Iceberg Table)
--   - Downstream MySQL Mart : hr_gold.peakon_segments (Zero-Downtime Physical Table)
-- S3 Storage: s3://<bucket>/gold/data/peakon/segments/
--
-- Target: hr_gold.peakon_segments
-- Description: Conformed Peakon segments with all attribute links and metadata for security/RLS.
-- ==============================================================================

select
    cast(id as string) as segment_id,
    cast(name as string) as segment_name,
    cast(type as string) as segment_type,
    cast(description as string) as description,
    cast(rel_attribute_id as string) as attribute_id,
    cast(rel_attribute_type as string) as attribute_type,
    _ingested_at as loaded_at,
    current_timestamp as _data_as_of
from tbl_segments
where _is_current = 'Y'
    and coalesce(_is_deleted, 'N') != 'Y';

