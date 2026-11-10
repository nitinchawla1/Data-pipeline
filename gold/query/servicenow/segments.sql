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
-- Description: Conformed Peakon segments with all attributes from tbl_segments
--              for Row-Level Security (RLS) and contextual reporting.
-- ==============================================================================

select
    cast(id as string)                 as segment_id,
    cast(name as string)               as segment_name,
    cast(type as string)               as segment_type,
    cast(identifier as string)         as identifier,
    cast(standard as string)           as standard,
    cast(contextid as string)          as context_id,
    cast(abbreviation as string)       as abbreviation,
    cast(direct as string)             as direct,
    cast(benchmarktype as string)      as benchmark_type,
    cast(benchmarkpropagate as string) as benchmark_propagate,
    cast(benchmarkid as string)        as benchmark_id,
    cast(benchmarkselector as string)  as benchmark_selector,
    cast(logo as string)               as logo,
    cast(nametranslated as string)     as name_translated,
    _inserted_at                       as loaded_at,
    current_timestamp                  as _data_as_of
from tbl_segments
where _is_current = 'Y'
    and coalesce(_is_deleted, 'N') != 'Y';
