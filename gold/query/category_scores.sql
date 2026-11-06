-- ==============================================================================
-- Gold Mart Query: category_scores.sql
-- Source System: peakon
-- Hierarchy: s3://<bucket>/gold/query/peakon/category_scores.sql
-- Produces:
--   - Athena Physical Table : gold_peakon_category_scores (Iceberg Table)
--   - Downstream MySQL Mart : hr_gold.peakon_category_scores (Zero-Downtime Physical Table)
-- S3 Storage: s3://<bucket>/gold/data/peakon/category_scores/
--
-- Target: hr_gold.peakon_category_scores
-- Description: Question set scores and driver scores for all groups, with the category names from the lookup.
-- ==============================================================================

select
    cast(s.segment_id as string) as segment_id,
    s.`group` as question_set,
    cast(s.id as string) as category_id,
    c.standard as category_standard, --driver code, e.g. autonomy
    c.name as category_name, --display name, e.g. Autonomy
    case when c.standard = c.`group` then 'question_set' else 'driver' end as level, --sets own score vs a driver
    cast(s.scores_mean as double) as mean_score,
    cast(s.scores_benchmark_mean as double) as benchmark_mean,
    cast(s.participation_respondents as int) as respondents,
    cast(s.participation_rate as double) as participation_rate,
    cast(s.participation_benchmark_mean as double) as participation_benchmark_mean,
    s.observations as observations_json,
    s._ingested_at as loaded_at
from tbl_scores_by_category s
left join tbl_peakon_categories c
    on cast(c.id as string) = cast(s.id as string)
    and c._is_current = 'Y'
where s._is_current = 'Y'
    and not (c.`group` is null and c.name is null); --agreed rule: drop when both names are blank
