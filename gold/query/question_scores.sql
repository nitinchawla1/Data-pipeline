-- ==============================================================================
-- Gold Mart Query: question_scores.sql
-- Source System: peakon
-- Hierarchy: s3://<bucket>/gold/query/peakon/question_scores.sql
-- Produces:
--   - Athena Physical Table : gold_peakon_question_scores (Iceberg Table)
--   - Downstream MySQL Mart : hr_gold.peakon_question_scores (Zero-Downtime Physical Table)
-- S3 Storage: s3://<bucket>/gold/data/peakon/question_scores/
--
-- Target: hr_gold.peakon_question_scores
-- Description: Question scores for every question set, with the wording and the driver each question belongs to.
-- ==============================================================================

select
    cast(s.segment_id as string) as segment_id,
    s.`group` as question_set,
    cast(s.question_id as string) as question_id,
    coalesce(get_json_object(q.text, '$.en'), q.text) as question_text, --CHECK THIS
    cast(q.category_id as string) as category_id,
    c.standard as category_standard,
    c.name as category_name,
    cast(s.scores_mean as double) as mean_score,
    cast(s.scores_benchmark_mean as double) as benchmark_mean,
    cast(s.participation_respondents as int) as respondents,
    cast(s.participation_rate as double) as participation_rate,
    cast(s.participation_benchmark_mean as double) as participation_benchmark_mean,
    s._ingested_at as loaded_at
from tbl_scores_by_question s
left join tbl_peakon_questions q
    on cast(q.id as string) = cast(s.question_id as string)
    and q._is_current = 'Y'
left join tbl_peakon_categories c
    on cast(c.id as string) = cast(q.category_id as string)
    and c._is_current = 'Y'
where s._is_current = 'Y'
    and not (c.`group` is null and c.name is null); --agreed rule: drop when both names are blank
