.mode list
.separator "\t"
.nullvalue NULL
SELECT '@@MARK@@';
SET TimeZone='UTC';
SELECT '@@MARK@@';
SET Calendar='hebrew';
SELECT '@@MARK@@';
SELECT '--- hebrew dates ---';
SELECT '@@MARK@@';
SELECT ts, year(ts), month(ts), day(ts), era(ts), dayofyear(ts), monthname(ts) FROM (VALUES
  ('2024-06-15 12:00:00+00'::TIMESTAMPTZ), ('1970-01-01 00:00:00+00'), ('2024-10-02 12:00:00+00'),
  ('2024-10-03 12:00:00+00'), ('2023-09-15 12:00:00+00'), ('2023-09-16 12:00:00+00'),
  ('2022-03-01 12:00:00+00'), ('2024-03-01 12:00:00+00'), ('0001-01-01 12:00:00+00'),
  ('1000-01-01 12:00:00+00'), ('2100-01-01 12:00:00+00'), ('1900-01-01 12:00:00+00')) t(ts);
SELECT '@@MARK@@';
SELECT '--- leap months ---';
SELECT '@@MARK@@';
SELECT y, count(*) FROM (SELECT year(ts) y FROM generate_series('2015-01-01 12:00:00+00'::TIMESTAMPTZ, '2035-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 1 DAY) t(ts) GROUP BY ALL) GROUP BY ALL ORDER BY ALL;
SELECT '@@MARK@@';
SELECT y, max(m) FROM (SELECT year(ts) y, month(ts) m FROM generate_series('2015-01-01 12:00:00+00'::TIMESTAMPTZ, '2035-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 1 DAY) t(ts)) GROUP BY ALL ORDER BY ALL;
SELECT '@@MARK@@';
SELECT '--- constructing dates ---';
SELECT '@@MARK@@';
SELECT make_timestamptz(5784, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(5784, 6, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(5784, 7, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(5784, 13, 29, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(5783, 6, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(5783, 13, 29, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT '--- truncation and parts ---';
SELECT '@@MARK@@';
SELECT date_trunc('year', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_trunc('month', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_trunc('quarter', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_trunc('week', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT last_day('2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT last_day('2023-11-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_part(['year','month','day','era','dayofyear','week','isoyear'], '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT '--- differences ---';
SELECT '@@MARK@@';
SELECT date_diff('year', '1900-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('month', '2023-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('month', '2022-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('day', '2023-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT age('2024-06-15 12:00:00+00'::TIMESTAMPTZ, '1900-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT age('2024-03-01 12:00:00+00'::TIMESTAMPTZ, '2022-03-01 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT '--- a sweep over three centuries ---';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2200-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 7 DAY) t(ts);
