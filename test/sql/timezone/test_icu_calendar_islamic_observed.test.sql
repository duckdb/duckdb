.mode list
.separator "\t"
.nullvalue NULL
SELECT '@@MARK@@';
SET TimeZone='UTC';
SELECT '@@MARK@@';
SELECT '--- islamic observed ---';
SELECT '@@MARK@@';
SET Calendar='islamic';
SELECT '@@MARK@@';
SELECT ts, year(ts), month(ts), day(ts), era(ts), dayofyear(ts) FROM (VALUES
  ('2024-06-15 12:00:00+00'::TIMESTAMPTZ), ('0622-07-19 12:00:00+00'), ('0622-07-15 12:00:00+00'),
  ('1970-01-01 00:00:00+00'), ('2024-07-07 12:00:00+00'), ('2024-07-08 12:00:00+00'),
  ('2024-03-10 12:00:00+00'), ('2024-03-11 12:00:00+00'), ('2024-04-09 12:00:00+00'),
  ('1900-01-01 12:00:00+00'), ('2100-01-01 12:00:00+00'), ('1500-01-01 12:00:00+00')) t(ts);
SELECT '@@MARK@@';
SELECT make_timestamptz(1445, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1445, 9, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1445, 12, 29, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT date_trunc('year', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_trunc('month', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT last_day('2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT last_day('2024-03-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('year', '1900-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('month', '2023-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT age('2024-06-15 12:00:00+00'::TIMESTAMPTZ, '1900-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_part(['year','month','day','era','dayofyear','week','isoyear'], '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1950-01-01 12:00:00+00'::TIMESTAMPTZ, '2050-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 5 DAY) t(ts);
SELECT '@@MARK@@';
SELECT '--- islamic rgsa ---';
SELECT '@@MARK@@';
SET Calendar='islamic-rgsa';
SELECT '@@MARK@@';
SELECT ts, year(ts), month(ts), day(ts), era(ts), dayofyear(ts) FROM (VALUES
  ('2024-06-15 12:00:00+00'::TIMESTAMPTZ), ('0622-07-19 12:00:00+00'), ('1970-01-01 00:00:00+00'),
  ('2024-07-07 12:00:00+00'), ('1500-01-01 12:00:00+00')) t(ts);
SELECT '@@MARK@@';
SELECT make_timestamptz(1445, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT last_day('2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('year', '1900-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_part(['year','month','day','era','dayofyear','week','isoyear'], '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1950-01-01 12:00:00+00'::TIMESTAMPTZ, '2050-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 5 DAY) t(ts);
