.mode list
.separator "\t"
.nullvalue NULL
SELECT '@@MARK@@';
SET TimeZone='UTC';
SELECT '@@MARK@@';
SELECT '--- chinese ---';
SELECT '@@MARK@@';
SET Calendar='chinese';
SELECT '@@MARK@@';
SELECT ts, era(ts), year(ts), month(ts), day(ts), dayofyear(ts) FROM (VALUES
  ('2024-06-15 12:00:00+00'::TIMESTAMPTZ), ('2024-02-09 12:00:00+00'), ('2024-02-10 12:00:00+00'),
  ('2023-01-21 12:00:00+00'), ('2023-01-22 12:00:00+00'), ('2023-03-21 12:00:00+00'),
  ('2023-03-22 12:00:00+00'), ('1970-01-01 00:00:00+00'), ('1900-01-01 12:00:00+00'),
  ('2033-11-01 12:00:00+00'), ('2033-12-01 12:00:00+00'), ('2034-01-01 12:00:00+00'),
  ('1582-10-04 12:00:00+00'), ('1582-10-15 12:00:00+00'), ('0001-01-01 12:00:00+00')) t(ts);
SELECT '@@MARK@@';
SELECT make_timestamptz(78, 41, 5, 12, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(78, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT date_trunc('year', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_trunc('month', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_trunc('day', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_trunc('week', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT last_day('2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT last_day('2024-02-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('year', '1900-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('month', '2023-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('day', '2023-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT age('2024-06-15 12:00:00+00'::TIMESTAMPTZ, '1900-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_part(['year','month','day','era','dayofyear','week','isoyear'], '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 5 DAY) t(ts);
SELECT '@@MARK@@';
SELECT y, count(distinct m) FROM (SELECT year(ts) y, month(ts) m FROM generate_series('2020-01-01 12:00:00+00'::TIMESTAMPTZ, '2030-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 1 DAY) t(ts)) GROUP BY ALL ORDER BY ALL;
SELECT '@@MARK@@';
SELECT '--- dangi ---';
SELECT '@@MARK@@';
SET Calendar='dangi';
SELECT '@@MARK@@';
SELECT ts, era(ts), year(ts), month(ts), day(ts), dayofyear(ts) FROM (VALUES
  ('2024-06-15 12:00:00+00'::TIMESTAMPTZ), ('1970-01-01 00:00:00+00'), ('1900-01-01 12:00:00+00'),
  ('1896-06-15 12:00:00+00'), ('1897-06-15 12:00:00+00'), ('1898-06-15 12:00:00+00'),
  ('1911-06-15 12:00:00+00'), ('1912-06-15 12:00:00+00'), ('0001-01-01 12:00:00+00')) t(ts);
SELECT '@@MARK@@';
SELECT make_timestamptz(78, 41, 5, 12, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT date_trunc('year', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_trunc('month', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT last_day('2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('year', '1900-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_part(['year','month','day','era','dayofyear','week','isoyear'], '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1880-01-01 12:00:00+00'::TIMESTAMPTZ, '1930-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 3 DAY) t(ts);
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1950-01-01 12:00:00+00'::TIMESTAMPTZ, '2050-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 5 DAY) t(ts);
