.mode list
.separator "\t"
.nullvalue NULL
SELECT '@@MARK@@';
SET TimeZone='UTC';
SELECT '@@MARK@@';
SELECT '--- coptic ---';
SELECT '@@MARK@@';
SET Calendar='coptic';
SELECT '@@MARK@@';
SELECT ts, year(ts), month(ts), day(ts), era(ts), dayofyear(ts) FROM (VALUES
  ('2024-06-15 12:00:00+00'::TIMESTAMPTZ), ('0284-08-29 12:00:00+00'), ('0283-08-29 12:00:00+00'),
  ('1970-01-01 00:00:00+00'), ('2024-09-10 12:00:00+00'), ('2024-09-11 12:00:00+00'),
  ('2023-09-11 12:00:00+00'), ('2027-09-11 12:00:00+00'), ('0001-01-01 12:00:00+00')) t(ts);
SELECT '@@MARK@@';
SELECT make_timestamptz(1740, 13, 5, 12, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1739, 13, 6, 12, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1740, 13, 6, 12, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1740, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT '2024-06-15 12:00:00+00'::TIMESTAMPTZ + INTERVAL 1 MONTH;
SELECT '@@MARK@@';
SELECT '2024-06-15 12:00:00+00'::TIMESTAMPTZ + INTERVAL 13 MONTH;
SELECT '@@MARK@@';
SELECT '2024-06-15 12:00:00+00'::TIMESTAMPTZ + INTERVAL 1 YEAR;
SELECT '@@MARK@@';
SELECT date_trunc('year', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_trunc('month', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_trunc('quarter', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_trunc('decade', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT last_day('2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT last_day('2024-09-05 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('year', '1900-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('month', '2023-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT age('2024-06-15 12:00:00+00'::TIMESTAMPTZ, '1900-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_part(['year','month','day','era','dayofyear','week','isoyear'], '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT * FROM generate_series('2024-09-05 12:00:00+00'::TIMESTAMPTZ, '2024-09-15 12:00:00+00'::TIMESTAMPTZ, INTERVAL 2 DAY);
SELECT '@@MARK@@';
SELECT '--- ethiopic ---';
SELECT '@@MARK@@';
SET Calendar='ethiopic';
SELECT '@@MARK@@';
SELECT ts, year(ts), month(ts), day(ts), era(ts), dayofyear(ts) FROM (VALUES
  ('2024-06-15 12:00:00+00'::TIMESTAMPTZ), ('0008-08-29 12:00:00+00'), ('0007-08-29 12:00:00+00'),
  ('1970-01-01 00:00:00+00'), ('2024-09-10 12:00:00+00'), ('2024-09-11 12:00:00+00'),
  ('0001-01-01 12:00:00+00'), ('5500-01-01 (BC) 12:00:00+00')) t(ts);
SELECT '@@MARK@@';
SELECT make_timestamptz(2016, 13, 5, 12, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(2016, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT '2024-06-15 12:00:00+00'::TIMESTAMPTZ + INTERVAL 1 YEAR;
SELECT '@@MARK@@';
SELECT date_trunc('year', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT last_day('2024-09-05 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('year', '1900-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_part(['year','month','day','era','dayofyear','week','isoyear'], '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT '--- ethiopic amete alem ---';
SELECT '@@MARK@@';
SET Calendar='ethiopic-amete-alem';
SELECT '@@MARK@@';
SELECT ts, year(ts), month(ts), day(ts), era(ts), dayofyear(ts) FROM (VALUES
  ('2024-06-15 12:00:00+00'::TIMESTAMPTZ), ('0008-08-29 12:00:00+00'), ('0007-08-29 12:00:00+00'),
  ('1970-01-01 00:00:00+00'), ('0001-01-01 12:00:00+00'), ('5500-01-01 (BC) 12:00:00+00')) t(ts);
SELECT '@@MARK@@';
SELECT make_timestamptz(7516, 13, 5, 12, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(7516, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT '2024-06-15 12:00:00+00'::TIMESTAMPTZ + INTERVAL 1 YEAR;
SELECT '@@MARK@@';
SELECT date_trunc('year', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('year', '1900-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_part(['year','month','day','era','dayofyear','week','isoyear'], '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
