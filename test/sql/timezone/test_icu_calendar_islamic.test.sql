.mode list
.separator "\t"
.nullvalue NULL
SELECT '@@MARK@@';
SET TimeZone='UTC';
SELECT '@@MARK@@';
SELECT '--- islamic civil ---';
SELECT '@@MARK@@';
SET Calendar='islamic-civil';
SELECT '@@MARK@@';
SELECT ts, year(ts), month(ts), day(ts), era(ts), dayofyear(ts) FROM (VALUES
  ('2024-06-15 12:00:00+00'::TIMESTAMPTZ), ('0622-07-19 12:00:00+00'), ('0622-07-18 12:00:00+00'),
  ('1970-01-01 00:00:00+00'), ('2024-07-07 12:00:00+00'), ('2024-07-08 12:00:00+00'),
  ('1882-11-12 12:00:00+00'), ('2100-01-01 12:00:00+00'), ('1000-01-01 12:00:00+00')) t(ts);
SELECT '@@MARK@@';
SELECT make_timestamptz(1445, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1445, 12, 29, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1445, 12, 30, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1444, 12, 30, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT date_trunc('year', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_trunc('month', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT last_day('2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT last_day('2024-07-20 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('year', '1900-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('month', '2023-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT age('2024-06-15 12:00:00+00'::TIMESTAMPTZ, '1900-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_part(['year','month','day','era','dayofyear','week','isoyear'], '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT * FROM generate_series('2024-07-01 12:00:00+00'::TIMESTAMPTZ, '2024-07-12 12:00:00+00'::TIMESTAMPTZ, INTERVAL 3 DAY);
SELECT '@@MARK@@';
SELECT '--- islamic tbla ---';
SELECT '@@MARK@@';
SET Calendar='islamic-tbla';
SELECT '@@MARK@@';
SELECT ts, year(ts), month(ts), day(ts), era(ts), dayofyear(ts) FROM (VALUES
  ('2024-06-15 12:00:00+00'::TIMESTAMPTZ), ('0622-07-19 12:00:00+00'), ('0622-07-18 12:00:00+00'),
  ('1970-01-01 00:00:00+00'), ('2024-07-07 12:00:00+00'), ('1000-01-01 12:00:00+00')) t(ts);
SELECT '@@MARK@@';
SELECT make_timestamptz(1445, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT last_day('2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('year', '1900-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_part(['year','month','day','era','dayofyear','week','isoyear'], '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT '--- islamic umalqura ---';
SELECT '@@MARK@@';
SET Calendar='islamic-umalqura';
SELECT '@@MARK@@';
SELECT ts, year(ts), month(ts), day(ts), era(ts), dayofyear(ts) FROM (VALUES
  ('2024-06-15 12:00:00+00'::TIMESTAMPTZ), ('1882-11-12 12:00:00+00'), ('1882-11-11 12:00:00+00'),
  ('1970-01-01 00:00:00+00'), ('2024-07-07 12:00:00+00'), ('2024-07-08 12:00:00+00'),
  ('2077-11-16 12:00:00+00'), ('2077-11-17 12:00:00+00'), ('2200-01-01 12:00:00+00'),
  ('1000-01-01 12:00:00+00'), ('1900-01-01 12:00:00+00'), ('2000-01-01 12:00:00+00')) t(ts);
SELECT '@@MARK@@';
SELECT make_timestamptz(1445, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1445, 12, 29, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1300, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1600, 12, 29, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1601, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1299, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT date_trunc('year', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_trunc('month', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT last_day('2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('year', '1900-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('month', '2023-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT age('2024-06-15 12:00:00+00'::TIMESTAMPTZ, '1900-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_part(['year','month','day','era','dayofyear','week','isoyear'], '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT * FROM generate_series('2024-07-01 12:00:00+00'::TIMESTAMPTZ, '2024-07-12 12:00:00+00'::TIMESTAMPTZ, INTERVAL 3 DAY);
SELECT '@@MARK@@';
SELECT count(*), min(y), max(y) FROM (SELECT year(ts) y FROM generate_series('1880-01-01 12:00:00+00'::TIMESTAMPTZ, '2180-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 37 DAY) t(ts));
SELECT '@@MARK@@';
SELECT sum(hash(ts::VARCHAR)) FROM generate_series('1880-01-01 12:00:00+00'::TIMESTAMPTZ, '2180-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
