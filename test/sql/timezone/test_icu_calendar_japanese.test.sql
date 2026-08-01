.mode list
.separator "\t"
.nullvalue NULL
SELECT '@@MARK@@';
SET TimeZone='UTC';
SELECT '@@MARK@@';
SET Calendar='japanese';
SELECT '@@MARK@@';
SELECT '--- era boundaries ---';
SELECT '@@MARK@@';
SELECT ts, era(ts), year(ts), month(ts), day(ts) FROM (VALUES
  ('2024-06-15 12:00:00+00'::TIMESTAMPTZ), ('2019-04-30 12:00:00+00'), ('2019-05-01 12:00:00+00'),
  ('1989-01-07 12:00:00+00'), ('1989-01-08 12:00:00+00'), ('1926-12-24 12:00:00+00'),
  ('1926-12-25 12:00:00+00'), ('1912-07-29 12:00:00+00'), ('1912-07-30 12:00:00+00'),
  ('1868-10-22 12:00:00+00'), ('1868-10-23 12:00:00+00'), ('0645-06-18 12:00:00+00'),
  ('0645-06-19 12:00:00+00'), ('0600-01-01 12:00:00+00'), ('1582-10-04 12:00:00+00'),
  ('1582-10-15 12:00:00+00')) t(ts);
SELECT '@@MARK@@';
SELECT '--- constructing dates ---';
SELECT '@@MARK@@';
SELECT make_timestamptz(6, 6, 15, 12, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(1, 1, 1, 0, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(31, 4, 30, 12, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT make_timestamptz(64, 1, 7, 12, 0, 0.0, 'UTC');
SELECT '@@MARK@@';
SELECT '--- truncation and parts ---';
SELECT '@@MARK@@';
SELECT date_trunc('year', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_trunc('year', '2019-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_trunc('month', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_trunc('decade', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_trunc('era', '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT last_day('2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_part(['year','month','day','era','dayofyear','week','isoyear'], '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT '--- arithmetic ---';
SELECT '@@MARK@@';
SELECT '2024-06-15 12:00:00+00'::TIMESTAMPTZ + INTERVAL 1 YEAR;
SELECT '@@MARK@@';
SELECT '2019-04-30 12:00:00+00'::TIMESTAMPTZ + INTERVAL 1 YEAR;
SELECT '@@MARK@@';
SELECT '2019-04-30 12:00:00+00'::TIMESTAMPTZ + INTERVAL 1 MONTH;
SELECT '@@MARK@@';
SELECT '2019-04-30 12:00:00+00'::TIMESTAMPTZ + INTERVAL 1 DAY;
SELECT '@@MARK@@';
SELECT date_diff('year', '1900-06-15 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT date_diff('year', '2019-01-01 12:00:00+00'::TIMESTAMPTZ, '2024-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT age('2024-06-15 12:00:00+00'::TIMESTAMPTZ, '1900-06-15 12:00:00+00'::TIMESTAMPTZ);
SELECT '@@MARK@@';
SELECT '--- a sweep across every era ---';
SELECT '@@MARK@@';
SELECT count(*), count(distinct era) FROM (SELECT era(ts) era FROM generate_series('0600-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 31 DAY) t(ts));
SELECT '@@MARK@@';
SELECT sum(hash(ts::VARCHAR)) FROM generate_series('0600-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 13 DAY) t(ts);
