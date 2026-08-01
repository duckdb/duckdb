.mode list
.separator "\t"
.nullvalue NULL
SELECT '@@MARK@@';
SET TimeZone='UTC';
SELECT '@@MARK@@';
SELECT '--- every zone at a spread of instants ---';
SELECT '@@MARK@@';
WITH instants(ts) AS (VALUES
  ('1700-06-15 12:00:00+00'::TIMESTAMPTZ), ('1850-01-01 00:00:00+00'), ('1900-06-15 12:00:00+00'),
  ('1918-03-31 12:00:00+00'), ('1942-06-15 12:00:00+00'), ('1970-01-01 00:00:00+00'),
  ('1985-07-01 12:00:00+00'), ('2000-01-01 00:00:00+00'), ('2011-12-30 12:00:00+00'),
  ('2020-03-29 01:30:00+00'), ('2024-06-15 12:00:00+00'), ('2038-01-19 03:14:07+00'),
  ('2100-11-07 12:00:00+00'), ('2400-01-01 00:00:00+00'))
SELECT count(*), sum(hash(strftime(timezone(name, ts), '%Y-%m-%d %H:%M:%S')))
FROM pg_timezone_names(), instants;
SELECT '@@MARK@@';
SELECT '--- every zone, naive to instant, which resolves ambiguous local times ---';
SELECT '@@MARK@@';
WITH locals(ts) AS (VALUES
  ('1900-01-01 00:00:00'::TIMESTAMP), ('1970-01-01 00:00:00'), ('2020-03-29 02:30:00'),
  ('2020-10-25 02:30:00'), ('2024-06-15 12:00:00'), ('2100-01-01 00:00:00'))
SELECT count(*), sum(hash(timezone(name, ts)::VARCHAR))
FROM pg_timezone_names(), locals;
SELECT '@@MARK@@';
SELECT '--- the zone list ---';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(name)), sum(hash(abbrev)) FROM pg_timezone_names();
SELECT '@@MARK@@';
SELECT '--- every calendar over two centuries ---';
SELECT '@@MARK@@';
SELECT name FROM icu_calendar_names() ORDER BY 1;
SELECT '@@MARK@@';
SELECT '--- every calendar over two centuries ---';
SELECT '@@MARK@@';
SET Calendar='gregorian';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
SELECT '@@MARK@@';
SET Calendar='buddhist';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
SELECT '@@MARK@@';
SET Calendar='roc';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
SELECT '@@MARK@@';
SET Calendar='iso8601';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
SELECT '@@MARK@@';
SET Calendar='coptic';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
SELECT '@@MARK@@';
SET Calendar='ethiopic';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
SELECT '@@MARK@@';
SET Calendar='ethiopic-amete-alem';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
SELECT '@@MARK@@';
SET Calendar='persian';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
SELECT '@@MARK@@';
SET Calendar='indian';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
SELECT '@@MARK@@';
SET Calendar='islamic';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
SELECT '@@MARK@@';
SET Calendar='islamic-civil';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
SELECT '@@MARK@@';
SET Calendar='islamic-rgsa';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
SELECT '@@MARK@@';
SET Calendar='islamic-tbla';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
SELECT '@@MARK@@';
SET Calendar='islamic-umalqura';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
SELECT '@@MARK@@';
SET Calendar='hebrew';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
SELECT '@@MARK@@';
SET Calendar='japanese';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
SELECT '@@MARK@@';
SET Calendar='chinese';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
SELECT '@@MARK@@';
SET Calendar='dangi';
SELECT '@@MARK@@';
SELECT count(*), sum(hash(ts::VARCHAR)) FROM generate_series('1900-01-01 12:00:00+00'::TIMESTAMPTZ, '2100-01-01 12:00:00+00'::TIMESTAMPTZ, INTERVAL 11 DAY) t(ts);
