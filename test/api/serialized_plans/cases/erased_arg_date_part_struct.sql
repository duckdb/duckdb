CREATE TABLE t1 (ts TIMESTAMP, d DATE);
INSERT INTO t1 VALUES ('2021-03-04 05:06:07', '2021-03-04'), (NULL, NULL);
SELECT date_part(['year', 'month', 'microsecond'], ts), date_part(['year', 'day'], d) FROM t1 ORDER BY ts;
