WITH RECURSIVE
decimal(total) AS (
	SELECT sum(value)
	FROM aoc2022_day25_values
),
snafu(decimal, encoded, digits) AS (
	SELECT total, ''::VARCHAR, 0
	FROM decimal

	UNION ALL

	SELECT (decimal + 2) // 5,
	       CASE ((decimal + 2) % 5)::INTEGER
	           WHEN 0 THEN '='
	           WHEN 1 THEN '-'
	           WHEN 2 THEN '0'
	           WHEN 3 THEN '1'
	           WHEN 4 THEN '2'
	       END || encoded,
	       digits + 1
	FROM snafu
	WHERE decimal > 0
),
final(encoded, digits) AS (
	SELECT encoded, digits
	FROM snafu
	WHERE decimal = 0
),
decoded(place, value) AS (
	SELECT 1, 0::HUGEINT

	UNION ALL

	SELECT place + 1,
	       value * 5 + CASE substr(encoded, place, 1)
	           WHEN '=' THEN -2
	           WHEN '-' THEN -1
	           WHEN '0' THEN 0
	           WHEN '1' THEN 1
	           WHEN '2' THEN 2
	       END
	FROM decoded, final
	WHERE place <= length(encoded)
)
SELECT final.encoded,
       final.digits,
       (SELECT total FROM decimal),
       decoded.value = (SELECT total FROM decimal),
       md5(final.encoded)
FROM final
JOIN decoded ON decoded.place = final.digits + 1;
