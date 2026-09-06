WITH RECURSIVE
file(size) AS (
	SELECT count(*) FROM aoc2022_day20_input
),
mix(iteration, size, pos, num, location) AS (
	SELECT 0,
	       file.size,
	       input.pos,
	       input.num * 811589153,
	       input.pos
	FROM aoc2022_day20_input input, file

	UNION ALL

	SELECT mix.iteration + 1,
	       mix.size,
	       mix.pos,
	       mix.num,
	       row_number() OVER (
	           ORDER BY CASE
	               WHEN mix.pos = aoc2022_day20_modulo(mix.iteration, mix.size)
	                   THEN aoc2022_day20_move(mix.num, mix.location, mix.size)
	               ELSE mix.location
	           END
	       ) - 1
	FROM mix
	WHERE mix.iteration < mix.size * 10
),
decrypted(location, num) AS (
	SELECT location, num
	FROM mix
	WHERE iteration = size * 10
)
SELECT sum(value.num) AS grove,
       md5(string_agg(value.num::VARCHAR, ',' ORDER BY value.location)) AS file_checksum
FROM decrypted zero,
     decrypted value,
     file,
     (VALUES (1000), (2000), (3000)) offsets(distance)
WHERE zero.num = 0
  AND value.location = aoc2022_day20_modulo(zero.location + offsets.distance, file.size);
