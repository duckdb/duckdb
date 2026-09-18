WITH RECURSIVE
knot1(move, xy) AS MATERIALIZED (
	SELECT 0, {x: 0, y: 0}
	UNION ALL
	SELECT head.move,
	       aoc2022_day09_follow(
	           knot1.xy,
	           head.xy.x - knot1.xy.x,
	           head.xy.y - knot1.xy.y
	       )
	FROM knot1, aoc2022_day09_head head
	WHERE knot1.move + 1 = head.move
),
knot2(move, xy) AS MATERIALIZED (
	SELECT 0, {x: 0, y: 0}
	UNION ALL
	SELECT head.move,
	       aoc2022_day09_follow(
	           knot2.xy,
	           head.xy.x - knot2.xy.x,
	           head.xy.y - knot2.xy.y
	       )
	FROM knot2, knot1 head
	WHERE knot2.move + 1 = head.move
),
knot3(move, xy) AS MATERIALIZED (
	SELECT 0, {x: 0, y: 0}
	UNION ALL
	SELECT head.move,
	       aoc2022_day09_follow(
	           knot3.xy,
	           head.xy.x - knot3.xy.x,
	           head.xy.y - knot3.xy.y
	       )
	FROM knot3, knot2 head
	WHERE knot3.move + 1 = head.move
),
knot4(move, xy) AS MATERIALIZED (
	SELECT 0, {x: 0, y: 0}
	UNION ALL
	SELECT head.move,
	       aoc2022_day09_follow(
	           knot4.xy,
	           head.xy.x - knot4.xy.x,
	           head.xy.y - knot4.xy.y
	       )
	FROM knot4, knot3 head
	WHERE knot4.move + 1 = head.move
),
knot5(move, xy) AS MATERIALIZED (
	SELECT 0, {x: 0, y: 0}
	UNION ALL
	SELECT head.move,
	       aoc2022_day09_follow(
	           knot5.xy,
	           head.xy.x - knot5.xy.x,
	           head.xy.y - knot5.xy.y
	       )
	FROM knot5, knot4 head
	WHERE knot5.move + 1 = head.move
),
knot6(move, xy) AS MATERIALIZED (
	SELECT 0, {x: 0, y: 0}
	UNION ALL
	SELECT head.move,
	       aoc2022_day09_follow(
	           knot6.xy,
	           head.xy.x - knot6.xy.x,
	           head.xy.y - knot6.xy.y
	       )
	FROM knot6, knot5 head
	WHERE knot6.move + 1 = head.move
),
knot7(move, xy) AS MATERIALIZED (
	SELECT 0, {x: 0, y: 0}
	UNION ALL
	SELECT head.move,
	       aoc2022_day09_follow(
	           knot7.xy,
	           head.xy.x - knot7.xy.x,
	           head.xy.y - knot7.xy.y
	       )
	FROM knot7, knot6 head
	WHERE knot7.move + 1 = head.move
),
knot8(move, xy) AS MATERIALIZED (
	SELECT 0, {x: 0, y: 0}
	UNION ALL
	SELECT head.move,
	       aoc2022_day09_follow(
	           knot8.xy,
	           head.xy.x - knot8.xy.x,
	           head.xy.y - knot8.xy.y
	       )
	FROM knot8, knot7 head
	WHERE knot8.move + 1 = head.move
),
tail(move, xy) AS (
	SELECT 0, {x: 0, y: 0}
	UNION ALL
	SELECT head.move,
	       aoc2022_day09_follow(
	           tail.xy,
	           head.xy.x - tail.xy.x,
	           head.xy.y - tail.xy.y
	       )
	FROM tail, knot8 head
	WHERE tail.move + 1 = head.move
)
SELECT count(*) AS positions,
       count(DISTINCT xy) AS visited,
       sum(xy.x) AS x_sum,
       sum(xy.y) AS y_sum,
       md5(string_agg(xy.x || ',' || xy.y, ';' ORDER BY move)) AS path_checksum
FROM tail;
