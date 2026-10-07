WITH RECURSIVE
points(id, x, y) AS (
	VALUES (1, 2, 10), (2, 2, 6), (3, 11, 11), (4, 6, 9), (5, 6, 4),
	       (6, 1, 2), (7, 5, 10), (8, 4, 9), (9, 10, 12), (10, 7, 5),
	       (11, 9, 11), (12, 4, 6), (13, 3, 10), (14, 3, 8), (15, 6, 11)
),
start_centroids(id, x, y) AS (
	VALUES (1, 2, 6), (2, 5, 10), (3, 6, 11)
),
kmeans(point_id, x, y, cluster_id) USING KEY (point_id, x, y) AS (
	SELECT p.id, p.x, p.y,
		arg_min(c.id, sqrt((p.x - c.x) ** 2 + (p.y - c.y) ** 2))
	FROM points p, start_centroids c
	GROUP BY p.*
	UNION ALL
	(
		WITH centroids(cluster_id, x, y) AS (
			SELECT cluster_id, avg(x), avg(y)
			FROM recurring.kmeans
			GROUP BY cluster_id
		)
		SELECT p.point_id, p.x, p.y,
			arg_min(c.cluster_id, sqrt((p.x - c.x) ** 2 + (p.y - c.y) ** 2)) AS new_cluster
		FROM recurring.kmeans p, centroids c
		GROUP BY p.*
		HAVING new_cluster != p.cluster_id
	)
)
SELECT count(*) AS points, sum(cluster_id)::BIGINT AS cluster_checksum,
	sum(point_id * cluster_id)::BIGINT AS assignment_checksum
FROM kmeans;
