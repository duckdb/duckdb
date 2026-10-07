WITH RECURSIVE
params(point_count, cluster_count, max_iterations) AS (
	VALUES (10000, 8, 20)
),
points(id, x, y) AS (
	SELECT point_id::INTEGER,
		((point_id * 37) % 10000)::INTEGER,
		((point_id * 101 + (point_id % 17) * 53) % 10000)::INTEGER
	FROM params, range(1, point_count + 1) points(point_id)
),
start_centroids(id, x, y) AS (
	SELECT cluster_id::INTEGER,
		((cluster_id * 1250 * 37) % 10000)::INTEGER,
		((cluster_id * 1250 * 101 + ((cluster_id * 1250) % 17) * 53) % 10000)::INTEGER
	FROM params, range(1, cluster_count + 1) clusters(cluster_id)
),
kmeans(point_id, x, y, cluster_id, iter) USING KEY (point_id, x, y) AS (
	SELECT point.id, point.x, point.y,
		arg_min(centroid.id, (point.x - centroid.x) ** 2 + (point.y - centroid.y) ** 2), 0
	FROM points point, start_centroids centroid
	GROUP BY point.*
	UNION ALL
	(
		WITH centroids(cluster_id, x, y) AS (
			SELECT cluster_id, avg(x), avg(y)
			FROM recurring.kmeans
			GROUP BY cluster_id
		)
		SELECT point.point_id, point.x, point.y,
			arg_min(centroid.cluster_id,
			        (point.x - centroid.x) ** 2 + (point.y - centroid.y) ** 2) AS new_cluster,
			point.iter + 1
		FROM recurring.kmeans point, centroids centroid, params
		WHERE point.iter < max_iterations
		GROUP BY point.*
		HAVING new_cluster != point.cluster_id
	)
)
SELECT count(*) AS points, sum(cluster_id)::BIGINT AS cluster_checksum,
	sum(point_id * cluster_id)::BIGINT AS assignment_checksum,
	max(iter) AS maximum_iterations
FROM kmeans;
