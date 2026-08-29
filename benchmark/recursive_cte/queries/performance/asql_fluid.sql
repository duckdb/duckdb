WITH RECURSIVE
simulation(iteration, x, value) AS (
	SELECT 0, initial.x, initial.value
	FROM asql_fluid_initial AS initial
	UNION ALL
	SELECT current.iteration + 1,
	       current.x,
	       current.value + coalesce(influences.delta, 0)
	FROM simulation AS current
	     JOIN asql_fluid_config AS config ON true
	     LEFT JOIN (
		     SELECT effect.x, sum(effect.delta) AS delta
		     FROM simulation AS source,
		          asql_fluid_config AS config,
		          LATERAL unnest([
			          struct_pack(
				          x := source.x,
				          delta := -config.rate * source.value
				                   * ((source.x > 0)::INTEGER
				                      + (source.x + 1 < config.width)::INTEGER)
			          ),
			          struct_pack(x := source.x - 1, delta := config.rate * source.value),
			          struct_pack(x := source.x + 1, delta := config.rate * source.value)
		          ]) AS effects(effect)
		     WHERE effect.x BETWEEN 0 AND config.width - 1
		     GROUP BY effect.x
	     ) AS influences ON current.x = influences.x
	WHERE current.iteration < config.iterations
),
final_state AS (
	SELECT simulation.*
	FROM simulation, asql_fluid_config AS config
	WHERE simulation.iteration = config.iterations
)
SELECT max(iteration)::INTEGER AS iterations,
       count(*)::BIGINT AS cells,
       round(sum(value), 6) AS total,
       round(min(value), 6) AS minimum,
       round(max(value), 6) AS maximum,
       md5(string_agg(round(value, 9)::VARCHAR, ',' ORDER BY x)) AS checksum
FROM final_state;
