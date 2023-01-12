CREATE TEMP TABLE ans AS SELECT id4, id5, quantile_cont(v3, 0.5) AS median_v3, stddev(v3) AS sd_v3 FROM x_group GROUP BY id4, id5;
