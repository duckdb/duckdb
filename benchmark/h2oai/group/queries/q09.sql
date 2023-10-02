CREATE TEMP TABLE ans AS SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM x_group GROUP BY id2, id4;
