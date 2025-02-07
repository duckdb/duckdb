CREATE TEMP TABLE ans AS SELECT id1, id2, sum(v1) AS v1 FROM x_group GROUP BY id1, id2;

