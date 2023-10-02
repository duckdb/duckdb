CREATE TEMP TABLE ans AS SELECT x.*, small.id4 AS small_id4, v2 FROM x JOIN small USING (id1);
