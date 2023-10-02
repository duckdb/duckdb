CREATE TEMP TABLE ans AS SELECT id3, max(v1)-min(v2) AS range_v1_v2 FROM x_group GROUP BY id3;
