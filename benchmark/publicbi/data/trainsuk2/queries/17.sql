SELECT SUM(CAST("TrainsUK2_2"."Number of Records" AS BIGINT)) AS "sum:Number of Records:ok" FROM "TrainsUK2_2" HAVING (COUNT(1) > 0) ORDER BY "sum:Number of Records:ok";
