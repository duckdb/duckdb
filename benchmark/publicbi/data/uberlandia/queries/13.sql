SELECT CAST(EXTRACT(YEAR FROM "Uberlandia_1"."data_de_inicio") AS BIGINT) AS "yr:data_de_inicio:ok" FROM "Uberlandia_1" GROUP BY "yr:data_de_inicio:ok" ORDER BY "yr:data_de_inicio:ok" ASC ;
