SELECT CAST(EXTRACT(YEAR FROM "Eixo_1"."data_de_inicio") AS BIGINT) AS "yr:data_de_inicio:ok" FROM "Eixo_1" GROUP BY "yr:data_de_inicio:ok" ORDER BY "yr:data_de_inicio:ok" ASC ;
