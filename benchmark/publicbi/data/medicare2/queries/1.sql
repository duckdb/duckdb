SELECT "Medicare2_1"."provider_type" AS "provider_type" FROM "Medicare2_1" WHERE ("Medicare2_1"."nppes_provider_state" = 'NY') GROUP BY "Medicare2_1"."provider_type" ORDER BY "provider_type" ASC ;
