SELECT trim(splitpart("IGlocations1_1"."City", ' ', 1),' \\t\ \\x0b\\f\\r') AS "City - Split 1" FROM "IGlocations1_1" GROUP BY "City - Split 1" ORDER BY "City - Split 1" ASC ;
