# unnest() is consistent with data frames

    Code
      dbplyr::sql_render(tidyr::unnest(tbl_nested, data))
    Output
      <SQL> SELECT
        "groups",
        STRUCT_EXTRACT("data", 'num') AS "num",
        STRUCT_EXTRACT("data", 'char') AS "char"
      FROM (
        SELECT "groups", UNNEST("data") AS "data"
        FROM "dbplyr_001"
      ) "q01"

# unpack() is consistent with data frames

    Code
      dbplyr::sql_render(unpack.tbl_duckdb_connection(tbl_packed, data))
    Output
      <SQL> SELECT
        "groups",
        STRUCT_EXTRACT("data", 'num') AS "num",
        STRUCT_EXTRACT("data", 'char') AS "char"
      FROM "dbplyr_002"

