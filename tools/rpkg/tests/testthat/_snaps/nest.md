# nest() is consistent with data frames

    Code
      dbplyr::sql_render(tidyr::nest(tbl, data = -groups))
    Output
      <SQL> SELECT "groups", LIST("data") AS "data"
      FROM (
        SELECT "groups", ROW("num", "char") AS "data"
        FROM "data"
      ) "q01"
      GROUP BY "groups"

# pack() is consistent with data frames

    Code
      dbplyr::sql_render(pack.tbl_duckdb_connection(tbl, data = -groups))
    Output
      <SQL> SELECT "groups", ROW("num", "char") AS "data"
      FROM "data"

