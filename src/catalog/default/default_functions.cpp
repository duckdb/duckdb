#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

static const DefaultMacro internal_macros[] = {
    {DEFAULT_SCHEMA, "current_role", "() AS 'duckdb'"}, // user name of current execution context
    {DEFAULT_SCHEMA, "current_user", "() AS 'duckdb'"}, // user name of current execution context
    {DEFAULT_SCHEMA, "current_catalog",
     "() AS main.current_database()"},              // name of current database (called "catalog" in the SQL standard)
    {DEFAULT_SCHEMA, "user", "() AS current_user"}, // equivalent to current_user
    {DEFAULT_SCHEMA, "session_user", "() AS 'duckdb'"}, // session user name
    {"pg_catalog", "inet_client_addr", "() AS NULL"},   // address of the remote connection
    {"pg_catalog", "inet_client_port", "() AS NULL"},   // port of the remote connection
    {"pg_catalog", "inet_server_addr", "() AS NULL"},   // address of the local connection
    {"pg_catalog", "inet_server_port", "() AS NULL"},   // port of the local connection
    {"pg_catalog", "pg_my_temp_schema", "() AS 0"},     // OID of session's temporary schema, or 0 if none
    {"pg_catalog", "pg_is_other_temp_schema", "(schema_id) AS false"}, // is schema another session's temporary schema?

    {"pg_catalog", "pg_conf_load_time", "() AS current_timestamp"},        // configuration load time
    {"pg_catalog", "pg_postmaster_start_time", "() AS current_timestamp"}, // server start time

    {"pg_catalog", "pg_typeof", "(expression) AS lower(typeof(expression))"}, // get the data type of any value

    {"pg_catalog", "current_database",
     "() AS system.main.current_database()"}, // name of current database (called "catalog" in the SQL standard)
    {"pg_catalog", "current_query",
     "() AS system.main.current_query()"}, // the currently executing query (NULL if not inside a plpgsql function)
    {"pg_catalog", "current_schema", "() AS system.main.current_schema()"}, // name of current schema
    {"pg_catalog", "current_schemas",
     "(include_implicit) AS system.main.current_schemas(include_implicit)"}, // names of schemas in search path

    // privilege functions
    {"pg_catalog", "has_any_column_privilege",
     "(\"table\", privilege) AS true, (user, \"table\", privilege) AS true"}, // boolean  //does current/named user have
                                                                              // privilege for any column of table
    {"pg_catalog", "has_column_privilege",
     "(\"table\", \"column\", privilege) AS true, (user, \"table\", \"column\", privilege) AS true"}, // boolean  //does
                                                                                                      // current/named
                                                                                                      // user have
                                                                                                      // privilege for
                                                                                                      // column
    {"pg_catalog", "has_database_privilege",
     "(database, privilege) AS true, (user, database, privilege) AS true"}, // boolean  //does current/named user have
                                                                            // privilege for database
    {"pg_catalog", "has_foreign_data_wrapper_privilege",
     "(fdw, privilege) AS true, (user, fdw, privilege) AS true"}, // boolean  //does current/named user have privilege
                                                                  // for foreign-data wrapper
    {"pg_catalog", "has_function_privilege",
     "(function, privilege) AS true, (user, function, privilege) AS true"}, // boolean  //does current/named user have
                                                                            // privilege for function
    {"pg_catalog", "has_language_privilege",
     "(language, privilege) AS true, (user, language, privilege) AS true"}, // boolean  //does current/named user have
                                                                            // privilege for language
    {"pg_catalog", "has_schema_privilege",
     "(schema, privilege) AS true, (user, schema, privilege) AS true"}, // boolean  //does current/named user have
                                                                        // privilege for schema
    {"pg_catalog", "has_sequence_privilege",
     "(sequence, privilege) AS true, (user, sequence, privilege) AS true"}, // boolean  //does current/named user have
                                                                            // privilege for sequence
    {"pg_catalog", "has_server_privilege",
     "(server, privilege) AS true, (user, server, privilege) AS true"}, // boolean  //does current/named user have
                                                                        // privilege for foreign server
    {"pg_catalog", "has_table_privilege",
     "(\"table\", privilege) AS true, (user, \"table\", privilege) AS true"}, // boolean  //does current/named user have
                                                                              // privilege for table
    {"pg_catalog", "has_tablespace_privilege",
     "(tablespace, privilege) AS true, (user, tablespace, privilege) AS true"}, // boolean  //does current/named user
                                                                                // have privilege for tablespace

    // various postgres system functions
    {"pg_catalog", "pg_get_viewdef", "(oid) AS (select sql from duckdb_views() v where v.view_oid=oid)"},
    {"pg_catalog", "pg_get_constraintdef",
     "(constraint_oid) AS (select constraint_text from duckdb_constraints() d_constraint where "
     "d_constraint.table_oid=constraint_oid//1000000 and d_constraint.constraint_index=constraint_oid%1000000), "
     "(constraint_oid, pretty_bool) AS pg_get_constraintdef(constraint_oid)"},
    {"pg_catalog", "pg_get_expr", "(pg_node_tree, relation_oid) AS pg_node_tree"},
    {"pg_catalog", "format_pg_type",
     "(logical_type, type_name) AS case upper(logical_type) when 'FLOAT' then 'float4' when 'DOUBLE' then 'float8' "
     "when 'DECIMAL' then 'numeric' when 'ENUM' then lower(type_name) when 'VARCHAR' then 'varchar' when 'BLOB' then "
     "'bytea' when 'TIMESTAMP' then 'timestamp' when 'TIME' then 'time' when 'TIMESTAMP WITH TIME ZONE' then "
     "'timestamptz' when 'TIME WITH TIME ZONE' then 'timetz' when 'SMALLINT' then 'int2' when 'INTEGER' then 'int4' "
     "when 'BIGINT' then 'int8' when 'BOOLEAN' then 'bool' else lower(logical_type) end"},
    {"pg_catalog", "format_type",
     "(type_oid, typemod) AS (select format_pg_type(logical_type, type_name) from duckdb_types() t where "
     "t.type_oid=type_oid) || case when typemod>0 then concat('(', typemod//1000, ',', typemod%1000, ')') else '' end"},
    {"pg_catalog", "map_to_pg_oid",
     "(type_name) AS case type_name when 'bool' then 16 when 'int16' then 21 when 'int' then 23 when 'bigint' then 20 "
     "when 'date' then 1082 when 'time' then 1083 when 'datetime' then 1114 when 'dec' then 1700 when 'float' then 700 "
     "when 'double' then 701 when 'bpchar' then 1043 when 'binary' then 17 when 'interval' then 1186 when "
     "'timestamptz' then 1184 when 'timestamp with time zone' then 1184 when 'timetz' then 1266 when 'time with time "
     "zone' then 1266 when 'bit' then 1560 when 'guid' then 2950 else null end"}, // map duckdb_oid to pg_oid. If no
                                                                                  // corresponding type, return null

    {"pg_catalog", "pg_has_role",
     "(user, role, privilege) AS true, (role, privilege) AS true"}, // boolean  //does current/named user have privilege
                                                                    // for role

    {"pg_catalog", "col_description", "(table_oid, column_number) AS NULL"}, // get comment for a table column
    {"pg_catalog", "obj_description", "(object_oid, catalog_name) AS NULL"}, // get comment for a database object
    {"pg_catalog", "shobj_description",
     "(object_oid, catalog_name) AS NULL"}, // get comment for a shared database object

    // visibility functions
    {"pg_catalog", "pg_collation_is_visible", "(collation_oid) AS true"},
    {"pg_catalog", "pg_conversion_is_visible", "(conversion_oid) AS true"},
    {"pg_catalog", "pg_function_is_visible", "(function_oid) AS true"},
    {"pg_catalog", "pg_opclass_is_visible", "(opclass_oid) AS true"},
    {"pg_catalog", "pg_operator_is_visible", "(operator_oid) AS true"},
    {"pg_catalog", "pg_opfamily_is_visible", "(opclass_oid) AS true"},
    {"pg_catalog", "pg_table_is_visible", "(table_oid) AS true"},
    {"pg_catalog", "pg_ts_config_is_visible", "(config_oid) AS true"},
    {"pg_catalog", "pg_ts_dict_is_visible", "(dict_oid) AS true"},
    {"pg_catalog", "pg_ts_parser_is_visible", "(parser_oid) AS true"},
    {"pg_catalog", "pg_ts_template_is_visible", "(template_oid) AS true"},
    {"pg_catalog", "pg_type_is_visible", "(type_oid) AS true"},

    {"pg_catalog", "pg_size_pretty", "(bytes) AS format_bytes(bytes)"},
    {"pg_catalog", "pg_sleep", "(seconds) AS sleep_ms(CAST(seconds * 1000 AS BIGINT))"},

    {DEFAULT_SCHEMA, "nullif", "(a, b) AS CASE WHEN a=b THEN NULL ELSE a END"},
    {DEFAULT_SCHEMA, "if", "(a, b, c) AS CASE WHEN a THEN b ELSE c END"},
    {DEFAULT_SCHEMA, "assert_true",
     "(condition) AS CASE WHEN condition THEN NULL ELSE error('Assertion failed') END, "
     "(condition, message) AS CASE WHEN condition THEN NULL ELSE "
     "error(COALESCE('Assertion: ' || message, 'Assertion failed')) END"},
    {DEFAULT_SCHEMA, "list_append", "(l, e) AS list_concat(l, list_value(e))"},
    {DEFAULT_SCHEMA, "array_append", "(arr, el) AS list_append(arr, el)"},
    {DEFAULT_SCHEMA, "list_prepend", "(e, l) AS list_concat(list_value(e), l)"},
    {DEFAULT_SCHEMA, "array_prepend", "(el, arr) AS list_prepend(el, arr)"},
    {DEFAULT_SCHEMA, "array_pop_back", "(arr) AS arr[:LEN(arr)-1]"},
    {DEFAULT_SCHEMA, "array_pop_front", "(arr) AS arr[2:]"},
    {DEFAULT_SCHEMA, "array_push_back", "(arr, e) AS list_concat(arr, list_value(e))"},
    {DEFAULT_SCHEMA, "array_push_front", "(arr, e) AS list_concat(list_value(e), arr)"},
    {DEFAULT_SCHEMA, "array_to_string",
     "(arr, sep) AS case len(arr::varchar[]) when 0 then '' else list_aggr(arr::varchar[], 'string_agg', sep) end"},
    // Test default parameters
    {DEFAULT_SCHEMA, "array_to_string_comma_default",
     "(arr, sep := ',') AS case len(arr::varchar[]) when 0 then '' else list_aggr(arr::varchar[], 'string_agg', sep) "
     "end"},

    {DEFAULT_SCHEMA, "generate_subscripts", "(arr, dim := 1) AS unnest(generate_series(1, array_length(arr, dim)))"},
    {DEFAULT_SCHEMA, "fdiv", "(x, y) AS floor(x/y)"},
    {DEFAULT_SCHEMA, "fmod", "(x, y) AS (x-y*floor(x/y))"},
    {DEFAULT_SCHEMA, "split_part",
     "(string, delimiter, \"position\") AS if(string IS NOT NULL AND delimiter IS NOT NULL AND position IS NOT NULL, "
     "coalesce(string_split(string, delimiter)[position],''), NULL)"},
    {DEFAULT_SCHEMA, "geomean", "(x) AS exp(avg(ln(x)))"},
    {DEFAULT_SCHEMA, "geometric_mean", "(x) AS geomean(x)"},

    {DEFAULT_SCHEMA, "weighted_avg",
     "(value, weight) AS SUM(value * weight) / SUM(CASE WHEN value IS NOT NULL THEN weight ELSE 0 END)"},
    {DEFAULT_SCHEMA, "wavg", "(value, weight) AS weighted_avg(value, weight)"},
    {DEFAULT_SCHEMA, "variant_group_array", "(x) AS list(x)::VARIANT"},

    {DEFAULT_SCHEMA, "list_reverse", "(l) AS l[:-:-1]"},
    {DEFAULT_SCHEMA, "array_reverse", "(l) AS list_reverse(l)"},

    // algebraic list aggregates
    {DEFAULT_SCHEMA, "list_avg", "(l) AS list_aggr(l, 'avg')"},
    {DEFAULT_SCHEMA, "list_var_samp", "(l) AS list_aggr(l, 'var_samp')"},
    {DEFAULT_SCHEMA, "list_var_pop", "(l) AS list_aggr(l, 'var_pop')"},
    {DEFAULT_SCHEMA, "list_stddev_pop", "(l) AS list_aggr(l, 'stddev_pop')"},
    {DEFAULT_SCHEMA, "list_stddev_samp", "(l) AS list_aggr(l, 'stddev_samp')"},
    {DEFAULT_SCHEMA, "list_sem", "(l) AS list_aggr(l, 'sem')"},

    // distributive list aggregates
    {DEFAULT_SCHEMA, "list_approx_count_distinct", "(l) AS list_aggr(l, 'approx_count_distinct')"},
    {DEFAULT_SCHEMA, "list_bit_xor", "(l) AS list_aggr(l, 'bit_xor')"},
    {DEFAULT_SCHEMA, "list_bit_or", "(l) AS list_aggr(l, 'bit_or')"},
    {DEFAULT_SCHEMA, "list_bit_and", "(l) AS list_aggr(l, 'bit_and')"},
    {DEFAULT_SCHEMA, "list_bool_and", "(l) AS list_aggr(l, 'bool_and')"},
    {DEFAULT_SCHEMA, "list_bool_or", "(l) AS list_aggr(l, 'bool_or')"},
    {DEFAULT_SCHEMA, "list_count", "(l) AS list_aggr(l, 'count')"},
    {DEFAULT_SCHEMA, "list_entropy", "(l) AS list_aggr(l, 'entropy')"},
    {DEFAULT_SCHEMA, "list_last", "(l) AS list_aggr(l, 'last')"},
    {DEFAULT_SCHEMA, "list_first", "(l) AS list_aggr(l, 'first')"},
    {DEFAULT_SCHEMA, "list_any_value", "(l) AS list_aggr(l, 'any_value')"},
    {DEFAULT_SCHEMA, "list_kurtosis", "(l) AS list_aggr(l, 'kurtosis')"},
    {DEFAULT_SCHEMA, "list_kurtosis_pop", "(l) AS list_aggr(l, 'kurtosis_pop')"},
    {DEFAULT_SCHEMA, "list_min", "(l) AS list_aggr(l, 'min')"},
    {DEFAULT_SCHEMA, "list_max", "(l) AS list_aggr(l, 'max')"},
    {DEFAULT_SCHEMA, "list_product", "(l) AS list_aggr(l, 'product')"},
    {DEFAULT_SCHEMA, "list_skewness", "(l) AS list_aggr(l, 'skewness')"},
    {DEFAULT_SCHEMA, "list_sum", "(l) AS list_aggr(l, 'sum')"},
    {DEFAULT_SCHEMA, "list_string_agg", "(l) AS list_aggr(l, 'string_agg')"},

    // holistic list aggregates
    {DEFAULT_SCHEMA, "list_mode", "(l) AS list_aggr(l, 'mode')"},
    {DEFAULT_SCHEMA, "list_median", "(l) AS list_aggr(l, 'median')"},
    {DEFAULT_SCHEMA, "list_mad", "(l) AS list_aggr(l, 'mad')"},

    // nested list aggregates
    {DEFAULT_SCHEMA, "list_histogram", "(l) AS list_aggr(l, 'histogram')"},

    // map functions
    {DEFAULT_SCHEMA, "map_contains_entry",
     "(map, key, value) AS contains(map_entries(map), {'key': key, 'value': value})"},
    {DEFAULT_SCHEMA, "map_contains_value", "(map, value) AS contains(map_values(map), value)"},

    // date functions
    // date_add(date, interval) is the original two-argument form. The three-argument overloads add the
    // date_add(unit, value, timestamp) argument order used in other SQL dialects (resolved by arity, so the two forms
    // cannot be confused): DATE input returns DATE (sub-day units are truncated); TIMESTAMP, TIMESTAMPTZ and TIME keep
    // their type. Unit names follow date_part ('day', 'week', 'month', 'quarter', 'year', 'hour', ...).
    {DEFAULT_SCHEMA, "date_add",
     "(date, \"interval\") AS date + interval, "
     "(part, n, ts DATE) AS (ts + to_interval(part, n))::DATE, "
     "(part, n, ts) AS ts + to_interval(part, n)"},

    // date functions - convenience macro for getting days in month
    {DEFAULT_SCHEMA, "days_in_month", "(date) AS day(last_day(date))"},

    // timestamptz functions
    {DEFAULT_SCHEMA, "ago", "(i) AS current_timestamp - i::interval"},

    // regexp functions
    {DEFAULT_SCHEMA, "regexp_split_to_table", "(text, pattern) AS unnest(string_split_regex(text, pattern))"},

    // storage helper functions
    {DEFAULT_SCHEMA, "get_block_size",
     "(db_name) AS (SELECT block_size FROM pragma_database_size() WHERE database_name = db_name)"},

    // string functions
    {DEFAULT_SCHEMA, "md5_number_upper", "(param) AS ((md5_number(param)::bit::varchar)[65:])::bit::uint64"},
    {DEFAULT_SCHEMA, "md5_number_lower", "(param) AS ((md5_number(param)::bit::varchar)[:64])::bit::uint64"},

    //===------------------------------------------------------------------===//
    // Alternative spellings of existing DuckDB functionality commonly used in other SQL dialects. Each entry
    // documents its behaviour and the small deviations from the usual meaning of the name.
    //===------------------------------------------------------------------===//

    // regexp_substr(source, pattern [, position [, occurrence [, parameters]]]) -> VARCHAR: the occurrence-th match
    // of the pattern starting at the 1-based position (position < 1 searches from the start); '' when nothing matches
    // or occurrence < 1. Parameters 'c'/'i' are passed to the regex engine, 'e' returns the first sub-expression; 'p'
    // is accepted but has no effect, and look-around/back-references raise an error. The parameters argument must be
    // a literal.
    {DEFAULT_SCHEMA, "regexp_substr",
     "(s, pat) AS regexp_extract(s, pat), "
     "(s, pat, pos) AS regexp_extract(s[greatest(pos, 1):], pat), "
     "(s, pat, pos, occ) AS CASE WHEN s IS NULL OR pat IS NULL THEN NULL WHEN occ < 1 THEN '' "
     "ELSE coalesce(regexp_extract_all(s[greatest(pos, 1):], pat)[occ], '') END, "
     "(s, pat, pos, occ, params) AS CASE WHEN s IS NULL OR pat IS NULL THEN NULL WHEN occ < 1 THEN '' "
     "ELSE coalesce(regexp_extract_all(s[greatest(pos, 1):], pat, CASE WHEN params LIKE '%e%' THEN 1 ELSE 0 END, "
     "replace(replace(replace(params, 'c', ''), 'e', ''), 'p', ''))[occ], '') END"},
    // regexp_count(source, pattern [, position [, parameters]]) -> BIGINT number of non-overlapping matches starting
    // at the 1-based position. The parameters argument must be a literal; 'p' is accepted but has no effect.
    {DEFAULT_SCHEMA, "regexp_count",
     "(s, pat) AS len(regexp_extract_all(s, pat)), "
     "(s, pat, pos) AS len(regexp_extract_all(s[greatest(pos, 1):], pat)), "
     "(s, pat, pos, params) AS CASE WHEN s IS NULL THEN NULL ELSE len(regexp_extract_all(s[greatest(pos, 1):], pat, "
     "0, replace(replace(params, 'c', ''), 'p', ''))) END"},
    // regexp_instr(source, pattern [, position [, occurrence]]) -> 1-based position of the start of the match, 0 when
    // there is no match (or fewer than `occurrence` matches), NULL for NULL input. The position is the length of the
    // shortest prefix before the n-th match, captured with a lazy anchored group in front of the pattern (so it is the
    // position of the match itself, not of the first occurrence of the matched text). Further optional arguments
    // (returning the position after the match, regex parameters) are not supported.
    {DEFAULT_SCHEMA, "regexp_instr",
     "(s, pat) AS CASE WHEN s IS NULL OR pat IS NULL THEN NULL WHEN regexp_matches(s, pat) THEN "
     "length(regexp_extract(s, '(?s)^(.*?)(?:' || pat || ')', 1)) + 1 ELSE 0 END, "
     "(s, pat, pos) AS CASE WHEN s IS NULL OR pat IS NULL THEN NULL WHEN regexp_matches(s[greatest(pos, 1):], pat) "
     "THEN "
     "greatest(pos, 1) + length(regexp_extract(s[greatest(pos, 1):], '(?s)^(.*?)(?:' || pat || ')', 1)) ELSE 0 END, "
     "(s, pat, pos, occ) AS CASE WHEN s IS NULL OR pat IS NULL THEN NULL "
     "WHEN occ < 1 OR len(regexp_extract_all(s[greatest(pos, 1):], pat)) < occ THEN 0 "
     "ELSE greatest(pos, 1) + length(regexp_extract(s[greatest(pos, 1):], "
     "'(?s)^((?:.*?(?:' || pat || ')){' || (occ - 1) || '}.*?)(?:' || pat || ')', 1)) END"},
    // dateadd(datepart, n, {date|time|timestamp}): DATE and TIMESTAMP input return TIMESTAMP, TIME returns TIME,
    // TIMESTAMPTZ returns TIMESTAMPTZ. Besides the date_part names, the common abbreviated part spellings are accepted
    // (year/y/yr/yrs, quarter/qtr/qtrs, month/mon/mons, week/w, day/d, hour/h/hr/hrs, minute/m/min/mins,
    // second/s/sec/secs, millisecond/ms/msec/millisec..., microsecond/us/usec/microsec...); the spellings date_part
    // does not know are mapped here, an unknown part raises an error (to_interval). Note: a leap day plus one year
    // gives Feb 28; string literals need an explicit cast.
    {DEFAULT_SCHEMA, "dateadd",
     "(part, n, ts) AS ts + to_interval(CASE lower(part) WHEN 'qtr' THEN 'quarter' WHEN 'qtrs' THEN 'quarter' "
     "WHEN 'millisec' THEN 'ms' WHEN 'millisecs' THEN 'ms' WHEN 'millisecon' THEN 'ms' WHEN 'microsec' THEN 'us' "
     "WHEN 'microsecs' THEN 'us' ELSE part END, n)"},
    // sysdate() / getdate() -> TIMESTAMP in the session time zone. Both return the transaction start time and follow
    // SET TimeZone. The parentheses are required (a bare sysdate keyword cannot be a macro).
    {DEFAULT_SCHEMA, "sysdate", "() AS now()::TIMESTAMP"},
    {DEFAULT_SCHEMA, "getdate", "() AS now()::TIMESTAMP"},
    // nvl(expression, expression, ...) is identical to coalesce; two to five arguments are supported.
    {DEFAULT_SCHEMA, "nvl",
     "(a, b) AS coalesce(a, b), (a, b, c) AS coalesce(a, b, c), (a, b, c, d) AS coalesce(a, b, c, d), "
     "(a, b, c, d, e) AS coalesce(a, b, c, d, e)"},
    // nvl2(expr, not_null_return, null_return): the result type is the common CASE type of the two return values.
    {DEFAULT_SCHEMA, "nvl2", "(a, b, c) AS CASE WHEN a IS NOT NULL THEN b ELSE c END"},
    // charindex(substring, string [, start_location]) -> 1-based position, 0 when not found.
    {DEFAULT_SCHEMA, "charindex",
     "(sub, s) AS strpos(s, sub), "
     "(sub, s, start_pos) AS CASE WHEN strpos(s[greatest(start_pos, 1):], sub) = 0 THEN 0 "
     "ELSE strpos(s[greatest(start_pos, 1):], sub) + greatest(start_pos, 1) - 1 END"},

    // from_unixtime(unixtime [, zone | hours, minutes]): the 1-argument form returns TIMESTAMP WITH TIME ZONE; the
    // zone/offset forms return the wall-clock TIMESTAMP in that zone (TIMESTAMPTZ values carry no zone); fractions are
    // kept at microsecond precision.
    {DEFAULT_SCHEMA, "from_unixtime",
     "(e) AS to_timestamp(e), (e, tz) AS timezone(tz, to_timestamp(e)), "
     "(e, h, m) AS timezone('UTC', to_timestamp(e)) + to_hours(h::BIGINT) + to_minutes(m::BIGINT)"},
    // to_unixtime(timestamp) -> DOUBLE seconds since the epoch.
    {DEFAULT_SCHEMA, "to_unixtime", "(ts) AS epoch(ts)"},
    // from_iso8601_timestamp(string) -> TIMESTAMP WITH TIME ZONE: a string without zone uses the session TimeZone, an
    // explicit offset is kept as the same instant.
    {DEFAULT_SCHEMA, "from_iso8601_timestamp", "(s) AS s::TIMESTAMPTZ"},
    // from_iso8601_date(string) -> DATE: a plain DATE cast. ISO week (2020-W10) and ordinal (2020-123) dates are not
    // accepted and raise a cast error.
    {DEFAULT_SCHEMA, "from_iso8601_date", "(s) AS s::DATE"},
    // sequence(start, stop [, step]): a plain pass-through to generate_series (inclusive bounds; integers, and
    // TIMESTAMP(TZ) bounds with an INTERVAL step). Note: the two-argument form does not count down when start > stop,
    // a zero or wrong-sign step gives an empty list instead of an error, and DATE bounds return TIMESTAMP[] rather
    // than DATE[].
    {DEFAULT_SCHEMA, "sequence",
     "(start_value, stop_value) AS generate_series(start_value, stop_value), "
     "(start_value, stop_value, step) AS generate_series(start_value, stop_value, step)"},
    // array_join(array, delimiter [, null_replacement]): NULL elements are skipped unless a replacement is given; a
    // NULL delimiter behaves like ''.
    {DEFAULT_SCHEMA, "array_join",
     "(arr, sep) AS array_to_string(arr, sep), "
     "(arr, sep, null_replacement) AS "
     "array_to_string(list_transform(arr, lambda __e: coalesce(__e::VARCHAR, null_replacement)), sep)"},
    // map_agg(key, value) -> MAP: rows with a NULL key are ignored and duplicate keys keep the first value. The
    // aggregate appears exactly once (bound to __l through a one-element list) so the macro also works with OVER.
    {DEFAULT_SCHEMA, "map_agg",
     "(k, v) AS list_transform([list({'key': k, 'value': v}) FILTER (WHERE k IS NOT NULL)], lambda __l: "
     "map_from_entries(list_filter(__l, lambda __e, __i: "
     "list_position(list_transform(__l, lambda __x: __x.key), __e.key) = __i)))[1]"},
    // last_day_of_month(x) -> DATE, day_of_year(x), week_of_year(x) -> ISO week number (all identical to the DuckDB
    // functions they point to). Macros rather than aliases so that the TIMESTAMPTZ overloads the ICU extension adds to
    // the underlying functions are picked up. (day_of_week is deliberately not added: that name commonly means ISO
    // numbering with Monday = 1, whereas DuckDB's dayofweek is Sunday = 0, and a near-identical name with different
    // numbering is confusing.)
    {DEFAULT_SCHEMA, "last_day_of_month", "(ts) AS last_day(ts)"},
    {DEFAULT_SCHEMA, "day_of_year", "(ts) AS dayofyear(ts)"},
    {DEFAULT_SCHEMA, "week_of_year", "(ts) AS weekofyear(ts)"},

    // substring_index(str, delim, count): count > 0 -> everything left of the count-th delimiter, count < 0 ->
    // everything right of the count-th delimiter from the right, count = 0 -> '', the whole string when there are
    // fewer delimiters, NULL if any argument is NULL.
    {DEFAULT_SCHEMA, "substring_index",
     "(s, delim, n) AS CASE WHEN s IS NULL OR delim IS NULL OR n IS NULL THEN NULL "
     "WHEN n > 0 THEN array_to_string(string_split(s, delim)[1:n], delim) "
     "WHEN n < 0 THEN array_to_string(string_split(s, delim)[greatest(len(string_split(s, delim)) + n + 1, 1):], "
     "delim) "
     "ELSE '' END"},
    // stuff(string, start, length, replacement): 1-based start; NULL when start <= 0, start > length or length < 0; a
    // length past the end deletes to the end; a NULL replacement deletes without inserting.
    {DEFAULT_SCHEMA, "stuff",
     "(s, start_pos, n_chars, rep) AS CASE WHEN start_pos <= 0 OR start_pos > length(s) OR n_chars < 0 THEN NULL "
     "ELSE s[1:start_pos - 1] || coalesce(rep, '') || s[start_pos + n_chars:] END"},
    // iff / iif(condition, true_value, false_value): the false value is returned for false and NULL conditions.
    {DEFAULT_SCHEMA, "iff", "(c, a, b) AS CASE WHEN c THEN a ELSE b END"},
    {DEFAULT_SCHEMA, "iif", "(c, a, b) AS CASE WHEN c THEN a ELSE b END"},
    {nullptr, nullptr, nullptr}};

unique_ptr<CreateMacroInfo> DefaultFunctionGenerator::CreateInternalMacroInfo(const DefaultMacro &default_macro) {
	auto parser = Parser::GetBuiltinParser();
	auto bind_info = make_uniq<CreateMacroInfo>(CatalogType::MACRO_ENTRY);
	// Build a full CREATE MACRO statement and let the parser handle parameters, types, and defaults.
	// macro_definition may contain multiple comma-separated overloads, e.g. "(x) AS x, (x, y) AS x+y".
	auto sql = StringUtil::Format("CREATE MACRO __dummy__%s", default_macro.macro_definition);
	parser.ParseQuery(sql);
	D_ASSERT(parser.statements.size() == 1);
	D_ASSERT(parser.statements[0]->type == StatementType::CREATE_STATEMENT);
	auto &create_stmt = parser.statements[0]->Cast<CreateStatement>();
	D_ASSERT(create_stmt.info->type == CatalogType::MACRO_ENTRY);
	auto &macro_info = create_stmt.info->Cast<CreateMacroInfo>();
	// Default-bind any typed parameters (e.g. DATE, TIMESTAMP) so overload resolution works correctly.
	// TryDefaultBind resolves built-in types without requiring a ClientContext.
	for (auto &macro : macro_info.macros) {
		for (auto &type : macro->types) {
			if (type.IsUnbound()) {
				type = UnboundType::TryDefaultBind(type);
			}
		}
	}
	bind_info->macros = std::move(macro_info.macros);
	bind_info->SetQualifiedName(
	    QualifiedName({Identifier(default_macro.schema)}, Identifier(default_macro.name)));
	bind_info->temporary = true;
	bind_info->internal = true;
	return bind_info;
}

static bool DefaultFunctionMatches(const DefaultMacro &macro, const Identifier &schema, const Identifier &name) {
	return macro.schema == schema && macro.name == name;
}

static unique_ptr<CreateFunctionInfo> GetDefaultFunction(const Identifier &input_schema, const Identifier &input_name) {
	auto &schema = input_schema;
	auto &name = input_name;
	for (idx_t index = 0; internal_macros[index].name != nullptr; index++) {
		if (DefaultFunctionMatches(internal_macros[index], schema, name)) {
			return DefaultFunctionGenerator::CreateInternalMacroInfo(internal_macros[index]);
		}
	}
	return nullptr;
}

DefaultFunctionGenerator::DefaultFunctionGenerator(Catalog &catalog, SchemaCatalogEntry &schema)
    : DefaultGenerator(catalog), schema(schema) {
}

unique_ptr<CatalogEntry> DefaultFunctionGenerator::CreateDefaultEntry(ClientContext &context,
                                                                      const Identifier &entry_name) {
	auto info = GetDefaultFunction(schema.name, entry_name);
	if (info) {
		return make_uniq_base<CatalogEntry, ScalarMacroCatalogEntry>(catalog, schema, info->Cast<CreateMacroInfo>());
	}
	return nullptr;
}

vector<Identifier> DefaultFunctionGenerator::GetDefaultEntries() {
	vector<Identifier> result;
	for (idx_t index = 0; internal_macros[index].name != nullptr; index++) {
		if (StringUtil::Lower(internal_macros[index].name) != internal_macros[index].name) {
			throw InternalException("Default macro name %s should be lowercase", internal_macros[index].name);
		}
		if (internal_macros[index].schema == schema.name) {
			result.emplace_back(internal_macros[index].name);
		}
	}
	return result;
}

} // namespace duckdb
