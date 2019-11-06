#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Spinque test: many nested views", "[monetdb][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	return;

	REQUIRE_NO_FAIL(con.Query("START TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE params_str (paramname VARCHAR, value VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE params_int (paramname VARCHAR, value BIGINT, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE bm_0_obj_dict (id INTEGER NOT NULL, idstr VARCHAR NOT NULL, prob DOUBLE NOT NULL, CONSTRAINT "
	    "bm_0_obj_dict_id_pkey PRIMARY KEY (id), CONSTRAINT bm_0_obj_dict_idstr_unique UNIQUE (idstr));"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_6 (a1 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE _cachedrel_21 (a1 VARCHAR, a2 VARCHAR, a3 VARCHAR, a4 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE _cachedrel_27 (a1 VARCHAR, a2 VARCHAR, a3 VARCHAR, a4 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_31 (a1 INTEGER, a2 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_40 (a1 INTEGER, a2 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_41 (a1 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE _cachedrel_42 (a1 INTEGER, a2 INTEGER, a3 VARCHAR, a4 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_44 (a1 CHAR(1), prob DECIMAL);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_49 (a1 INTEGER, a2 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_52 (a1 INTEGER, a2 INTEGER, a3 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_60 (a1 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_64 (a1 INTEGER, a2 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_68 (a1 INTEGER, a2 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_77 (a1 INTEGER, a2 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_78 (a1 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE _cachedrel_81 (a1 INTEGER, a2 INTEGER, a3 VARCHAR, a4 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE _cachedrel_82 (a1 INTEGER, a2 INTEGER, a3 VARCHAR, a4 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_90 (a1 INTEGER, a2 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_99 (a1 INTEGER, a2 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_100 (a1 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE _cachedrel_101 (a1 INTEGER, a2 INTEGER, a3 VARCHAR, a4 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_102 (a1 INTEGER, a2 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_106 (a1 INTEGER, a2 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_115 (a1 INTEGER, a2 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_116 (a1 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_118 (a1 CHAR(1), prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_120 (a1 CHAR(1), prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_146 (a1 INTEGER, a2 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("create view _cachedrel_147 as select a2 as a1, a1 as a2, prob from _cachedrel_146;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_150 (a1 INTEGER, a2 INTEGER, a3 BIGINT, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_152 (a1 CHAR(1), prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_158 (a1 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_160 (a1 CHAR(1), prob DOUBLE);"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE _cachedrel_163 (a1 INTEGER, a2 INTEGER, a3 VARCHAR, a4 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("create view _cachedrel_1 as select idstr as a1, id as a2, prob from bm_0_obj_dict;"));
	REQUIRE_NO_FAIL(con.Query("create view _cachedrel_2 as select a2 as a1, prob from _cachedrel_1;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x0 AS SELECT 0 AS a1, a2, prob FROM (SELECT paramName AS a1, value AS a2, "
	                          "prob FROM params_str WHERE paramName = 's0_keyword') AS t__x0;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x1 AS SELECT a1, a2, prob FROM _x0;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x2 AS SELECT a2 AS a1, prob FROM _x1;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x3 AS SELECT a1, prob FROM (SELECT _x2.a1 AS a1, _x2.prob / t__x3.prob AS "
	                          "prob FROM _x2,(SELECT max(prob) AS prob FROM _x2) AS t__x3) AS t__x4;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x4 AS SELECT a1||a2 AS a1, prob FROM (SELECT t__x5_1.a1 AS a1, t__x5_2.a1 AS a2, t__x5_1.prob * "
	    "t__x5_2.prob AS prob FROM _x3 AS t__x5_1,_x3 AS t__x5_2 WHERE t__x5_1.a1 <> t__x5_2.a1) AS t__x6;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x5 AS SELECT a1, prob FROM _x3 UNION ALL SELECT a1, prob FROM _x4;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x6 AS SELECT lower(a1) AS a1, prob FROM _x5;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x7 AS SELECT upper(a1) AS a1, prob FROM _x6;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x8 AS SELECT a2 AS a1, a4 AS a2, max(prob) AS prob FROM (SELECT _cachedrel_21.a1 AS a1, "
	    "_cachedrel_21.a2 AS a2, _cachedrel_21.a3 AS a3, _cachedrel_21.a4 AS a4, _x7.a1 AS a5, _cachedrel_21.prob * "
	    "_x7.prob AS prob FROM _cachedrel_21,_x7 WHERE _cachedrel_21.a1 = _x7.a1) AS t__x8 GROUP BY a2, a4;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x9 AS SELECT a1, max(prob) AS prob FROM (SELECT a2 AS a1, prob FROM _x8 "
	                          "UNION ALL SELECT a1, prob FROM _x5) AS t__x9 GROUP BY a1;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x10 AS SELECT upper(a1) AS a1, prob FROM _x6;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x11 AS SELECT a2 AS a1, a4 AS a2, max(prob) AS prob FROM (SELECT _cachedrel_27.a1 AS a1, "
	    "_cachedrel_27.a2 AS a2, _cachedrel_27.a3 AS a3, _cachedrel_27.a4 AS a4, _x10.a1 AS a5, _cachedrel_27.prob * "
	    "_x10.prob AS prob FROM _cachedrel_27,_x10 WHERE _cachedrel_27.a1 = _x10.a1) AS t__x11 GROUP BY a2, a4;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x12 AS SELECT a1, max(prob) AS prob FROM (SELECT a2 AS a1, prob FROM _x11 "
	                          "UNION ALL SELECT a1, prob FROM _x5) AS t__x12 GROUP BY a1;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x13 AS SELECT a1, prob FROM (SELECT _x9.a1 AS a1, t__x14.a1 AS a2, _x9.prob * "
	              "t__x14.prob AS prob FROM _x9,(SELECT '1',1.0) AS t__x14(a1,prob)) AS t__x15;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x14 AS SELECT a1, prob FROM (SELECT _x12.a1 AS a1, t__x17.a1 AS a2, _x12.prob * "
	              "t__x17.prob AS prob FROM _x12,(SELECT '2',1.0) AS t__x17(a1,prob)) AS t__x18;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x15 AS SELECT a1, prob FROM _x13 UNION ALL SELECT a1, prob FROM _x14;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x16 AS SELECT a1, max(prob) AS prob FROM _x15 GROUP BY a1;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x17 AS SELECT a1, prob FROM (SELECT _x16.a1 AS a1, t__x20.a1 AS a2, "
	                          "_x16.prob * t__x20.prob AS prob FROM _x16,(SELECT '3',1.0) AS t__x20(a1,prob) WHERE "
	                          "length(_x16.a1) >= CAST(t__x20.a1 AS INT)) AS t__x21;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x18 AS SELECT a1, prob FROM _x17;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x19 AS SELECT 0 AS a1, a1 AS a2, prob FROM _x18;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x20 AS SELECT a1, a2, prob FROM _x19;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x21 AS SELECT a1, upper(a2) AS a2, prob FROM _x20;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x22 AS SELECT a1, lower(a2) AS a2, prob FROM _x21;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x23 AS SELECT a1, a3 AS a2, prob FROM (SELECT _x22.a1 AS a1, _x22.a2 AS "
	                          "a2, _cachedrel_31.a1 AS a3, _cachedrel_31.a2 AS a4, _x22.prob * _cachedrel_31.prob AS "
	                          "prob FROM _x22,_cachedrel_31 WHERE _x22.a2 = _cachedrel_31.a2) AS t__x24;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x24 AS SELECT a1, a2, sum(prob) AS prob FROM _x23 GROUP BY a1, a2;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x25 AS SELECT a1, a2, 1 AS prob FROM _x24;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x26 AS SELECT a1, 1 AS prob FROM (SELECT a2 AS a1, max(prob) AS prob FROM "
	                          "_x24 GROUP BY a2) AS t__x25;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x27 AS SELECT a1, a2, prob FROM (SELECT _x25.a1 AS a1, _x25.a2 AS a2, _x26.a1 AS a3, "
	              "_x25.prob * _x26.prob AS prob FROM _x25,_x26 WHERE _x25.a2 = _x26.a1) AS t__x27;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x28 AS SELECT a1, a3 AS a2, sum(prob) AS prob FROM (SELECT _cachedrel_40.a1 AS a1, "
	              "_cachedrel_40.a2 AS a2, _x27.a1 AS a3, _x27.a2 AS a4, _cachedrel_40.prob * _x27.prob AS prob FROM "
	              "_cachedrel_40,_x27 WHERE _cachedrel_40.a2 = _x27.a2) AS t__x29 GROUP BY a1, a3;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x29 AS SELECT a1, prob FROM (SELECT t__x30.a1 AS a1, t__x30.a2 AS a2, _cachedrel_41.a1 "
	              "AS a3, t__x30.prob * _cachedrel_41.prob AS prob FROM (SELECT a1, a2, prob FROM _x28 WHERE a2 = 0) "
	              "AS t__x30,_cachedrel_41 WHERE t__x30.a1 = _cachedrel_41.a1) AS t__x31;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x30 AS SELECT a1, prob FROM (SELECT _x29.a1 AS a1, _x29.prob / t__x33.prob "
	                          "AS prob FROM _x29,(SELECT max(prob) AS prob FROM _x29) AS t__x33) AS t__x34;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x31 AS SELECT lower(a1) AS a1, prob FROM _x16;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x32 AS SELECT a1, prob FROM (SELECT _cachedrel_42.a1 AS a1, _cachedrel_42.a2 AS a2, "
	              "_cachedrel_42.a3 AS a3, _cachedrel_42.a4 AS a4, _x31.a1 AS a5, _cachedrel_42.prob * _x31.prob AS "
	              "prob FROM _cachedrel_42,_x31 WHERE _cachedrel_42.a3 =_x31.a1) AS t__x36;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x33 AS SELECT a1, prob FROM (SELECT _cachedrel_6.a1 AS a1, _x32.a1 AS a2, _cachedrel_6.prob * "
	    "_x32.prob AS prob FROM _cachedrel_6,_x32 WHERE _cachedrel_6.a1 = _x32.a1) AS t__x38;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x34 AS SELECT a1, prob FROM (SELECT _x30.a1 AS a1, _x30.prob / t__x40.prob "
	                          "AS prob FROM _x30,(SELECT max(prob) AS prob FROM _x30) AS t__x40) AS t__x41;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x35 AS SELECT a1, prob FROM (SELECT _x33.a1 AS a1, _x33.prob / t__x43.prob "
	                          "AS prob FROM _x33,(SELECT max(prob) AS prob FROM _x33) AS t__x43) AS t__x44;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x36 AS SELECT a1, prob FROM (SELECT _x34.a1 AS a1, t__x46.a1 AS a2, _x34.prob * t__x46.prob AS "
	    "prob FROM _x34,(SELECT a1, prob FROM _cachedrel_44 WHERE a1 = '1') AS t__x46) AS t__x47;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x37 AS SELECT a1, prob FROM (SELECT _x35.a1 AS a1, t__x49.a1 AS a2, _x35.prob * t__x49.prob AS "
	    "prob FROM _x35,(SELECT a1, prob FROM _cachedrel_44 WHERE a1 = '2') AS t__x49) AS t__x50;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x38 AS SELECT a1, prob FROM _x36 UNION ALL SELECT a1, prob FROM _x37;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x39 AS SELECT a1, sum(prob) AS prob FROM _x38 GROUP BY a1;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x40 AS SELECT a1, a2, prob FROM (SELECT _cachedrel_49.a1 AS a1, "
	                          "_cachedrel_49.a2 AS a2, _x39.a1 AS a3, _cachedrel_49.prob * _x39.prob AS prob FROM "
	                          "_cachedrel_49,_x39 WHERE _cachedrel_49.a1 = _x39.a1) AS t__x52;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x41 AS SELECT a2 AS a1, sum(prob) AS prob FROM _x40 GROUP BY a2;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x42 AS SELECT a1, a3 AS a2, prob FROM (SELECT _x41.a1 AS a1, t__x55.a1 AS a2, "
	              "t__x55.a2 AS a3, _x41.prob * t__x55.prob AS prob FROM _x41,(SELECT a1, a2, max(prob) AS prob FROM "
	              "(SELECT a1, a3 AS a2, prob FROM _cachedrel_52 UNION ALL SELECT a3 AS a1, a1 AS a2, prob FROM "
	              "_cachedrel_52) AS t__x54 GROUP BY a1, a2) AS t__x55 WHERE _x41.a1 = t__x55.a1) AS t__x56;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x43 AS SELECT a2 AS a1, max(prob) AS prob FROM _x42 GROUP BY a2;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x44 AS SELECT 0 AS a1, a1 AS a2, prob FROM _x16;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x45 AS SELECT a1, a2, prob FROM _x44;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x46 AS SELECT a1, upper(a2) AS a2, prob FROM _x45;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x47 AS SELECT a1, lower(a2) AS a2, prob FROM _x46;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x48 AS SELECT a1, a3 AS a2, prob FROM (SELECT _x47.a1 AS a1, _x47.a2 AS "
	                          "a2, _cachedrel_68.a1 AS a3, _cachedrel_68.a2 AS a4, _x47.prob * _cachedrel_68.prob AS "
	                          "prob FROM _x47,_cachedrel_68 WHERE _x47.a2 = _cachedrel_68.a2) AS t__x59;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x49 AS SELECT a1, a2, sum(prob) AS prob FROM _x48 GROUP BY a1, a2;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x50 AS SELECT a1, a2, 1 AS prob FROM _x49;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x51 AS SELECT a1, 1 AS prob FROM (SELECT a2 AS a1, max(prob) AS prob FROM "
	                          "_x49 GROUP BY a2) AS t__x60;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x52 AS SELECT a1, a2, prob FROM (SELECT _x50.a1 AS a1, _x50.a2 AS a2, _x51.a1 AS a3, "
	              "_x50.prob * _x51.prob AS prob FROM _x50,_x51 WHERE _x50.a2 = _x51.a1) AS t__x62;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x53 AS SELECT a1, a3 AS a2, sum(prob) AS prob FROM (SELECT _cachedrel_77.a1 AS a1, "
	              "_cachedrel_77.a2 AS a2, _x52.a1 AS a3, _x52.a2 AS a4, _cachedrel_77.prob * _x52.prob AS prob FROM "
	              "_cachedrel_77,_x52 WHERE _cachedrel_77.a2 = _x52.a2) AS t__x64 GROUP BY a1, a3;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x54 AS SELECT a1, prob FROM (SELECT t__x65.a1 AS a1, t__x65.a2 AS a2, _cachedrel_78.a1 "
	              "AS a3, t__x65.prob * _cachedrel_78.prob AS prob FROM (SELECT a1, a2, prob FROM _x53 WHERE a2 = 0) "
	              "AS t__x65,_cachedrel_78 WHERE t__x65.a1 = _cachedrel_78.a1) AS t__x66;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x55 AS SELECT a1, prob FROM (SELECT _x54.a1 AS a1, _x54.prob / t__x68.prob "
	                          "AS prob FROM _x54,(SELECT max(prob) AS prob FROM _x54) AS t__x68) AS t__x69;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x56 AS SELECT a1, prob FROM (SELECT _cachedrel_82.a1 AS a1, _cachedrel_82.a2 AS a2, "
	              "_cachedrel_82.a3 AS a3, _cachedrel_82.a4 AS a4, _x31.a1 AS a5, _cachedrel_82.prob * _x31.prob AS "
	              "prob FROM _cachedrel_82,_x31 WHERE _cachedrel_82.a3 = _x31.a1) AS t__x71;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x57 AS SELECT a1, prob FROM (SELECT _cachedrel_60.a1 AS a1, _x56.a1 AS a2, _cachedrel_60.prob * "
	    "_x56.prob AS prob FROM _cachedrel_60,_x56 WHERE _cachedrel_60.a1 = _x56.a1) AS t__x73;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x58 AS SELECT a1, a3 AS a2, prob FROM (SELECT _x47.a1 AS a1, _x47.a2 AS "
	                          "a2, _cachedrel_90.a1 AS a3, _cachedrel_90.a2 AS a4, _x47.prob * _cachedrel_90.prob AS "
	                          "prob FROM _x47,_cachedrel_90 WHERE _x47.a2 = _cachedrel_90.a2) AS t__x74;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x59 AS SELECT a1, a2, sum(prob) AS prob FROM _x58 GROUP BY a1, a2;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x60 AS SELECT a1, a2, 1 AS prob FROM _x59;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x61 AS SELECT a1, 1 AS prob FROM (SELECT a2 AS a1, max(prob) AS prob FROM "
	                          "_x59 GROUP BY a2) AS t__x75;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x62 AS SELECT a1, a2, prob FROM (SELECT _x60.a1 AS a1, _x60.a2 AS a2, _x61.a1 AS a3, "
	              "_x60.prob * _x61.prob AS prob FROM _x60,_x61 WHERE _x60.a2 = _x61.a1) AS t__x77;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x63 AS SELECT a1, a3 AS a2, sum(prob) AS prob FROM (SELECT _cachedrel_99.a1 AS a1, "
	              "_cachedrel_99.a2 AS a2, _x62.a1 AS a3, _x62.a2 AS a4, _cachedrel_99.prob * _x62.prob AS prob FROM "
	              "_cachedrel_99,_x62 WHERE _cachedrel_99.a2 = _x62.a2) AS t__x79 GROUP BY a1, a3;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x64 AS SELECT a1, prob FROM (SELECT t__x80.a1 AS a1, t__x80.a2 AS a2, "
	              "_cachedrel_100.a1 AS a3, t__x80.prob * _cachedrel_100.prob AS prob FROM (SELECT a1, a2, prob FROM "
	              "_x63 WHERE a2 = 0) AS t__x80,_cachedrel_100 WHERE t__x80.a1 = _cachedrel_100.a1) AS t__x81;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x65 AS SELECT a1, prob FROM (SELECT _x64.a1 AS a1, _x64.prob / t__x83.prob "
	                          "AS prob FROM _x64,(SELECT max(prob) AS prob FROM _x64) AS t__x83) AS t__x84;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x66 AS SELECT lower(a1) AS a1, prob FROM _x18;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x67 AS SELECT a1, prob FROM (SELECT _cachedrel_101.a1 AS a1, _cachedrel_101.a2 AS a2, "
	              "_cachedrel_101.a3 AS a3, _cachedrel_101.a4 AS a4, _x66.a1 AS a5, _cachedrel_101.prob * _x66.prob AS "
	              "prob FROM _cachedrel_101,_x66 WHERE _cachedrel_101.a3 =_x66.a1) AS t__x86;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x68 AS SELECT a1, prob FROM (SELECT _cachedrel_60.a1 AS a1, _x67.a1 AS a2, _cachedrel_60.prob * "
	    "_x67.prob AS prob FROM _cachedrel_60,_x67 WHERE _cachedrel_60.a1 = _x67.a1) AS t__x87;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x69 AS SELECT a1, a3 AS a2, prob FROM (SELECT _x47.a1 AS a1, _x47.a2 AS "
	                          "a2, _cachedrel_106.a1 AS a3, _cachedrel_106.a2 AS a4, _x47.prob * _cachedrel_106.prob "
	                          "AS prob FROM _x47,_cachedrel_106 WHERE _x47.a2 = _cachedrel_106.a2) AS t__x88;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x70 AS SELECT a1, a2, sum(prob) AS prob FROM _x69 GROUP BY a1, a2;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x71 AS SELECT a1, a2, 1 AS prob FROM _x70;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x72 AS SELECT a1, 1 AS prob FROM (SELECT a2 AS a1, max(prob) AS prob FROM "
	                          "_x70 GROUP BY a2) AS t__x89;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x73 AS SELECT a1, a2, prob FROM (SELECT _x71.a1 AS a1, _x71.a2 AS a2, _x72.a1 AS a3, "
	              "_x71.prob * _x72.prob AS prob FROM _x71,_x72 WHERE _x71.a2 = _x72.a1) AS t__x91;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x74 AS SELECT a1, a3 AS a2, sum(prob) AS prob FROM (SELECT _cachedrel_115.a1 AS a1, "
	              "_cachedrel_115.a2 AS a2, _x73.a1 AS a3, _x73.a2 AS a4, _cachedrel_115.prob * _x73.prob AS prob FROM "
	              "_cachedrel_115,_x73 WHERE _cachedrel_115.a2 = _x73.a2) AS t__x93 GROUP BY a1, a3;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x75 AS SELECT a1, prob FROM (SELECT t__x94.a1 AS a1, t__x94.a2 AS a2, "
	              "_cachedrel_116.a1 AS a3, t__x94.prob * _cachedrel_116.prob AS prob FROM (SELECT a1, a2, prob FROM "
	              "_x74 WHERE a2 = 0) AS t__x94,_cachedrel_116 WHERE t__x94.a1 = _cachedrel_116.a1) AS t__x95;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x76 AS SELECT a1, prob FROM (SELECT _x75.a1 AS a1, _x75.prob / t__x97.prob "
	                          "AS prob FROM _x75,(SELECT max(prob) AS prob FROM _x75) AS t__x97) AS t__x98;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x77 AS SELECT _cachedrel_64.a1 AS a1, _cachedrel_64.a2 AS a2, _x16.a1 AS a3, "
	              "_cachedrel_64.prob * _x16.prob AS prob FROM _cachedrel_64,_x16 WHERE _cachedrel_64.a2 =_x16.a1;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x78 AS SELECT a1, a2, max(prob) AS prob FROM _x77 GROUP BY a1, a2;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x79 AS SELECT _cachedrel_102.a1 AS a1, _cachedrel_102.a2 AS a2, _x16.a1 AS a3, "
	    "_cachedrel_102.prob * _x16.prob AS prob FROM _cachedrel_102,_x16 WHERE _cachedrel_102.a2 = _x16.a1;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x80 AS SELECT a1, a2, max(prob) AS prob FROM _x79 GROUP BY a1, a2;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x81 AS SELECT a1, a2, prob FROM (SELECT _x78.a1 AS a1, _x78.a2 AS a2, "
	                          "t__x102.a1 AS a3, _x78.prob * t__x102.prob AS prob FROM _x78,(SELECT a1, prob FROM "
	                          "_cachedrel_118 WHERE a1 = '1') AS t__x102) AS t__x103;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x82 AS SELECT a1, a2, prob FROM (SELECT _x80.a1 AS a1, _x80.a2 AS a2, "
	                          "t__x105.a1 AS a3, _x80.prob * t__x105.prob AS prob FROM _x80,(SELECT a1, prob FROM "
	                          "_cachedrel_118 WHERE a1 = '2') AS t__x105) AS t__x106;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x83 AS SELECT a1, a2, prob FROM _x81 UNION ALL SELECT a1, a2, prob FROM _x82;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x84 AS SELECT a1, a2, sum(prob) AS prob FROM _x83 GROUP BY a1, a2;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x85 AS SELECT a1, prob FROM (SELECT a1, prob FROM _x84) AS t__x107;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x86 AS SELECT a1, prob FROM (SELECT _x43.a1 AS a1, _x43.prob / t__x109.prob AS prob "
	              "FROM _x43,(SELECT max(prob) AS prob FROM _x43) AS t__x109) AS t__x110;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x87 AS SELECT a1, prob FROM (SELECT _x55.a1 AS a1, _x55.prob / t__x112.prob AS prob "
	              "FROM _x55,(SELECT max(prob) AS prob FROM _x55) AS t__x112) AS t__x113;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x88 AS SELECT a1, prob FROM (SELECT _x57.a1 AS a1, _x57.prob / t__x115.prob AS prob "
	              "FROM _x57,(SELECT max(prob) AS prob FROM _x57) AS t__x115) AS t__x116;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x89 AS SELECT a1, prob FROM (SELECT _x65.a1 AS a1, _x65.prob / t__x118.prob AS prob "
	              "FROM _x65,(SELECT max(prob) AS prob FROM _x65) AS t__x118) AS t__x119;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x90 AS SELECT a1, prob FROM (SELECT _x68.a1 AS a1, _x68.prob / t__x121.prob AS prob "
	              "FROM _x68,(SELECT max(prob) AS prob FROM _x68) AS t__x121) AS t__x122;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x91 AS SELECT a1, prob FROM (SELECT _x76.a1 AS a1, _x76.prob / t__x124.prob AS prob "
	              "FROM _x76,(SELECT max(prob) AS prob FROM _x76) AS t__x124) AS t__x125;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x92 AS SELECT a1, prob FROM (SELECT _x85.a1 AS a1, _x85.prob / t__x127.prob AS prob "
	              "FROM _x85,(SELECT max(prob) AS prob FROM _x85) AS t__x127) AS t__x128;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x93 AS SELECT a1, prob FROM (SELECT _x86.a1 AS a1, t__x130.a1 AS a2, _x86.prob * t__x130.prob AS "
	    "prob FROM _x86,(SELECT a1, prob FROM _cachedrel_120 WHERE a1 = '1') AS t__x130) AS t__x131;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x94 AS SELECT a1, prob FROM (SELECT _x87.a1 AS a1, t__x133.a1 AS a2, _x87.prob * t__x133.prob AS "
	    "prob FROM _x87,(SELECT a1, prob FROM _cachedrel_120 WHERE a1 = '2') AS t__x133) AS t__x134;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x95 AS SELECT a1, prob FROM (SELECT _x88.a1 AS a1, t__x136.a1 AS a2, _x88.prob * t__x136.prob AS "
	    "prob FROM _x88,(SELECT a1, prob FROM _cachedrel_120 WHERE a1 = '3') AS t__x136) AS t__x137;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x96 AS SELECT a1, prob FROM (SELECT _x89.a1 AS a1, t__x139.a1 AS a2, _x89.prob * t__x139.prob AS "
	    "prob FROM _x89,(SELECT a1, prob FROM _cachedrel_120 WHERE a1 = '4') AS t__x139) AS t__x140;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x97 AS SELECT a1, prob FROM (SELECT _x90.a1 AS a1, t__x142.a1 AS a2, _x90.prob * t__x142.prob AS "
	    "prob FROM _x90,(SELECT a1, prob FROM _cachedrel_120 WHERE a1 = '5') AS t__x142) AS t__x143;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x98 AS SELECT a1, prob FROM (SELECT _x91.a1 AS a1, t__x145.a1 AS a2, _x91.prob * t__x145.prob AS "
	    "prob FROM _x91,(SELECT a1, prob FROM _cachedrel_120 WHERE a1 = '6') AS t__x145) AS t__x146;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x99 AS SELECT a1, prob FROM (SELECT _x92.a1 AS a1, t__x148.a1 AS a2, _x92.prob * t__x148.prob AS "
	    "prob FROM _x92,(SELECT a1, prob FROM _cachedrel_120 WHERE a1 = '7') AS t__x148) AS t__x149;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x100 AS SELECT a1, prob FROM _x93 UNION ALL (SELECT a1, prob FROM _x94 UNION ALL "
	              "(SELECT a1, prob FROM _x95 UNION ALL (SELECT a1, prob FROM _x96 UNION ALL (SELECT a1, prob FROM "
	              "_x97 UNION ALL (SELECT a1, prob FROM _x98 UNION ALL SELECT a1, prob FROM _x99)))));"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x101 AS SELECT a1, sum(prob) AS prob FROM _x100 GROUP BY a1;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x102 AS SELECT a1, prob FROM (SELECT _cachedrel_150.a1 AS a1, _cachedrel_150.a2 AS a2, "
	    "_cachedrel_150.a3 AS a3, t__x152.a1 AS a4, _cachedrel_150.prob * t__x152.prob AS prob FROM "
	    "_cachedrel_150,(SELECT a2 AS a1, prob FROM (SELECT paramName AS a1, value AS a2, prob FROM params_int WHERE "
	    "paramName = 's0_userid') AS t__x151) AS t__x152 WHERE _cachedrel_150.a3 = t__x152.a1) AS t__x153;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x103 AS SELECT a1, prob FROM (SELECT _cachedrel_2.a1 AS a1, _x102.a1 AS a2, _cachedrel_2.prob * "
	    "_x102.prob AS prob FROM _cachedrel_2,_x102 WHERE _cachedrel_2.a1 = _x102.a1) AS t__x155;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x104 AS SELECT a1, prob FROM (SELECT _cachedrel_147.a1 AS a1, "
	                          "_cachedrel_147.a2 AS a2, _x103.a1 AS a3, _cachedrel_147.prob * _x103.prob AS prob FROM "
	                          "_cachedrel_147,_x103 WHERE _cachedrel_147.a2 = _x103.a1) AS t__x157;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x105 AS SELECT a1, 1 AS prob FROM _x104;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x106 AS SELECT a1, a4 AS a2, prob FROM (SELECT _x105.a1 AS a1, _cachedrel_52.a1 AS a2, "
	              "_cachedrel_52.a2 AS a3, _cachedrel_52.a3 AS a4, _x105.prob * _cachedrel_52.prob AS prob FROM "
	              "_x105,_cachedrel_52 WHERE _x105.a1 = _cachedrel_52.a1) AS t__x159;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x107 AS SELECT a2 AS a1, max(prob) AS prob FROM _x106 GROUP BY a2;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x108 AS SELECT a1, a2, max(prob) AS prob FROM _x106 GROUP BY a1, a2;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x109 AS SELECT a1, a4 AS a2, prob FROM (SELECT _x107.a1 AS a1, t__x161.a1 AS a2, "
	              "t__x161.a2 AS a3, t__x161.a3 AS a4, _x107.prob * t__x161.prob AS prob FROM _x107,(SELECT a3 AS a1, "
	              "a2, a1 AS a3, prob FROM _cachedrel_52) AS t__x161 WHERE _x107.a1 = t__x161.a1) AS t__x162;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x110 AS SELECT a2 AS a1, prob FROM _x109;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x111 AS SELECT a1, a3 AS a2, max(prob) AS prob FROM (SELECT _cachedrel_81.a1 AS a1, "
	    "_cachedrel_81.a2 AS a2, _cachedrel_81.a3 AS a3, _cachedrel_81.a4 AS a4, _x110.a1 AS a5, _cachedrel_81.prob * "
	    "_x110.prob AS prob FROM _cachedrel_81,_x110 WHERE _cachedrel_81.a1 = _x110.a1) AS t__x164 GROUP BY a1, a3;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x112 AS SELECT a2 AS a1, a1 AS a2, prob FROM _x108;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x113 AS SELECT a1, a3 AS a2, prob FROM (SELECT _cachedrel_81.a1 AS a1, _cachedrel_81.a2 AS a2, "
	    "_cachedrel_81.a3 AS a3, _cachedrel_81.a4 AS a4, _x104.a1 AS a5, _cachedrel_81.prob * _x104.prob AS prob FROM "
	    "_cachedrel_81,_x104 WHERE _cachedrel_81.a1 = _x104.a1) AS t__x165;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x114 AS SELECT a1, a4 AS a2, sum(prob) AS prob FROM (SELECT _x112.a1 AS "
	                          "a1, _x112.a2 AS a2, _x113.a1 AS a3, _x113.a2 AS a4, _x112.prob * _x113.prob AS prob "
	                          "FROM _x112,_x113 WHERE _x112.a2 = _x113.a1) AS t__x167 GROUP BY a1, a4;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x115 AS SELECT _x111.a1 AS a1, _x111.a2 AS a2, _x114.a1 AS a3, _x114.a2 AS "
	                          "a4, _x111.prob * _x114.prob AS prob FROM _x111,_x114 WHERE _x111.a2 = _x114.a2;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x116 AS SELECT a1, a3 AS a2, max(prob) AS prob FROM _x115 GROUP BY a1, a3;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x117 AS SELECT a1, prob FROM (SELECT a1, prob FROM _x116) AS t__x169;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x118 AS SELECT a1, prob FROM (SELECT _x104.a1 AS a1, _x104.prob / t__x171.prob AS prob "
	              "FROM _x104,(SELECT max(prob) AS prob FROM _x104) AS t__x171) AS t__x172;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x119 AS SELECT a1, prob FROM (SELECT _x117.a1 AS a1, _x117.prob / t__x174.prob AS prob "
	              "FROM _x117,(SELECT max(prob) AS prob FROM _x117) AS t__x174) AS t__x175;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x120 AS SELECT a1, prob FROM (SELECT _x118.a1 AS a1, t__x177.a1 AS a2, _x118.prob * t__x177.prob "
	    "AS prob FROM _x118,(SELECT a1, prob FROM _cachedrel_152 WHERE a1 = '1') AS t__x177) AS t__x178;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x121 AS SELECT a1, prob FROM (SELECT _x119.a1 AS a1, t__x180.a1 AS a2, _x119.prob * t__x180.prob "
	    "AS prob FROM _x119,(SELECT a1, prob FROM _cachedrel_152 WHERE a1 = '2') AS t__x180) AS t__x181;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x122 AS SELECT a1, prob FROM _x120 UNION ALL SELECT a1, prob FROM _x121;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x123 AS SELECT a1, sum(prob) AS prob FROM _x122 GROUP BY a1;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x124 AS SELECT a1, sum(prob) AS prob FROM (SELECT _x101.a1 AS a1, _x123.a1 AS a2, _x101.prob * "
	    "_x123.prob AS prob FROM _x101,_x123 WHERE _x101.a1 = _x123.a1) AS t__x183 GROUP BY a1;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x125 AS SELECT a1, prob FROM (SELECT _x101.a1 AS a1, _cachedrel_158.a1 AS a2, _x101.prob * "
	    "_cachedrel_158.prob AS prob FROM _x101,_cachedrel_158 WHERE _x101.a1 = _cachedrel_158.a1) AS t__x184;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x126 AS SELECT a1, prob FROM (SELECT _x101.a1 AS a1, _x101.prob / t__x185.prob AS prob "
	              "FROM _x101,(SELECT max(prob) AS prob FROM _x101) AS t__x185) AS t__x186;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x127 AS SELECT a1, prob FROM (SELECT _x124.a1 AS a1, _x124.prob / t__x188.prob AS prob "
	              "FROM _x124,(SELECT max(prob) AS prob FROM _x124) AS t__x188) AS t__x189;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x128 AS SELECT a1, prob FROM (SELECT _x125.a1 AS a1, _x125.prob / t__x191.prob AS prob "
	              "FROM _x125,(SELECT max(prob) AS prob FROM _x125) AS t__x191) AS t__x192;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x129 AS SELECT a1, prob FROM (SELECT _x126.a1 AS a1, t__x194.a1 AS a2, _x126.prob * t__x194.prob "
	    "AS prob FROM _x126,(SELECT a1, prob FROM _cachedrel_160 WHERE a1 = '1') AS t__x194) AS t__x195;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x130 AS SELECT a1, prob FROM (SELECT _x127.a1 AS a1, t__x197.a1 AS a2, _x127.prob * t__x197.prob "
	    "AS prob FROM _x127,(SELECT a1, prob FROM _cachedrel_160 WHERE a1 = '2') AS t__x197) AS t__x198;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x131 AS SELECT a1, prob FROM (SELECT _x128.a1 AS a1, t__x200.a1 AS a2, _x128.prob * t__x200.prob "
	    "AS prob FROM _x128,(SELECT a1, prob FROM _cachedrel_160 WHERE a1 = '3') AS t__x200) AS t__x201;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x132 AS SELECT a1, prob FROM _x129 UNION ALL (SELECT a1, prob FROM _x130 "
	                          "UNION ALL SELECT a1, prob FROM _x131);"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x133 AS SELECT a1, sum(prob) AS prob FROM _x132 GROUP BY a1;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x134 AS SELECT a1, prob FROM (SELECT _cachedrel_163.a1 AS a1, _cachedrel_163.a2 AS a2, "
	              "_cachedrel_163.a3 AS a3, _cachedrel_163.a4 AS a4, _x31.a1 AS a5, _cachedrel_163.prob * _x31.prob AS "
	              "prob FROM _cachedrel_163,_x31 WHERE _cachedrel_163.a3 = _x31.a1) AS t__x203;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW _x135 AS SELECT a1, prob FROM (SELECT _cachedrel_60.a1 AS a1, _x134.a1 AS a2, _cachedrel_60.prob "
	    "* _x134.prob AS prob FROM _cachedrel_60,_x134 WHERE _cachedrel_60.a1 = _x134.a1) AS t__x204;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x136 AS SELECT CAST(count(prob) AS DOUBLE) AS prob FROM _x135;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW _x137 AS SELECT a1, prob FROM (SELECT _x135.a1 AS a1, _x135.prob * t__x207.prob AS prob "
	              "FROM _x135,(SELECT prob FROM (SELECT CASE WHEN prob > CAST(0 AS DOUBLE) THEN CAST(1 AS DOUBLE) ELSE "
	              "CAST(0 AS DOUBLE) END AS prob FROM _x136) AS t__x206 WHERE prob > 0.0) AS t__x207 UNION ALL SELECT "
	              "_x133.a1 AS a1, _x133.prob * t__x210.prob AS prob FROM _x133,(SELECT prob FROM (SELECT CASE WHEN "
	              "prob > CAST(0 AS DOUBLE) THEN CAST(0 AS DOUBLE) ELSE CAST(1 AS DOUBLE) END AS prob FROM _x136) AS "
	              "t__x209 WHERE prob > 0.0) AS t__x210) AS t__x211;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW _x138 AS SELECT a1, sum(prob) AS prob FROM (SELECT _x137.a1 AS a1, "
	                          "_cachedrel_60.a1 AS a2, _x137.prob * _cachedrel_60.prob AS prob FROM "
	                          "_x137,_cachedrel_60 WHERE _x137.a1 = _cachedrel_60.a1) AS t__x213 GROUP BY a1;"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW s0_RESULTVIEW_result AS SELECT a1, prob FROM _x138;"));

	result = con.Query("SELECT * FROM s0_RESULTVIEW_result;");
	result->Print();
	REQUIRE_NO_FAIL(con.Query("ROLLBACK;"));
}

TEST_CASE("Spinque test: many CTEs", "[monetdb][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	return;

	REQUIRE_NO_FAIL(con.Query("START TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE params_str (paramname VARCHAR, value VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE params_int (paramname VARCHAR, value BIGINT, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE bm_0_obj_dict (id INTEGER NOT NULL, idstr VARCHAR NOT NULL, prob DOUBLE NOT NULL, CONSTRAINT "
	    "bm_0_obj_dict_id_pkey PRIMARY KEY (id), CONSTRAINT bm_0_obj_dict_idstr_unique UNIQUE (idstr));"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_6 (a1 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE _cachedrel_21 (a1 VARCHAR, a2 VARCHAR, a3 VARCHAR, a4 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE _cachedrel_27 (a1 VARCHAR, a2 VARCHAR, a3 VARCHAR, a4 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_31 (a1 INTEGER, a2 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_40 (a1 INTEGER, a2 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_41 (a1 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE _cachedrel_42 (a1 INTEGER, a2 INTEGER, a3 VARCHAR, a4 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_44 (a1 CHAR(1), prob DECIMAL);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_49 (a1 INTEGER, a2 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_52 (a1 INTEGER, a2 INTEGER, a3 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_60 (a1 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_64 (a1 INTEGER, a2 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_68 (a1 INTEGER, a2 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_77 (a1 INTEGER, a2 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_78 (a1 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE _cachedrel_81 (a1 INTEGER, a2 INTEGER, a3 VARCHAR, a4 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE _cachedrel_82 (a1 INTEGER, a2 INTEGER, a3 VARCHAR, a4 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_90 (a1 INTEGER, a2 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_99 (a1 INTEGER, a2 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_100 (a1 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE _cachedrel_101 (a1 INTEGER, a2 INTEGER, a3 VARCHAR, a4 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_102 (a1 INTEGER, a2 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_106 (a1 INTEGER, a2 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_115 (a1 INTEGER, a2 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_116 (a1 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_118 (a1 CHAR(1), prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_120 (a1 CHAR(1), prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_146 (a1 INTEGER, a2 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("create view _cachedrel_147 as select a2 as a1, a1 as a2, prob from _cachedrel_146;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_150 (a1 INTEGER, a2 INTEGER, a3 BIGINT, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_152 (a1 CHAR(1), prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_158 (a1 INTEGER, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE _cachedrel_160 (a1 CHAR(1), prob DOUBLE);"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE _cachedrel_163 (a1 INTEGER, a2 INTEGER, a3 VARCHAR, a4 VARCHAR, prob DOUBLE);"));
	REQUIRE_NO_FAIL(con.Query("create view _cachedrel_1 as select idstr as a1, id as a2, prob from bm_0_obj_dict;"));
	REQUIRE_NO_FAIL(con.Query("create view _cachedrel_2 as select a2 as a1, prob from _cachedrel_1;"));

	result = con.Query(
	    "WITH  _x0 AS (SELECT 0 AS a1, a2, prob FROM (SELECT paramName AS a1, value AS a2, prob FROM params_str WHERE "
	    "paramName = 's0_keyword') AS t__x0),"
	    "	_x1 AS (SELECT a1, a2, prob FROM _x0),"
	    "	_x2 AS (SELECT a2 AS a1, prob FROM _x1),"
	    "	_x3 AS (SELECT a1, prob FROM (SELECT _x2.a1 AS a1, _x2.prob / t__x3.prob AS prob FROM _x2,(SELECT "
	    "max(prob) AS prob FROM _x2) AS t__x3) AS t__x4),"
	    "	_x4 AS (SELECT a1||a2 AS a1, prob FROM (SELECT t__x5_1.a1 AS a1, t__x5_2.a1 AS a2, t__x5_1.prob * "
	    "t__x5_2.prob AS prob FROM _x3 AS t__x5_1,_x3 AS t__x5_2 WHERE t__x5_1.a1 <> t__x5_2.a1) AS t__x6),"
	    "	_x5 AS (SELECT a1, prob FROM _x3 UNION ALL SELECT a1, prob FROM _x4),"
	    "	_x6 AS (SELECT lower(a1) AS a1, prob FROM _x5),"
	    "	_x7 AS (SELECT upper(a1) AS a1, prob FROM _x6),"
	    "	_x8 AS (SELECT a2 AS a1, a4 AS a2, max(prob) AS prob FROM (SELECT _cachedrel_21.a1 AS a1, _cachedrel_21.a2 "
	    "AS a2, _cachedrel_21.a3 AS a3, _cachedrel_21.a4 AS a4, _x7.a1 AS a5, _cachedrel_21.prob * _x7.prob AS prob "
	    "FROM _cachedrel_21,_x7 WHERE _cachedrel_21.a1 = _x7.a1) AS t__x8 GROUP BY a2, a4),"
	    "	_x9 AS (SELECT a1, max(prob) AS prob FROM (SELECT a2 AS a1, prob FROM _x8 UNION ALL SELECT a1, prob FROM "
	    "_x5) AS t__x9 GROUP BY a1),"
	    "	_x10 AS (SELECT upper(a1) AS a1, prob FROM _x6),"
	    "	_x11 AS (SELECT a2 AS a1, a4 AS a2, max(prob) AS prob FROM (SELECT _cachedrel_27.a1 AS a1, "
	    "_cachedrel_27.a2 AS a2, _cachedrel_27.a3 AS a3, _cachedrel_27.a4 AS a4, _x10.a1 AS a5, _cachedrel_27.prob * "
	    "_x10.prob AS prob FROM _cachedrel_27,_x10 WHERE _cachedrel_27.a1 = _x10.a1) AS t__x11 GROUP BY a2, a4),"
	    "	_x12 AS (SELECT a1, max(prob) AS prob FROM (SELECT a2 AS a1, prob FROM _x11 UNION ALL SELECT a1, prob FROM "
	    "_x5) AS t__x12 GROUP BY a1),"
	    "	_x13 AS (SELECT a1, prob FROM (SELECT _x9.a1 AS a1, t__x14.a1 AS a2, _x9.prob * t__x14.prob AS prob FROM "
	    "_x9,(SELECT '1',1.0) AS t__x14(a1,prob)) AS t__x15),"
	    "	_x14 AS (SELECT a1, prob FROM (SELECT _x12.a1 AS a1, t__x17.a1 AS a2, _x12.prob * t__x17.prob AS prob FROM "
	    "_x12,(SELECT '2',1.0) AS t__x17(a1,prob)) AS t__x18),"
	    "	_x15 AS (SELECT a1, prob FROM _x13 UNION ALL SELECT a1, prob FROM _x14),"
	    "	_x16 AS (SELECT a1, max(prob) AS prob FROM _x15 GROUP BY a1),"
	    "	_x17 AS (SELECT a1, prob FROM (SELECT _x16.a1 AS a1, t__x20.a1 AS a2, _x16.prob * t__x20.prob AS prob FROM "
	    "_x16,(SELECT '3',1.0) AS t__x20(a1,prob) WHERE length(_x16.a1) >= CAST(t__x20.a1 AS INT)) AS t__x21),"
	    "	_x18 AS (SELECT a1, prob FROM _x17),"
	    "	_x19 AS (SELECT 0 AS a1, a1 AS a2, prob FROM _x18),"
	    "	_x20 AS (SELECT a1, a2, prob FROM _x19),"
	    "	_x21 AS (SELECT a1, upper(a2) AS a2, prob FROM _x20),"
	    "	_x22 AS (SELECT a1, lower(a2) AS a2, prob FROM _x21),"
	    "	_x23 AS (SELECT a1, a3 AS a2, prob FROM (SELECT _x22.a1 AS a1, _x22.a2 AS a2, _cachedrel_31.a1 AS a3, "
	    "_cachedrel_31.a2 AS a4, _x22.prob * _cachedrel_31.prob AS prob FROM _x22,_cachedrel_31 WHERE _x22.a2 = "
	    "_cachedrel_31.a2) AS t__x24),"
	    "	_x24 AS (SELECT a1, a2, sum(prob) AS prob FROM _x23 GROUP BY a1, a2),"
	    "	_x25 AS (SELECT a1, a2, 1 AS prob FROM _x24),"
	    "	_x26 AS (SELECT a1, 1 AS prob FROM (SELECT a2 AS a1, max(prob) AS prob FROM _x24 GROUP BY a2) AS t__x25),"
	    "	_x27 AS (SELECT a1, a2, prob FROM (SELECT _x25.a1 AS a1, _x25.a2 AS a2, _x26.a1 AS a3, _x25.prob * "
	    "_x26.prob AS prob FROM _x25,_x26 WHERE _x25.a2 = _x26.a1) AS t__x27),"
	    "	_x28 AS (SELECT a1, a3 AS a2, sum(prob) AS prob FROM (SELECT _cachedrel_40.a1 AS a1, _cachedrel_40.a2 AS "
	    "a2, _x27.a1 AS a3, _x27.a2 AS a4, _cachedrel_40.prob * _x27.prob AS prob FROM _cachedrel_40,_x27 WHERE "
	    "_cachedrel_40.a2 = _x27.a2) AS t__x29 GROUP BY a1, a3),"
	    "	_x29 AS (SELECT a1, prob FROM (SELECT t__x30.a1 AS a1, t__x30.a2 AS a2, _cachedrel_41.a1 AS a3, "
	    "t__x30.prob * _cachedrel_41.prob AS prob FROM (SELECT a1, a2, prob FROM _x28 WHERE a2 = 0) AS "
	    "t__x30,_cachedrel_41 WHERE t__x30.a1 = _cachedrel_41.a1) AS t__x31),"
	    "	_x30 AS (SELECT a1, prob FROM (SELECT _x29.a1 AS a1, _x29.prob / t__x33.prob AS prob FROM _x29,(SELECT "
	    "max(prob) AS prob FROM _x29) AS t__x33) AS t__x34),"
	    "	_x31 AS (SELECT lower(a1) AS a1, prob FROM _x16),"
	    "	_x32 AS (SELECT a1, prob FROM (SELECT _cachedrel_42.a1 AS a1, _cachedrel_42.a2 AS a2, _cachedrel_42.a3 AS "
	    "a3, _cachedrel_42.a4 AS a4, _x31.a1 AS a5, _cachedrel_42.prob * _x31.prob AS prob FROM _cachedrel_42,_x31 "
	    "WHERE _cachedrel_42.a3 =_x31.a1) AS t__x36),"
	    "	_x33 AS (SELECT a1, prob FROM (SELECT _cachedrel_6.a1 AS a1, _x32.a1 AS a2, _cachedrel_6.prob * _x32.prob "
	    "AS prob FROM _cachedrel_6,_x32 WHERE _cachedrel_6.a1 = _x32.a1) AS t__x38),"
	    "	_x34 AS (SELECT a1, prob FROM (SELECT _x30.a1 AS a1, _x30.prob / t__x40.prob AS prob FROM _x30,(SELECT "
	    "max(prob) AS prob FROM _x30) AS t__x40) AS t__x41),"
	    "	_x35 AS (SELECT a1, prob FROM (SELECT _x33.a1 AS a1, _x33.prob / t__x43.prob AS prob FROM _x33,(SELECT "
	    "max(prob) AS prob FROM _x33) AS t__x43) AS t__x44),"
	    "	_x36 AS (SELECT a1, prob FROM (SELECT _x34.a1 AS a1, t__x46.a1 AS a2, _x34.prob * t__x46.prob AS prob FROM "
	    "_x34,(SELECT a1, prob FROM _cachedrel_44 WHERE a1 = '1') AS t__x46) AS t__x47),"
	    "	_x37 AS (SELECT a1, prob FROM (SELECT _x35.a1 AS a1, t__x49.a1 AS a2, _x35.prob * t__x49.prob AS prob FROM "
	    "_x35,(SELECT a1, prob FROM _cachedrel_44 WHERE a1 = '2') AS t__x49) AS t__x50),"
	    "	_x38 AS (SELECT a1, prob FROM _x36 UNION ALL SELECT a1, prob FROM _x37),"
	    "	_x39 AS (SELECT a1, sum(prob) AS prob FROM _x38 GROUP BY a1),"
	    "	_x40 AS (SELECT a1, a2, prob FROM (SELECT _cachedrel_49.a1 AS a1, _cachedrel_49.a2 AS a2, _x39.a1 AS a3, "
	    "_cachedrel_49.prob * _x39.prob AS prob FROM _cachedrel_49,_x39 WHERE _cachedrel_49.a1 = _x39.a1) AS t__x52),"
	    "	_x41 AS (SELECT a2 AS a1, sum(prob) AS prob FROM _x40 GROUP BY a2),"
	    "	_x42 AS (SELECT a1, a3 AS a2, prob FROM (SELECT _x41.a1 AS a1, t__x55.a1 AS a2, t__x55.a2 AS a3, _x41.prob "
	    "* t__x55.prob AS prob FROM _x41,(SELECT a1, a2, max(prob) AS prob FROM (SELECT a1, a3 AS a2, prob FROM "
	    "_cachedrel_52 UNION ALL SELECT a3 AS a1, a1 AS a2, prob FROM _cachedrel_52) AS t__x54 GROUP BY a1, a2) AS "
	    "t__x55 WHERE _x41.a1 = t__x55.a1) AS t__x56),"
	    "	_x43 AS (SELECT a2 AS a1, max(prob) AS prob FROM _x42 GROUP BY a2),"
	    "	_x44 AS (SELECT 0 AS a1, a1 AS a2, prob FROM _x16),"
	    "	_x45 AS (SELECT a1, a2, prob FROM _x44),"
	    "	_x46 AS (SELECT a1, upper(a2) AS a2, prob FROM _x45),"
	    "	_x47 AS (SELECT a1, lower(a2) AS a2, prob FROM _x46),"
	    "	_x48 AS (SELECT a1, a3 AS a2, prob FROM (SELECT _x47.a1 AS a1, _x47.a2 AS a2, _cachedrel_68.a1 AS a3, "
	    "_cachedrel_68.a2 AS a4, _x47.prob * _cachedrel_68.prob AS prob FROM _x47,_cachedrel_68 WHERE _x47.a2 = "
	    "_cachedrel_68.a2) AS t__x59),"
	    "	_x49 AS (SELECT a1, a2, sum(prob) AS prob FROM _x48 GROUP BY a1, a2),"
	    "	_x50 AS (SELECT a1, a2, 1 AS prob FROM _x49),"
	    "	_x51 AS (SELECT a1, 1 AS prob FROM (SELECT a2 AS a1, max(prob) AS prob FROM _x49 GROUP BY a2) AS t__x60),"
	    "	_x52 AS (SELECT a1, a2, prob FROM (SELECT _x50.a1 AS a1, _x50.a2 AS a2, _x51.a1 AS a3, _x50.prob * "
	    "_x51.prob AS prob FROM _x50,_x51 WHERE _x50.a2 = _x51.a1) AS t__x62),"
	    "	_x53 AS (SELECT a1, a3 AS a2, sum(prob) AS prob FROM (SELECT _cachedrel_77.a1 AS a1, _cachedrel_77.a2 AS "
	    "a2, _x52.a1 AS a3, _x52.a2 AS a4, _cachedrel_77.prob * _x52.prob AS prob FROM _cachedrel_77,_x52 WHERE "
	    "_cachedrel_77.a2 = _x52.a2) AS t__x64 GROUP BY a1, a3),"
	    "	_x54 AS (SELECT a1, prob FROM (SELECT t__x65.a1 AS a1, t__x65.a2 AS a2, _cachedrel_78.a1 AS a3, "
	    "t__x65.prob * _cachedrel_78.prob AS prob FROM (SELECT a1, a2, prob FROM _x53 WHERE a2 = 0) AS "
	    "t__x65,_cachedrel_78 WHERE t__x65.a1 = _cachedrel_78.a1) AS t__x66),"
	    "	_x55 AS (SELECT a1, prob FROM (SELECT _x54.a1 AS a1, _x54.prob / t__x68.prob AS prob FROM _x54,(SELECT "
	    "max(prob) AS prob FROM _x54) AS t__x68) AS t__x69),"
	    "	_x56 AS (SELECT a1, prob FROM (SELECT _cachedrel_82.a1 AS a1, _cachedrel_82.a2 AS a2, _cachedrel_82.a3 AS "
	    "a3, _cachedrel_82.a4 AS a4, _x31.a1 AS a5, _cachedrel_82.prob * _x31.prob AS prob FROM _cachedrel_82,_x31 "
	    "WHERE _cachedrel_82.a3 = _x31.a1) AS t__x71),"
	    "	_x57 AS (SELECT a1, prob FROM (SELECT _cachedrel_60.a1 AS a1, _x56.a1 AS a2, _cachedrel_60.prob * "
	    "_x56.prob AS prob FROM _cachedrel_60,_x56 WHERE _cachedrel_60.a1 = _x56.a1) AS t__x73),"
	    "	_x58 AS (SELECT a1, a3 AS a2, prob FROM (SELECT _x47.a1 AS a1, _x47.a2 AS a2, _cachedrel_90.a1 AS a3, "
	    "_cachedrel_90.a2 AS a4, _x47.prob * _cachedrel_90.prob AS prob FROM _x47,_cachedrel_90 WHERE _x47.a2 = "
	    "_cachedrel_90.a2) AS t__x74),"
	    "	_x59 AS (SELECT a1, a2, sum(prob) AS prob FROM _x58 GROUP BY a1, a2),"
	    "	_x60 AS (SELECT a1, a2, 1 AS prob FROM _x59),"
	    "	_x61 AS (SELECT a1, 1 AS prob FROM (SELECT a2 AS a1, max(prob) AS prob FROM _x59 GROUP BY a2) AS t__x75),"
	    "	_x62 AS (SELECT a1, a2, prob FROM (SELECT _x60.a1 AS a1, _x60.a2 AS a2, _x61.a1 AS a3, _x60.prob * "
	    "_x61.prob AS prob FROM _x60,_x61 WHERE _x60.a2 = _x61.a1) AS t__x77),"
	    "	_x63 AS (SELECT a1, a3 AS a2, sum(prob) AS prob FROM (SELECT _cachedrel_99.a1 AS a1, _cachedrel_99.a2 AS "
	    "a2, _x62.a1 AS a3, _x62.a2 AS a4, _cachedrel_99.prob * _x62.prob AS prob FROM _cachedrel_99,_x62 WHERE "
	    "_cachedrel_99.a2 = _x62.a2) AS t__x79 GROUP BY a1, a3),"
	    "	_x64 AS (SELECT a1, prob FROM (SELECT t__x80.a1 AS a1, t__x80.a2 AS a2, _cachedrel_100.a1 AS a3, "
	    "t__x80.prob * _cachedrel_100.prob AS prob FROM (SELECT a1, a2, prob FROM _x63 WHERE a2 = 0) AS "
	    "t__x80,_cachedrel_100 WHERE t__x80.a1 = _cachedrel_100.a1) AS t__x81),"
	    "	_x65 AS (SELECT a1, prob FROM (SELECT _x64.a1 AS a1, _x64.prob / t__x83.prob AS prob FROM _x64,(SELECT "
	    "max(prob) AS prob FROM _x64) AS t__x83) AS t__x84),"
	    "	_x66 AS (SELECT lower(a1) AS a1, prob FROM _x18),"
	    "	_x67 AS (SELECT a1, prob FROM (SELECT _cachedrel_101.a1 AS a1, _cachedrel_101.a2 AS a2, _cachedrel_101.a3 "
	    "AS a3, _cachedrel_101.a4 AS a4, _x66.a1 AS a5, _cachedrel_101.prob * _x66.prob AS prob FROM "
	    "_cachedrel_101,_x66 WHERE _cachedrel_101.a3 =_x66.a1) AS t__x86),"
	    "	_x68 AS (SELECT a1, prob FROM (SELECT _cachedrel_60.a1 AS a1, _x67.a1 AS a2, _cachedrel_60.prob * "
	    "_x67.prob AS prob FROM _cachedrel_60,_x67 WHERE _cachedrel_60.a1 = _x67.a1) AS t__x87),"
	    "	_x69 AS (SELECT a1, a3 AS a2, prob FROM (SELECT _x47.a1 AS a1, _x47.a2 AS a2, _cachedrel_106.a1 AS a3, "
	    "_cachedrel_106.a2 AS a4, _x47.prob * _cachedrel_106.prob AS prob FROM _x47,_cachedrel_106 WHERE _x47.a2 = "
	    "_cachedrel_106.a2) AS t__x88),"
	    "	_x70 AS (SELECT a1, a2, sum(prob) AS prob FROM _x69 GROUP BY a1, a2),"
	    "	_x71 AS (SELECT a1, a2, 1 AS prob FROM _x70),"
	    "	_x72 AS (SELECT a1, 1 AS prob FROM (SELECT a2 AS a1, max(prob) AS prob FROM _x70 GROUP BY a2) AS t__x89),"
	    "	_x73 AS (SELECT a1, a2, prob FROM (SELECT _x71.a1 AS a1, _x71.a2 AS a2, _x72.a1 AS a3, _x71.prob * "
	    "_x72.prob AS prob FROM _x71,_x72 WHERE _x71.a2 = _x72.a1) AS t__x91),"
	    "	_x74 AS (SELECT a1, a3 AS a2, sum(prob) AS prob FROM (SELECT _cachedrel_115.a1 AS a1, _cachedrel_115.a2 AS "
	    "a2, _x73.a1 AS a3, _x73.a2 AS a4, _cachedrel_115.prob * _x73.prob AS prob FROM _cachedrel_115,_x73 WHERE "
	    "_cachedrel_115.a2 = _x73.a2) AS t__x93 GROUP BY a1, a3),"
	    "	_x75 AS (SELECT a1, prob FROM (SELECT t__x94.a1 AS a1, t__x94.a2 AS a2, _cachedrel_116.a1 AS a3, "
	    "t__x94.prob * _cachedrel_116.prob AS prob FROM (SELECT a1, a2, prob FROM _x74 WHERE a2 = 0) AS "
	    "t__x94,_cachedrel_116 WHERE t__x94.a1 = _cachedrel_116.a1) AS t__x95),"
	    "	_x76 AS (SELECT a1, prob FROM (SELECT _x75.a1 AS a1, _x75.prob / t__x97.prob AS prob FROM _x75,(SELECT "
	    "max(prob) AS prob FROM _x75) AS t__x97) AS t__x98),"
	    "	_x77 AS (SELECT _cachedrel_64.a1 AS a1, _cachedrel_64.a2 AS a2, _x16.a1 AS a3, _cachedrel_64.prob * "
	    "_x16.prob AS prob FROM _cachedrel_64,_x16 WHERE _cachedrel_64.a2 =_x16.a1),"
	    "	_x78 AS (SELECT a1, a2, max(prob) AS prob FROM _x77 GROUP BY a1, a2),"
	    "	_x79 AS (SELECT _cachedrel_102.a1 AS a1, _cachedrel_102.a2 AS a2, _x16.a1 AS a3, _cachedrel_102.prob * "
	    "_x16.prob AS prob FROM _cachedrel_102,_x16 WHERE _cachedrel_102.a2 = _x16.a1),"
	    "	_x80 AS (SELECT a1, a2, max(prob) AS prob FROM _x79 GROUP BY a1, a2),"
	    "	_x81 AS (SELECT a1, a2, prob FROM (SELECT _x78.a1 AS a1, _x78.a2 AS a2, t__x102.a1 AS a3, _x78.prob * "
	    "t__x102.prob AS prob FROM _x78,(SELECT a1, prob FROM _cachedrel_118 WHERE a1 = '1') AS t__x102) AS t__x103),"
	    "	_x82 AS (SELECT a1, a2, prob FROM (SELECT _x80.a1 AS a1, _x80.a2 AS a2, t__x105.a1 AS a3, _x80.prob * "
	    "t__x105.prob AS prob FROM _x80,(SELECT a1, prob FROM _cachedrel_118 WHERE a1 = '2') AS t__x105) AS t__x106),"
	    "	_x83 AS (SELECT a1, a2, prob FROM _x81 UNION ALL SELECT a1, a2, prob FROM _x82),"
	    "	_x84 AS (SELECT a1, a2, sum(prob) AS prob FROM _x83 GROUP BY a1, a2),"
	    "	_x85 AS (SELECT a1, prob FROM (SELECT a1, prob FROM _x84) AS t__x107),"
	    "	_x86 AS (SELECT a1, prob FROM (SELECT _x43.a1 AS a1, _x43.prob / t__x109.prob AS prob FROM _x43,(SELECT "
	    "max(prob) AS prob FROM _x43) AS t__x109) AS t__x110),"
	    "	_x87 AS (SELECT a1, prob FROM (SELECT _x55.a1 AS a1, _x55.prob / t__x112.prob AS prob FROM _x55,(SELECT "
	    "max(prob) AS prob FROM _x55) AS t__x112) AS t__x113),"
	    "	_x88 AS (SELECT a1, prob FROM (SELECT _x57.a1 AS a1, _x57.prob / t__x115.prob AS prob FROM _x57,(SELECT "
	    "max(prob) AS prob FROM _x57) AS t__x115) AS t__x116),"
	    "	_x89 AS (SELECT a1, prob FROM (SELECT _x65.a1 AS a1, _x65.prob / t__x118.prob AS prob FROM _x65,(SELECT "
	    "max(prob) AS prob FROM _x65) AS t__x118) AS t__x119),"
	    "	_x90 AS (SELECT a1, prob FROM (SELECT _x68.a1 AS a1, _x68.prob / t__x121.prob AS prob FROM _x68,(SELECT "
	    "max(prob) AS prob FROM _x68) AS t__x121) AS t__x122),"
	    "	_x91 AS (SELECT a1, prob FROM (SELECT _x76.a1 AS a1, _x76.prob / t__x124.prob AS prob FROM _x76,(SELECT "
	    "max(prob) AS prob FROM _x76) AS t__x124) AS t__x125),"
	    "	_x92 AS (SELECT a1, prob FROM (SELECT _x85.a1 AS a1, _x85.prob / t__x127.prob AS prob FROM _x85,(SELECT "
	    "max(prob) AS prob FROM _x85) AS t__x127) AS t__x128),"
	    "	_x93 AS (SELECT a1, prob FROM (SELECT _x86.a1 AS a1, t__x130.a1 AS a2, _x86.prob * t__x130.prob AS prob "
	    "FROM _x86,(SELECT a1, prob FROM _cachedrel_120 WHERE a1 = '1') AS t__x130) AS t__x131),"
	    "	_x94 AS (SELECT a1, prob FROM (SELECT _x87.a1 AS a1, t__x133.a1 AS a2, _x87.prob * t__x133.prob AS prob "
	    "FROM _x87,(SELECT a1, prob FROM _cachedrel_120 WHERE a1 = '2') AS t__x133) AS t__x134),"
	    "	_x95 AS (SELECT a1, prob FROM (SELECT _x88.a1 AS a1, t__x136.a1 AS a2, _x88.prob * t__x136.prob AS prob "
	    "FROM _x88,(SELECT a1, prob FROM _cachedrel_120 WHERE a1 = '3') AS t__x136) AS t__x137),"
	    "	_x96 AS (SELECT a1, prob FROM (SELECT _x89.a1 AS a1, t__x139.a1 AS a2, _x89.prob * t__x139.prob AS prob "
	    "FROM _x89,(SELECT a1, prob FROM _cachedrel_120 WHERE a1 = '4') AS t__x139) AS t__x140),"
	    "	_x97 AS (SELECT a1, prob FROM (SELECT _x90.a1 AS a1, t__x142.a1 AS a2, _x90.prob * t__x142.prob AS prob "
	    "FROM _x90,(SELECT a1, prob FROM _cachedrel_120 WHERE a1 = '5') AS t__x142) AS t__x143),"
	    "	_x98 AS (SELECT a1, prob FROM (SELECT _x91.a1 AS a1, t__x145.a1 AS a2, _x91.prob * t__x145.prob AS prob "
	    "FROM _x91,(SELECT a1, prob FROM _cachedrel_120 WHERE a1 = '6') AS t__x145) AS t__x146),"
	    "	_x99 AS (SELECT a1, prob FROM (SELECT _x92.a1 AS a1, t__x148.a1 AS a2, _x92.prob * t__x148.prob AS prob "
	    "FROM _x92,(SELECT a1, prob FROM _cachedrel_120 WHERE a1 = '7') AS t__x148) AS t__x149),"
	    "	_x100 AS (SELECT a1, prob FROM _x93 UNION ALL (SELECT a1, prob FROM _x94 UNION ALL (SELECT a1, prob FROM "
	    "_x95 UNION ALL (SELECT a1, prob FROM _x96 UNION ALL (SELECT a1, prob FROM _x97 UNION ALL (SELECT a1, prob "
	    "FROM _x98 UNION ALL SELECT a1, prob FROM _x99)))))),"
	    "	_x101 AS (SELECT a1, sum(prob) AS prob FROM _x100 GROUP BY a1),"
	    "	_x102 AS (SELECT a1, prob FROM (SELECT _cachedrel_150.a1 AS a1, _cachedrel_150.a2 AS a2, _cachedrel_150.a3 "
	    "AS a3, t__x152.a1 AS a4, _cachedrel_150.prob * t__x152.prob AS prob FROM _cachedrel_150,(SELECT a2 AS a1, "
	    "prob FROM (SELECT paramName AS a1, value AS a2, prob FROM params_int WHERE paramName = 's0_userid') AS "
	    "t__x151) AS t__x152 WHERE _cachedrel_150.a3 = t__x152.a1) AS t__x153),"
	    "	_x103 AS (SELECT a1, prob FROM (SELECT _cachedrel_2.a1 AS a1, _x102.a1 AS a2, _cachedrel_2.prob * "
	    "_x102.prob AS prob FROM _cachedrel_2,_x102 WHERE _cachedrel_2.a1 = _x102.a1) AS t__x155),"
	    "	_x104 AS (SELECT a1, prob FROM (SELECT _cachedrel_147.a1 AS a1, _cachedrel_147.a2 AS a2, _x103.a1 AS a3, "
	    "_cachedrel_147.prob * _x103.prob AS prob FROM _cachedrel_147,_x103 WHERE _cachedrel_147.a2 = _x103.a1) AS "
	    "t__x157),"
	    "	_x105 AS (SELECT a1, 1 AS prob FROM _x104),"
	    "	_x106 AS (SELECT a1, a4 AS a2, prob FROM (SELECT _x105.a1 AS a1, _cachedrel_52.a1 AS a2, _cachedrel_52.a2 "
	    "AS a3, _cachedrel_52.a3 AS a4, _x105.prob * _cachedrel_52.prob AS prob FROM _x105,_cachedrel_52 WHERE "
	    "_x105.a1 = _cachedrel_52.a1) AS t__x159),"
	    "	_x107 AS (SELECT a2 AS a1, max(prob) AS prob FROM _x106 GROUP BY a2),"
	    "	_x108 AS (SELECT a1, a2, max(prob) AS prob FROM _x106 GROUP BY a1, a2),"
	    "	_x109 AS (SELECT a1, a4 AS a2, prob FROM (SELECT _x107.a1 AS a1, t__x161.a1 AS a2, t__x161.a2 AS a3, "
	    "t__x161.a3 AS a4, _x107.prob * t__x161.prob AS prob FROM _x107,(SELECT a3 AS a1, a2, a1 AS a3, prob FROM "
	    "_cachedrel_52) AS t__x161 WHERE _x107.a1 = t__x161.a1) AS t__x162),"
	    "	_x110 AS (SELECT a2 AS a1, prob FROM _x109),"
	    "	_x111 AS (SELECT a1, a3 AS a2, max(prob) AS prob FROM (SELECT _cachedrel_81.a1 AS a1, _cachedrel_81.a2 AS "
	    "a2, _cachedrel_81.a3 AS a3, _cachedrel_81.a4 AS a4, _x110.a1 AS a5, _cachedrel_81.prob * _x110.prob AS prob "
	    "FROM _cachedrel_81,_x110 WHERE _cachedrel_81.a1 = _x110.a1) AS t__x164 GROUP BY a1, a3),"
	    "	_x112 AS (SELECT a2 AS a1, a1 AS a2, prob FROM _x108),"
	    "	_x113 AS (SELECT a1, a3 AS a2, prob FROM (SELECT _cachedrel_81.a1 AS a1, _cachedrel_81.a2 AS a2, "
	    "_cachedrel_81.a3 AS a3, _cachedrel_81.a4 AS a4, _x104.a1 AS a5, _cachedrel_81.prob * _x104.prob AS prob FROM "
	    "_cachedrel_81,_x104 WHERE _cachedrel_81.a1 = _x104.a1) AS t__x165),"
	    "	_x114 AS (SELECT a1, a4 AS a2, sum(prob) AS prob FROM (SELECT _x112.a1 AS a1, _x112.a2 AS a2, _x113.a1 AS "
	    "a3, _x113.a2 AS a4, _x112.prob * _x113.prob AS prob FROM _x112,_x113 WHERE _x112.a2 = _x113.a1) AS t__x167 "
	    "GROUP BY a1, a4),"
	    "	_x115 AS (SELECT _x111.a1 AS a1, _x111.a2 AS a2, _x114.a1 AS a3, _x114.a2 AS a4, _x111.prob * _x114.prob "
	    "AS prob FROM _x111,_x114 WHERE _x111.a2 = _x114.a2),"
	    "	_x116 AS (SELECT a1, a3 AS a2, max(prob) AS prob FROM _x115 GROUP BY a1, a3),"
	    "	_x117 AS (SELECT a1, prob FROM (SELECT a1, prob FROM _x116) AS t__x169),"
	    "	_x118 AS (SELECT a1, prob FROM (SELECT _x104.a1 AS a1, _x104.prob / t__x171.prob AS prob FROM "
	    "_x104,(SELECT max(prob) AS prob FROM _x104) AS t__x171) AS t__x172),"
	    "	_x119 AS (SELECT a1, prob FROM (SELECT _x117.a1 AS a1, _x117.prob / t__x174.prob AS prob FROM "
	    "_x117,(SELECT max(prob) AS prob FROM _x117) AS t__x174) AS t__x175),"
	    "	_x120 AS (SELECT a1, prob FROM (SELECT _x118.a1 AS a1, t__x177.a1 AS a2, _x118.prob * t__x177.prob AS prob "
	    "FROM _x118,(SELECT a1, prob FROM _cachedrel_152 WHERE a1 = '1') AS t__x177) AS t__x178),"
	    "	_x121 AS (SELECT a1, prob FROM (SELECT _x119.a1 AS a1, t__x180.a1 AS a2, _x119.prob * t__x180.prob AS prob "
	    "FROM _x119,(SELECT a1, prob FROM _cachedrel_152 WHERE a1 = '2') AS t__x180) AS t__x181),"
	    "	_x122 AS (SELECT a1, prob FROM _x120 UNION ALL SELECT a1, prob FROM _x121),"
	    "	_x123 AS (SELECT a1, sum(prob) AS prob FROM _x122 GROUP BY a1),"
	    "	_x124 AS (SELECT a1, sum(prob) AS prob FROM (SELECT _x101.a1 AS a1, _x123.a1 AS a2, _x101.prob * "
	    "_x123.prob AS prob FROM _x101,_x123 WHERE _x101.a1 = _x123.a1) AS t__x183 GROUP BY a1),"
	    "	_x125 AS (SELECT a1, prob FROM (SELECT _x101.a1 AS a1, _cachedrel_158.a1 AS a2, _x101.prob * "
	    "_cachedrel_158.prob AS prob FROM _x101,_cachedrel_158 WHERE _x101.a1 = _cachedrel_158.a1) AS t__x184),"
	    "	_x126 AS (SELECT a1, prob FROM (SELECT _x101.a1 AS a1, _x101.prob / t__x185.prob AS prob FROM "
	    "_x101,(SELECT max(prob) AS prob FROM _x101) AS t__x185) AS t__x186),"
	    "	_x127 AS (SELECT a1, prob FROM (SELECT _x124.a1 AS a1, _x124.prob / t__x188.prob AS prob FROM "
	    "_x124,(SELECT max(prob) AS prob FROM _x124) AS t__x188) AS t__x189),"
	    "	_x128 AS (SELECT a1, prob FROM (SELECT _x125.a1 AS a1, _x125.prob / t__x191.prob AS prob FROM "
	    "_x125,(SELECT max(prob) AS prob FROM _x125) AS t__x191) AS t__x192),"
	    "	_x129 AS (SELECT a1, prob FROM (SELECT _x126.a1 AS a1, t__x194.a1 AS a2, _x126.prob * t__x194.prob AS prob "
	    "FROM _x126,(SELECT a1, prob FROM _cachedrel_160 WHERE a1 = '1') AS t__x194) AS t__x195),"
	    "	_x130 AS (SELECT a1, prob FROM (SELECT _x127.a1 AS a1, t__x197.a1 AS a2, _x127.prob * t__x197.prob AS prob "
	    "FROM _x127,(SELECT a1, prob FROM _cachedrel_160 WHERE a1 = '2') AS t__x197) AS t__x198),"
	    "	_x131 AS (SELECT a1, prob FROM (SELECT _x128.a1 AS a1, t__x200.a1 AS a2, _x128.prob * t__x200.prob AS prob "
	    "FROM _x128,(SELECT a1, prob FROM _cachedrel_160 WHERE a1 = '3') AS t__x200) AS t__x201),"
	    "	_x132 AS (SELECT a1, prob FROM _x129 UNION ALL (SELECT a1, prob FROM _x130 UNION ALL SELECT a1, prob FROM "
	    "_x131)),"
	    "	_x133 AS (SELECT a1, sum(prob) AS prob FROM _x132 GROUP BY a1),"
	    "	_x134 AS (SELECT a1, prob FROM (SELECT _cachedrel_163.a1 AS a1, _cachedrel_163.a2 AS a2, _cachedrel_163.a3 "
	    "AS a3, _cachedrel_163.a4 AS a4, _x31.a1 AS a5, _cachedrel_163.prob * _x31.prob AS prob FROM "
	    "_cachedrel_163,_x31 WHERE _cachedrel_163.a3 = _x31.a1) AS t__x203),"
	    "	_x135 AS (SELECT a1, prob FROM (SELECT _cachedrel_60.a1 AS a1, _x134.a1 AS a2, _cachedrel_60.prob * "
	    "_x134.prob AS prob FROM _cachedrel_60,_x134 WHERE _cachedrel_60.a1 = _x134.a1) AS t__x204),"
	    "	_x136 AS (SELECT CAST(count(prob) AS DECIMAL) AS prob FROM _x135),"
	    "	_x137 AS (SELECT a1, prob FROM (SELECT _x135.a1 AS a1, _x135.prob * t__x207.prob AS prob FROM "
	    "_x135,(SELECT prob FROM (SELECT CASE WHEN prob > CAST(0 AS DECIMAL) THEN CAST(1 AS DECIMAL) ELSE CAST(0 AS "
	    "DECIMAL) END AS prob FROM _x136) AS t__x206 WHERE prob > 0.0) AS t__x207 UNION ALL SELECT _x133.a1 AS a1, "
	    "_x133.prob * t__x210.prob AS prob FROM _x133,(SELECT prob FROM (SELECT CASE WHEN prob > CAST(0 AS DECIMAL) "
	    "THEN CAST(0 AS DECIMAL) ELSE CAST(1 AS DECIMAL) END AS prob FROM _x136) AS t__x209 WHERE prob > 0.0) AS "
	    "t__x210) AS t__x211),"
	    "	_x138 AS (SELECT a1, sum(prob) AS prob FROM (SELECT _x137.a1 AS a1, _cachedrel_60.a1 AS a2, _x137.prob * "
	    "_cachedrel_60.prob AS prob FROM _x137,_cachedrel_60 WHERE _x137.a1 = _cachedrel_60.a1) AS t__x213 GROUP BY a1)"
	    "	 SELECT a1, prob FROM _x138;");

	REQUIRE_NO_FAIL(con.Query("ROLLBACK;"));
}
