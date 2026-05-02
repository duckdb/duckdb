#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"

using namespace duckdb;

static void TestArrowRoundtrip(const string &query, bool export_large_buffer = false,
                               bool lossless_conversion = false) {
	DuckDB db;
	Connection con(db);
	if (export_large_buffer) {
		auto res = con.Query("SET arrow_large_buffer_size=True");
		REQUIRE(!res->HasError());
	}
	if (lossless_conversion) {
		auto res = con.Query("SET arrow_lossless_conversion = true");
		REQUIRE(!res->HasError());
	}
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, query, true));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, query, false));
}

static void TestArrowRoundtripStringView(const string &query) {
	DuckDB db;
	Connection con(db);
	auto res = con.Query("SET produce_arrow_string_view=True");
	REQUIRE(!res->HasError());
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, query, false));
}

static void TestParquetRoundtrip(const string &path) {
	DBConfig config;
	// This needs to be set since this test will be triggered when testing autoloading
	config.SetOptionByName("allow_unsigned_extensions", true);

	DuckDB db(nullptr, &config);
	Connection con(db);

	// run the query
	auto query = "SELECT * FROM parquet_scan('" + path + "')";
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, query, true));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, query));
}

TEST_CASE("Test Export Large", "[arrow]") {
	// Test with Regular Buffer Size
	TestArrowRoundtrip("SELECT 'bla' FROM range(10000)");

	TestArrowRoundtrip("SELECT 'bla'::BLOB FROM range(10000)");

	TestArrowRoundtrip("SELECT '3d038406-6275-4aae-bec1-1235ccdeaade'::UUID FROM range(10000) tbl(i)", false, true);

	// Test with Large Buffer Size
	TestArrowRoundtrip("SELECT 'bla' FROM range(10000)", true);

	TestArrowRoundtrip("SELECT 'bla'::BLOB FROM range(10000)", true);

	TestArrowRoundtrip("SELECT '3d038406-6275-4aae-bec1-1235ccdeaade'::UUID FROM range(10000) tbl(i)", true, true);
}

TEST_CASE("Test arrow roundtrip", "[arrow]") {
	TestArrowRoundtrip("SELECT * FROM range(10000) tbl(i) UNION ALL SELECT NULL");
	TestArrowRoundtrip("SELECT m from (select MAP(list_value(1), list_value(2)) from range(5) tbl(i)) tbl(m)");
	TestArrowRoundtrip("SELECT * FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT case when i%2=0 then null else i end i FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT case when i%2=0 then true else false end b FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT case when i%2=0 then i%4=0 else null end b FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT 'thisisalongstring'||i::varchar str FROM range(10) tbl(i)");
	TestArrowRoundtrip(
	    "SELECT case when i%2=0 then null else 'thisisalongstring'||i::varchar end str FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT {'i': i, 'b': 10-i} str FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT case when i%2=0 then {'i': case when i%4=0 then null else i end, 'b': 10-i} else null "
	                   "end str FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT [i, i+1, i+2] FROM range(10) tbl(i)");
	TestArrowRoundtrip(
	    "SELECT MAP(LIST_VALUE({'i':1,'j':2},{'i':3,'j':4}),LIST_VALUE({'i':1,'j':2},{'i':3,'j':4})) as a");
	TestArrowRoundtrip(
	    "SELECT MAP(LIST_VALUE({'i':i,'j':i+2},{'i':3,'j':NULL}),LIST_VALUE({'i':i+10,'j':2},{'i':i+4,'j':4})) as a "
	    "FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT MAP(['hello', 'world'||i::VARCHAR],[i + 1, NULL]) as a FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT (1.5 + i)::DECIMAL(4,2) dec4, (1.5 + i)::DECIMAL(9,3) dec9, (1.5 + i)::DECIMAL(18,3) "
	                   "dec18, (1.5 + i)::DECIMAL(38,3) dec38 FROM range(10) tbl(i)");
	TestArrowRoundtrip(
	    "SELECT case when i%2=0 then null else INTERVAL (i) seconds end AS interval FROM range(10) tbl(i)");
#if STANDARD_VECTOR_SIZE < 64
	// FIXME: there seems to be a bug in the enum arrow reader in this test when run with vsize=2
	return;
#endif
	TestArrowRoundtrip("SELECT * FROM test_all_types()", false, true);
}

TEST_CASE("Test Arrow fixed-size binary format parsing", "[arrow]") {
	// Verify that GetTypeFromFormat correctly parses the size from "w:NN" format strings.
	// Regression test for duckdb/duckdb-wasm#2199: format.find(':') would match colons
	// in extension metadata (e.g. CRS strings like "ogc:crs84"), causing std::stoi to crash.
	{
		string format = "w:16";
		auto type = ArrowType::GetTypeFromFormat(format);
		REQUIRE(type);
		REQUIRE(type->GetDuckType() == LogicalType::BLOB);
	}
	{
		string format = "w:1";
		auto type = ArrowType::GetTypeFromFormat(format);
		REQUIRE(type);
		REQUIRE(type->GetDuckType() == LogicalType::BLOB);
	}
	{
		string format = "w:128";
		auto type = ArrowType::GetTypeFromFormat(format);
		REQUIRE(type);
		REQUIRE(type->GetDuckType() == LogicalType::BLOB);
	}
}

static void SetupUnionTable(Connection &con, idx_t num_rows, bool with_nulls = false) {
	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE union_tbl(u UNION(i INT, s VARCHAR))"));
	// Insert alternating int and string union members via separate statements
	REQUIRE_NO_FAIL(con.Query("INSERT INTO union_tbl SELECT union_value(i := i::INT) FROM range(" +
	                          to_string(num_rows) + ") tbl(i) WHERE i % 2 = 0"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO union_tbl SELECT union_value(s := 'val' || i::VARCHAR) FROM range(" +
	                          to_string(num_rows) + ") tbl(i) WHERE i % 2 = 1"));
	if (with_nulls) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO union_tbl SELECT NULL::UNION(i INT, s VARCHAR) FROM range(" +
		                          to_string(num_rows / 5) + ") tbl(i)"));
	}
}

TEST_CASE("Test Arrow UNION type roundtrip", "[arrow]") {
	DuckDB db;
	Connection con(db);

	// Small union with mixed tags
	SetupUnionTable(con, 10);
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", false));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", true));

	// Union with NULLs
	SetupUnionTable(con, 10, true);
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", false));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", true));

	// Single-member union
	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE union_tbl(u UNION(i INT))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO union_tbl SELECT union_value(i := i::INT) FROM range(10) tbl(i)"));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", false));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", true));

	// All-NULL union column
	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE union_tbl(u UNION(i INT, s VARCHAR))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO union_tbl SELECT NULL::UNION(i INT, s VARCHAR) FROM range(10) tbl(i)"));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", false));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", true));

	// Single-tag-only: all rows use the same member, other member completely empty
	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE union_tbl(u UNION(i INT, s VARCHAR))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO union_tbl SELECT union_value(i := i::INT) FROM range(10000) tbl(i)"));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", false));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", true));

	// Union alongside other columns
	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE union_tbl(id INT, u UNION(i INT, s VARCHAR), label VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO union_tbl SELECT i, union_value(i := i::INT), 'row' || i::VARCHAR "
	                          "FROM range(10000) tbl(i) WHERE i % 2 = 0"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO union_tbl SELECT i, union_value(s := 'val' || i::VARCHAR), 'row' || "
	                          "i::VARCHAR FROM range(10000) tbl(i) WHERE i % 2 = 1"));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", false));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", true));

	// Nested struct as union member
	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE union_tbl(u UNION(a INT, b STRUCT(x INT, y VARCHAR)))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO union_tbl SELECT union_value(a := i::INT) FROM range(10000) tbl(i) "
	                          "WHERE i % 2 = 0"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO union_tbl SELECT union_value(b := ROW(i, 'v' || i::VARCHAR)) "
	                          "FROM range(10000) tbl(i) WHERE i % 2 = 1"));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", false));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", true));

	// Union inside a struct
	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE union_tbl(s STRUCT(tag INT, u UNION(i INT, v VARCHAR)))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO union_tbl SELECT ROW(i, union_value(i := i::INT)) "
	                          "FROM range(10000) tbl(i) WHERE i % 2 = 0"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO union_tbl SELECT ROW(i, union_value(v := i::VARCHAR)) "
	                          "FROM range(10000) tbl(i) WHERE i % 2 = 1"));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", false));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", true));

	// Large batch - exercises chunk_offset across multiple scan passes (> STANDARD_VECTOR_SIZE)
	SetupUnionTable(con, 10000);
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", false));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", true));

	// Large batch with NULLs
	SetupUnionTable(con, 10000, true);
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", false));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl", true));

	// Three-member union, large batch
	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE union_tbl3(u UNION(a INT, b FLOAT, c VARCHAR))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO union_tbl3 SELECT union_value(a := i::INT) FROM range(10000) tbl(i) "
	                          "WHERE i % 3 = 0"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO union_tbl3 SELECT union_value(b := (i * 1.5)::FLOAT) FROM range(10000) "
	                          "tbl(i) WHERE i % 3 = 1"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO union_tbl3 SELECT union_value(c := 'str' || i::VARCHAR) FROM "
	                          "range(10000) tbl(i) WHERE i % 3 = 2"));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl3", false));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM union_tbl3", true));
}

// Regression: a BOOLEAN child of any container (UNION, STRUCT, LIST,
// FIXED_SIZE_LIST, MAP) must keep the appender's data layout in sync with
// the schema emitted by SetArrowFormat. Under arrow_lossless_conversion=true,
// the schema declares the bool child as the arrow.bool8 extension
// (byte-packed int8), so the appender must also push values through that
// extension. Previously every container appender called InitializeChild
// without any extension info and used the plain bit-packed bool appender,
// so the bytes on the wire described 4 bools per byte while the schema
// said 1 bool per byte; consumers misread every row past the first as
// `false`. For MAP keys this even crashes ingest with a duplicate-key error.
TEST_CASE("Test Arrow nested BOOLEAN with arrow.bool8 roundtrip", "[arrow]") {
	// --- STRUCT(... BOOLEAN ...) ---
	TestArrowRoundtrip("SELECT {'b': (i % 2 = 0)}::STRUCT(b BOOLEAN) AS s FROM range(64) tbl(i)", false, true);
	TestArrowRoundtrip("SELECT {'tag': i, 'flag': (i % 3 = 0), 'name': 'r' || i::VARCHAR} "
	                   "::STRUCT(tag INT, flag BOOLEAN, \"name\" VARCHAR) AS s FROM range(10000) tbl(i)",
	                   false, true);

	// --- LIST(BOOLEAN) ---
	TestArrowRoundtrip("SELECT [(i % 2 = 0), (i % 3 = 0)]::BOOLEAN[] AS l FROM range(64) tbl(i)", false, true);
	// Large batch with >STANDARD_VECTOR_SIZE child elements per chunk: regression
	// for the case where the converted internal vector is sized only for
	// STANDARD_VECTOR_SIZE and duckdb_to_arrow writes past the end.
	TestArrowRoundtrip("SELECT [(i % 2 = 0), (i % 3 = 0), (i % 5 = 0)]::BOOLEAN[] AS l FROM range(10000) tbl(i)", false,
	                   true);

	// --- FIXED_SIZE_LIST[3](BOOLEAN) ---
	TestArrowRoundtrip("SELECT [(i % 2 = 0), (i % 3 = 0), true]::BOOLEAN[3] AS fa FROM range(64) tbl(i)", false, true);

	// --- MAP(VARCHAR, BOOLEAN) ---
	TestArrowRoundtrip("SELECT MAP {'a': (i % 2 = 0), 'b': (i % 3 = 0)} AS m FROM range(64) tbl(i)", false, true);

	// --- MAP(BOOLEAN, INT) — bool-as-key (previously crashed ingest with a
	// duplicate-key error because every key collapsed to the same byte value).
	TestArrowRoundtrip("SELECT MAP {true: i, false: i + 1} AS m FROM range(64) tbl(i)", false, true);

	// --- LIST(STRUCT(BOOLEAN)) and STRUCT(LIST(BOOLEAN)) ---
	TestArrowRoundtrip("SELECT [{'flag': (i % 2 = 0)}, {'flag': (i % 3 = 0)}]::STRUCT(flag BOOLEAN)[] AS l "
	                   "FROM range(64) tbl(i)",
	                   false, true);
	TestArrowRoundtrip("SELECT {'flags': [(i % 2 = 0), true, (i % 3 = 0)]}::STRUCT(flags BOOLEAN[]) AS s "
	                   "FROM range(64) tbl(i)",
	                   false, true);

	// --- LIST(BOOLEAN) with mixed NULL container rows ---
	TestArrowRoundtrip("SELECT * FROM (VALUES ([true, false]::BOOLEAN[]), (NULL), ([(true), (false), (true)])) t(l)",
	                   false, true);
}

// Regression for BOOLEAN children of a UNION (the original bug from
// duckdb#22444). Kept as a separate case so failures point straight at
// the union path even after the centralised fix lands.
TEST_CASE("Test Arrow UNION with BOOLEAN member roundtrip", "[arrow]") {
	// Default mode (lossless = false): schema says "b" (bit-packed), data
	// is bit-packed - no extension involved, regression is N/A but worth
	// keeping as a backwards-compat guard.
	TestArrowRoundtrip("SELECT union_value(b := (i % 2 = 0))::UNION(i INT, b BOOLEAN) AS u "
	                   "FROM range(64) tbl(i)");
	TestArrowRoundtrip("SELECT * FROM (VALUES "
	                   "(union_value(b := true)::UNION(i INT, b BOOLEAN)), "
	                   "(union_value(b := false)), "
	                   "(union_value(b := true)), "
	                   "(union_value(b := false))) t(u)");

	// Lossless mode: schema says arrow.bool8 (byte-packed) - exercises the
	// path the regression is on.
	TestArrowRoundtrip("SELECT union_value(b := true)::UNION(i INT, b BOOLEAN) AS u "
	                   "FROM range(4) tbl(i)",
	                   false, true);
	TestArrowRoundtrip("SELECT union_value(b := (i % 2 = 0))::UNION(i INT, b BOOLEAN) AS u "
	                   "FROM range(64) tbl(i)",
	                   false, true);
	// Mixed variants: bool variant interleaved with int variant.
	TestArrowRoundtrip("SELECT * FROM (VALUES "
	                   "(union_value(i := 1)::UNION(i INT, b BOOLEAN)), "
	                   "(union_value(b := true)), "
	                   "(union_value(i := 7)), "
	                   "(union_value(b := true)), "
	                   "(union_value(b := false))) t(u)",
	                   false, true);
	// Three-member union including BOOLEAN.
	TestArrowRoundtrip("SELECT * FROM (VALUES "
	                   "(union_value(i := 1)::UNION(i INT, s VARCHAR, b BOOLEAN)), "
	                   "(union_value(s := 'x')), "
	                   "(union_value(b := true)), "
	                   "(union_value(b := false)), "
	                   "(union_value(i := 99))) t(u)",
	                   false, true);
	// Large batch crossing STANDARD_VECTOR_SIZE.
	TestArrowRoundtrip("SELECT union_value(b := (i % 2 = 0))::UNION(i INT, b BOOLEAN) AS u "
	                   "FROM range(10000) tbl(i)",
	                   false, true);
	// Bool-in-union nested inside a struct.
	TestArrowRoundtrip("SELECT {'tag': i, 'val': union_value(b := (i % 2 = 0))} "
	                   "::STRUCT(tag INT, val UNION(i INT, b BOOLEAN)) AS s "
	                   "FROM range(64) tbl(i)",
	                   false, true);
	// Bool-in-union with NULL rows.
	TestArrowRoundtrip("SELECT * FROM (VALUES "
	                   "(union_value(b := true)::UNION(i INT, b BOOLEAN)), "
	                   "(NULL), "
	                   "(union_value(b := false)), "
	                   "(NULL), "
	                   "(union_value(b := true))) t(u)",
	                   false, true);
}
TEST_CASE("Test Arrow Extension Types", "[arrow][.]") {
	// UUID
	TestArrowRoundtrip("SELECT '2d89ebe6-1e13-47e5-803a-b81c87660b66'::UUID str FROM range(5) tbl(i)", false, true);

	// HUGEINT
	TestArrowRoundtrip("SELECT '170141183460469231731687303715884105727'::HUGEINT str FROM range(5) tbl(i)", false,
	                   true);

	// UHUGEINT
	TestArrowRoundtrip("SELECT '170141183460469231731687303715884105727'::UHUGEINT str FROM range(5) tbl(i)", false,
	                   true);

	// UHUGEINT (lossy - should export as Decimal(38,0), not extension type)
	{
		DuckDB db;
		Connection con(db);
		auto client_properties = con.context->GetClientProperties();
		ArrowSchema schema;
		schema.Init();
		vector<LogicalType> types = {LogicalType::UHUGEINT};
		vector<string> names = {"col"};
		ArrowConverter::ToArrowSchema(&schema, types, names, client_properties);
		REQUIRE(schema.n_children == 1);
		REQUIRE(string(schema.children[0]->format) == "d:38,0");
		schema.release(&schema);
	}

	// BIT
	TestArrowRoundtrip("SELECT '0101011'::BIT str FROM range(5) tbl(i)", false, true);

	// TIME_TZ
	TestArrowRoundtrip("SELECT '02:30:00+04'::TIMETZ str FROM range(5) tbl(i)", false, true);

	// BIGNUM
	TestArrowRoundtrip("SELECT 85070591730234614260976917445211069672::BIGNUM str FROM range(5) tbl(i)", false, true);

	TestArrowRoundtrip("SELECT 85070591730234614260976917445211069672::BIGNUM str FROM range(5) tbl(i)", true, true);
}

TEST_CASE("Test Arrow Extension Types - JSON", "[arrow][.]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	Connection con(db);

	if (!db.ExtensionIsLoaded("json")) {
		return;
	}

	// JSON
	TestArrowRoundtrip("SELECT '{\"name\":\"Pedro\", \"age\":28, \"car\":\"VW Fox\"}'::JSON str FROM range(5) tbl(i)",
	                   false, true);
}

TEST_CASE("Test Arrow String View", "[arrow][.]") {
	// Test Small Strings
	TestArrowRoundtripStringView("SELECT (i*10^i)::varchar str FROM range(5) tbl(i)");

	// Test Small Strings + Nulls
	TestArrowRoundtripStringView("SELECT (i*10^i)::varchar str FROM range(5) tbl(i) UNION SELECT NULL");

	// Test Big Strings
	TestArrowRoundtripStringView("SELECT 'Imaverybigstringmuchbiggerthanfourbytes' str FROM range(5) tbl(i)");

	// Test Big Strings + Nulls
	TestArrowRoundtripStringView("SELECT 'Imaverybigstringmuchbiggerthanfourbytes'||i::varchar str FROM range(5) "
	                             "tbl(i) UNION SELECT NULL order by str");

	// Test Mix of Small/Big/NULL Strings
	TestArrowRoundtripStringView(
	    "SELECT 'Imaverybigstringmuchbiggerthanfourbytes'||i::varchar str FROM range(10000) tbl(i) UNION "
	    "SELECT NULL UNION SELECT (i*10^i)::varchar str FROM range(10000) tbl(i)");
}

TEST_CASE("Test TPCH arrow roundtrip", "[arrow][.]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	Connection con(db);

#if defined(D_ASSERT_IS_ENABLED) && !defined(DEBUG)
	return; // Skip in relassert, takes too long
#endif

	if (!db.ExtensionIsLoaded("tpch")) {
		return;
	}
	con.SendQuery("CALL dbgen(sf=0.5)");

	// REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM lineitem;", false));
	// REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT l_orderkey, l_shipdate, l_comment FROM lineitem ORDER BY
	// l_orderkey DESC;", false)); REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT lineitem FROM lineitem;",
	// false)); REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT [lineitem] FROM lineitem;", false));

	con.SendQuery("create table lineitem_no_constraint as from lineitem;");
	con.SendQuery("update lineitem_no_constraint set l_comment=null where l_orderkey%2=0;");

	// REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM lineitem_no_constraint;", false));
	REQUIRE(ArrowTestHelper::RunArrowComparison(
	    con, "SELECT l_orderkey, l_shipdate, l_comment FROM lineitem_no_constraint ORDER BY l_orderkey DESC;", false));
	REQUIRE(
	    ArrowTestHelper::RunArrowComparison(con, "SELECT lineitem_no_constraint FROM lineitem_no_constraint;", false));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT [lineitem_no_constraint] FROM lineitem_no_constraint;",
	                                            false));
}

TEST_CASE("Test Parquet Files round-trip", "[arrow][.]") {
	std::vector<std::string> data;
	// data.emplace_back("data/parquet-testing/7-set.snappy.arrow2.parquet");
	//	data.emplace_back("data/parquet-testing/adam_genotypes.parquet");
	data.emplace_back("data/parquet-testing/apkwan.parquet");
	data.emplace_back("data/parquet-testing/aws1.snappy.parquet");
	// not supported by arrow
	// data.emplace_back("data/parquet-testing/aws2.parquet");
	data.emplace_back("data/parquet-testing/binary_string.parquet");
	data.emplace_back("data/parquet-testing/blob.parquet");
	data.emplace_back("data/parquet-testing/boolean_stats.parquet");
	// arrow can't read this
	// data.emplace_back("data/parquet-testing/broken-arrow.parquet");
	data.emplace_back("data/parquet-testing/bug1554.parquet");
	data.emplace_back("data/parquet-testing/bug1588.parquet");
	data.emplace_back("data/parquet-testing/bug1589.parquet");
	data.emplace_back("data/parquet-testing/bug1618_struct_strings.parquet");
	data.emplace_back("data/parquet-testing/bug2267.parquet");
	data.emplace_back("data/parquet-testing/bug2557.parquet");
	// slow
	// data.emplace_back("data/parquet-testing/bug687_nulls.parquet");
	// data.emplace_back("data/parquet-testing/complex.parquet");
	data.emplace_back("data/parquet-testing/data-types.parquet");
	data.emplace_back("data/parquet-testing/date.parquet");
	// arrow can't read this because it's a time with a timezone and it's not supported by arrow
	//	data.emplace_back("data/parquet-testing/date_stats.parquet");
	data.emplace_back("data/parquet-testing/decimal_stats.parquet");
	data.emplace_back("data/parquet-testing/decimals.parquet");
	data.emplace_back("data/parquet-testing/enum.parquet");
	data.emplace_back("data/parquet-testing/filter_bug1391.parquet");
	//	data.emplace_back("data/parquet-testing/fixed.parquet");
	// slow
	// data.emplace_back("data/parquet-testing/leftdate3_192_loop_1.parquet");
	data.emplace_back("data/parquet-testing/lineitem-top10000.gzip.parquet");
	data.emplace_back("data/parquet-testing/manyrowgroups.parquet");
	data.emplace_back("data/parquet-testing/manyrowgroups2.parquet");
	//	data.emplace_back("data/parquet-testing/map.parquet");
	// Can't roundtrip NaNs
	data.emplace_back("data/parquet-testing/nan-float.parquet");
	// null byte in file
	// data.emplace_back("data/parquet-testing/nullbyte.parquet");
	// data.emplace_back("data/parquet-testing/nullbyte_multiple.parquet");
	// borked
	// data.emplace_back("data/parquet-testing/p2.parquet");
	// data.emplace_back("data/parquet-testing/p2strings.parquet");
	data.emplace_back("data/parquet-testing/pandas-date.parquet");
	data.emplace_back("data/parquet-testing/signed_stats.parquet");
	data.emplace_back("data/parquet-testing/silly-names.parquet");
	// borked
	// data.emplace_back("data/parquet-testing/simple.parquet");
	// data.emplace_back("data/parquet-testing/sorted.zstd_18_131072_small.parquet");
	data.emplace_back("data/parquet-testing/struct.parquet");
	data.emplace_back("data/parquet-testing/struct_skip_test.parquet");
	data.emplace_back("data/parquet-testing/timestamp-ms.parquet");
	data.emplace_back("data/parquet-testing/timestamp.parquet");
	data.emplace_back("data/parquet-testing/unsigned.parquet");
	data.emplace_back("data/parquet-testing/unsigned_stats.parquet");
	data.emplace_back("data/parquet-testing/userdata1.parquet");
	data.emplace_back("data/parquet-testing/varchar_stats.parquet");
	data.emplace_back("data/parquet-testing/zstd.parquet");

	for (auto &parquet_path : data) {
		TestParquetRoundtrip(parquet_path);
	}
}
