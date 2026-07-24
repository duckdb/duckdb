#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/arrow/arrow_type_extension.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table/arrow/arrow_type_info.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include <atomic>

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

TEST_CASE("Test arrow NULL value roundtrip", "[arrow]") {
	// null types
	TestArrowRoundtrip("SELECT NULL");
	TestArrowRoundtrip("SELECT [NULL, NULL]");
	TestArrowRoundtrip("SELECT {'x': NULL, 'y': NULL}");
	TestArrowRoundtrip("SELECT [{'x': NULL, 'y': NULL}, {'x': NULL, 'y': NULL}]");
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

// Regression for duckdb#22444: under arrow_lossless_conversion a BOOLEAN child of a
// nested type is declared as arrow.bool8 (byte-packed) in the schema, so its data must
// be byte-packed too. Nested BOOLEAN children used to be written bit-packed, so every
// row past the first read back wrong.
TEST_CASE("Test Arrow nested BOOLEAN roundtrip", "[arrow]") {
	// BOOLEAN inside each container type.
	TestArrowRoundtrip("SELECT {'b': (i % 2 = 0)}::STRUCT(b BOOLEAN) AS s FROM range(64) tbl(i)", false, true);
	TestArrowRoundtrip("SELECT [(i % 2 = 0), (i % 3 = 0)]::BOOLEAN[] AS l FROM range(64) tbl(i)", false, true);
	TestArrowRoundtrip("SELECT [(i % 2 = 0), true, (i % 3 = 0)]::BOOLEAN[3] AS a FROM range(64) tbl(i)", false, true);
	TestArrowRoundtrip("SELECT MAP {'x': (i % 2 = 0), 'y': (i % 3 = 0)} AS m FROM range(64) tbl(i)", false, true);
	TestArrowRoundtrip("SELECT union_value(b := (i % 2 = 0))::UNION(i INT, b BOOLEAN) AS u FROM range(64) tbl(i)",
	                   false, true);

	// BOOLEAN as a MAP key: keys used to collapse to one byte value, crashing ingest with
	// a duplicate-key error.
	TestArrowRoundtrip("SELECT MAP {true: i, false: i + 1} AS m FROM range(64) tbl(i)", false, true);

	// Two levels of nesting.
	TestArrowRoundtrip("SELECT [{'b': (i % 2 = 0)}]::STRUCT(b BOOLEAN)[] AS l FROM range(64) tbl(i)", false, true);
}

// A constant BOOLEAN child reaches the arrow.bool8 conversion with a non-identity
// selection (every row maps to the single value); it must still expand to that value
// for every row.
TEST_CASE("Test Arrow constant BOOLEAN child roundtrip", "[arrow]") {
	TestArrowRoundtrip("SELECT {'c': true, 'v': (i % 2 = 0)}::STRUCT(c BOOLEAN, v BOOLEAN) AS s FROM range(64) tbl(i)",
	                   false, true);
	TestArrowRoundtrip("SELECT [true, true]::BOOLEAN[] AS l FROM range(64) tbl(i)", false, true);
}

// A LIST(BOOLEAN) whose total child element count per chunk exceeds STANDARD_VECTOR_SIZE:
// guards the converted vector being sized to the actual child count rather than 2048.
TEST_CASE("Test Arrow large LIST(BOOLEAN) roundtrip", "[arrow]") {
	TestArrowRoundtrip("SELECT [(i % 2 = 0), (i % 3 = 0), (i % 5 = 0)]::BOOLEAN[] AS l FROM range(10000) tbl(i)", false,
	                   true);
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
	    con, "SELECT l_orderkey, l_shipdate, l_comment FROM lineitem_no_constraint ORDER BY l_orderkey DESC;", true));
	REQUIRE(
	    ArrowTestHelper::RunArrowComparison(con, "SELECT lineitem_no_constraint FROM lineitem_no_constraint;", true));
	REQUIRE(
	    ArrowTestHelper::RunArrowComparison(con, "SELECT [lineitem_no_constraint] FROM lineitem_no_constraint;", true));
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

//===--------------------------------------------------------------------===//
// Registered extension type with a NESTED (struct) storage type
//===--------------------------------------------------------------------===//
// A synthetic extension whose Arrow STORAGE type is a struct with a DIFFERENT child count than the
// logical type: logical = STRUCT(a BIGINT, b BIGINT) registered under the alias "arrow_test_pair",
// storage = STRUCT(packed STRUCT(a BIGINT, b BIGINT)) — a single wrapping child. This mirrors the shape
// of e.g. the canonical arrow.parquet.variant extension (a logical VARIANT over struct<metadata, value>
// storage). The appender initializes and appends with the extension's INTERNAL (storage) type, so it
// must also FINALIZE with it: before the fix, FinalizeChild handed the finalizer the LOGICAL type, and
// the struct finalizer walked the logical child count over the storage-built child_data — out of bounds.

namespace {

//! Engagement counters: the roundtrip silently degrades to a plain-struct roundtrip (values still
//! compare equal!) if the extension is not picked up — assert every stage actually ran.
static std::atomic<int> pair_populate_calls {0};
static std::atomic<int> pair_get_type_calls {0};
static std::atomic<int> pair_duck_to_arrow_calls {0};
static std::atomic<int> pair_arrow_to_duck_calls {0};

LogicalType PairStorageMemberType() {
	return LogicalType::STRUCT({{"a", LogicalType::BIGINT}, {"b", LogicalType::BIGINT}});
}

LogicalType PairLogicalType() {
	// The extension registry keys on (alias, type id), and the schema conversion consults it for
	// alias-carrying types — the logical pair type therefore carries an alias. (A SQL cast to a
	// CREATE TYPE alias resolves the alias away, so the test produces values through a registered
	// scalar function whose RETURN type carries it — the same way extension-defined types do.)
	auto type = PairStorageMemberType();
	type.SetAlias("arrow_test_pair");
	return type;
}

// pair_typed(STRUCT(a, b)) -> the alias-carrying logical pair type (children pass through).
void PairTypedFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &in = args.data[0];
	in.Flatten();
	auto &in_entries = StructVector::GetEntries(in);
	auto &out_entries = StructVector::GetEntries(result);
	out_entries[0].Reference(in_entries[0]);
	out_entries[1].Reference(in_entries[1]);
	for (idx_t i = 0; i < args.size(); i++) {
		if (FlatVector::IsNull(in, i)) {
			FlatVector::SetNull(result, i, true);
		}
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
}

LogicalType PairStorageType() {
	return LogicalType::STRUCT({{"packed", PairStorageMemberType()}});
}

// DuckDB -> Arrow: wrap the logical pair rows in the single storage child (buffers shared, no copy).
void PairDuckToArrow(ClientContext &, const Vector &source, Vector &result, idx_t count) {
	++pair_duck_to_arrow_calls;
	source.Flatten();
	auto &packed = StructVector::GetEntries(result)[0];
	// Reinterpret, not Reference: the storage child and the logical pair type have the same physical layout
	// but differ in the ALIAS, which LogicalType equality compares - so Reference would trip its type
	// assertion in an assertion-enabled build.
	packed.Reinterpret(source);
	for (idx_t i = 0; i < count; i++) {
		if (FlatVector::IsNull(source, i)) {
			FlatVector::SetNull(result, i, true);
		}
	}
}

// Arrow -> DuckDB: unwrap the storage child back into the logical pair rows.
void PairArrowToDuck(ClientContext &, Vector &source, Vector &result, idx_t count) {
	++pair_arrow_to_duck_calls;
	source.Flatten();
	auto &packed = StructVector::GetEntries(source)[0];
	packed.Flatten();
	auto &packed_entries = StructVector::GetEntries(packed);
	auto &result_entries = StructVector::GetEntries(result);
	result_entries[0].Reference(packed_entries[0]);
	result_entries[1].Reference(packed_entries[1]);
	for (idx_t i = 0; i < count; i++) {
		if (FlatVector::IsNull(source, i) || FlatVector::IsNull(packed, i)) {
			FlatVector::SetNull(result, i, true);
		}
	}
}

// Declares the STORAGE schema (struct<packed: struct<a, b>>) tagged with the extension name. All names
// and formats are string literals (static storage), so no ownership registration is needed.
void PairPopulateSchema(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &schema, const LogicalType &, ClientContext &,
                        const ArrowTypeExtension &extension) {
	++pair_populate_calls;
	const auto metadata = ArrowSchemaMetadata::ArrowCanonicalType(extension.GetInfo().GetExtensionName());
	root_holder.metadata_info.emplace_back(metadata.SerializeMetadata());
	schema.metadata = root_holder.metadata_info.back().get();

	auto release_child = [](ArrowSchema *child) {
		child->release = nullptr;
	};

	schema.format = "+s";
	schema.n_children = 1;
	root_holder.nested_children.emplace_back();
	root_holder.nested_children.back().resize(1);
	root_holder.nested_children_ptr.emplace_back();
	root_holder.nested_children_ptr.back().push_back(&root_holder.nested_children.back()[0]);
	schema.children = root_holder.nested_children_ptr.back().data();

	auto &packed = *schema.children[0];
	packed.format = "+s";
	packed.name = "packed";
	packed.flags = ARROW_FLAG_NULLABLE;
	packed.release = release_child;
	packed.n_children = 2;
	root_holder.nested_children.emplace_back();
	root_holder.nested_children.back().resize(2);
	root_holder.nested_children_ptr.emplace_back();
	root_holder.nested_children_ptr.back().push_back(&root_holder.nested_children.back()[0]);
	root_holder.nested_children_ptr.back().push_back(&root_holder.nested_children.back()[1]);
	packed.children = root_holder.nested_children_ptr.back().data();

	const char *member_names[] = {"a", "b"};
	for (idx_t i = 0; i < 2; i++) {
		auto &member = *packed.children[i];
		member.format = "l";
		member.name = member_names[i];
		member.flags = ARROW_FLAG_NULLABLE;
		member.release = release_child;
	}
}

// Maps the tagged schema back: the LOGICAL type for the result, with type info describing the STORAGE
// tree for the reader's buffer walk.
unique_ptr<ArrowType> PairGetType(ClientContext &, const ArrowSchema &, const ArrowSchemaMetadata &) {
	++pair_get_type_calls;
	vector<shared_ptr<ArrowType>> members;
	members.push_back(make_shared_ptr<ArrowType>(LogicalType::BIGINT));
	members.push_back(make_shared_ptr<ArrowType>(LogicalType::BIGINT));
	auto packed = make_shared_ptr<ArrowType>(PairStorageMemberType(), make_uniq<ArrowStructInfo>(std::move(members)));
	vector<shared_ptr<ArrowType>> children;
	children.push_back(std::move(packed));
	return make_uniq<ArrowType>(PairLogicalType(), make_uniq<ArrowStructInfo>(std::move(children)));
}

} // namespace

TEST_CASE("Test Arrow extension type with nested storage", "[arrow]") {
	DuckDB db(nullptr, nullptr);
	// register on the INSTANCE's config — the extension registry is not carried over from a user config
	DBConfig::GetConfig(*db.instance)
	    .RegisterArrowExtension({"arrow_test.pair", &PairPopulateSchema, &PairGetType,
	                             make_shared_ptr<ArrowTypeExtensionData>(PairLogicalType(), PairStorageType(),
	                                                                     PairArrowToDuck, PairDuckToArrow)});
	Connection con(db);

	// values reach the boundary through a function whose RETURN type carries the registered alias
	ScalarFunction fn("pair_typed", {PairStorageMemberType()}, PairLogicalType(), PairTypedFunction);
	CreateScalarFunctionInfo fn_info(fn);
	con.context->RunFunctionInTransaction(
	    [&]() { Catalog::GetSystemCatalog(*con.context).CreateFunction(*con.context, fn_info); });

	// plain values (multiple vectors' worth) + NULL rows + NULL members
	REQUIRE(
	    ArrowTestHelper::RunArrowComparison(con, "SELECT pair_typed({'a': i, 'b': i + 1}) AS p FROM range(3000) t(i)"));
	REQUIRE(ArrowTestHelper::RunArrowComparison(
	    con, "SELECT pair_typed(CASE WHEN i % 3 = 0 THEN NULL WHEN i % 3 = 1 THEN {'a': i, 'b': NULL} "
	         "ELSE {'a': NULL, 'b': i} END) AS p FROM range(100) t(i)"));

	// the roundtrip must have gone THROUGH the extension (schema declared, values converted both ways) —
	// otherwise the comparison above passes vacuously on a plain-struct roundtrip
	REQUIRE(pair_populate_calls.load() > 0);
	REQUIRE(pair_duck_to_arrow_calls.load() > 0);
	REQUIRE(pair_get_type_calls.load() > 0);
	REQUIRE(pair_arrow_to_duck_calls.load() > 0);
}

TEST_CASE("Test Arrow VARIANT roundtrip", "[arrow]") {
	// VARIANT travels as the canonical arrow.parquet.variant extension type (registered by the parquet
	// extension, statically linked here): struct<metadata: binary, value: binary> storage carrying the
	// Variant spec's binary encoding.
	TestArrowRoundtrip("SELECT 42::VARIANT AS v");
	TestArrowRoundtrip("SELECT {'a': i, 'b': 'x' || i::VARCHAR}::VARIANT AS v FROM range(3000) t(i)");
	TestArrowRoundtrip("SELECT [i, i + 1, NULL]::VARIANT AS v FROM range(100) t(i)");
	TestArrowRoundtrip("SELECT CASE WHEN i % 2 = 0 THEN i::VARIANT ELSE ('s' || i::VARCHAR)::VARIANT END "
	                   "AS v FROM range(10) t(i)"); // mixed types, no NULLs
	TestArrowRoundtrip("SELECT CASE WHEN i % 2 = 0 THEN NULL ELSE i::VARIANT END AS v FROM range(10) t(i)");
	TestArrowRoundtrip("SELECT CASE WHEN i % 3 = 0 THEN NULL WHEN i % 3 = 1 THEN i::VARIANT "
	                   "ELSE ('s' || i::VARCHAR)::VARIANT END AS v FROM range(100) t(i)");
}
