#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"

using namespace duckdb;

static void TestArrowRoundtrip(const string &query, bool export_large_buffer = false) {
	DuckDB db;
	Connection con(db);
	if (export_large_buffer) {
		auto res = con.Query("SET arrow_large_buffer_size=True");
		REQUIRE(!res->HasError());
	}
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, query, true));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, query, false));
}

static void TestParquetRoundtrip(const string &path) {
	DuckDB db;
	Connection con(db);

	if (ExtensionHelper::LoadExtension(db, "parquet") == ExtensionLoadResult::NOT_LOADED) {
		FAIL();
		return;
	}

	// run the query
	auto query = "SELECT * FROM parquet_scan('" + path + "')";
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, query, true));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, query));
}

TEST_CASE("Test Export Large", "[arrow]") {
	// Test with Regular Buffer Size
	TestArrowRoundtrip("SELECT 'bla' FROM range(10000)");

	TestArrowRoundtrip("SELECT 'bla'::BLOB FROM range(10000)");

	TestArrowRoundtrip("SELECT '3d038406-6275-4aae-bec1-1235ccdeaade'::UUID FROM range(10000) tbl(i)");

	// Test with Large Buffer Size
	TestArrowRoundtrip("SELECT 'bla' FROM range(10000)", true);

	TestArrowRoundtrip("SELECT 'bla'::BLOB FROM range(10000)", true);

	TestArrowRoundtrip("SELECT '3d038406-6275-4aae-bec1-1235ccdeaade'::UUID FROM range(10000) tbl(i)", true);
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
	TestArrowRoundtrip("SELECT * EXCLUDE(bit,time_tz) REPLACE "
	                   "(interval (1) seconds AS interval, hugeint::DOUBLE as hugeint) FROM test_all_types()");
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
