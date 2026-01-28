#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"
using namespace duckdb;
using namespace std;

TEST_CASE("Test union all with allow_stream_result", "[storage][.]") {
	DBConfig config;
	config.options.maximum_memory = 104857600;
	config.options.maximum_threads = 4;

	duckdb::unique_ptr<DuckDB> database;
	duckdb::unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("union_all_with_allow_stream");

	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, &config);
		Connection con(db);

		REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='1KB';"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(id bigint, a string);"));

		REQUIRE_NO_FAIL(con.Query(
		    "INSERT INTO test SELECT row_number() over () AS z, gen_random_uuid() id FROM generate_series(0, 160000)"));
		REQUIRE_NO_FAIL(con.Query("create table test_2 as from test;"));

		REQUIRE_NO_FAIL(con.Query("SET GLOBAL memory_limit='128MB';"));
		auto res = con.context->Query("SELECT distinct(test.a) as b FROM test left join test_2 on test.a = test_2.a "
		                              "UNION ALL SELECT a FROM test UNION ALL SELECT distinct(a) as b FROM test",
		                              /* allow_stream_result = */ true);
		REQUIRE_NO_FAIL(*res);
	}

	DeleteDatabase(storage_database);
}
