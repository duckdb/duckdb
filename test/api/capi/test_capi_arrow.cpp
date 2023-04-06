#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test arrow in C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	duckdb_prepared_statement stmt = nullptr;
	duckdb_arrow arrow_result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	// test rows changed
	{
		REQUIRE_NO_FAIL(tester.Query("CREATE TABLE test(a INTEGER)"));
		REQUIRE(duckdb_query_arrow(tester.connection, "INSERT INTO test VALUES (1), (2);", &arrow_result) ==
		        DuckDBSuccess);
		REQUIRE(duckdb_arrow_rows_changed(arrow_result) == 2);
		duckdb_destroy_arrow(&arrow_result);
		REQUIRE_NO_FAIL(tester.Query("drop table test"));
	}

	// test query arrow
	{
		REQUIRE(duckdb_query_arrow(tester.connection, "SELECT 42 AS VALUE", &arrow_result) == DuckDBSuccess);
		REQUIRE(duckdb_arrow_row_count(arrow_result) == 1);
		REQUIRE(duckdb_arrow_column_count(arrow_result) == 1);
		REQUIRE(duckdb_arrow_rows_changed(arrow_result) == 0);

		// query schema
		ArrowSchema *arrow_schema = new ArrowSchema();
		REQUIRE(duckdb_query_arrow_schema(arrow_result, (duckdb_arrow_schema *)&arrow_schema) == DuckDBSuccess);
		REQUIRE(string(arrow_schema->name) == "duckdb_query_result");
		// User need to release the data themselves
		arrow_schema->release(arrow_schema);
		delete arrow_schema;

		// query array data
		ArrowArray *arrow_array = new ArrowArray();
		REQUIRE(duckdb_query_arrow_array(arrow_result, (duckdb_arrow_array *)&arrow_array) == DuckDBSuccess);
		REQUIRE(arrow_array->length == 1);
		arrow_array->release(arrow_array);
		delete arrow_array;

		duckdb_arrow_array null_array = nullptr;
		REQUIRE(duckdb_query_arrow_array(arrow_result, &null_array) == DuckDBSuccess);
		REQUIRE(null_array == nullptr);

		// destroy result
		duckdb_destroy_arrow(&arrow_result);
	}

	// test multiple chunks
	{
		// create table that consists of multiple chunks
		REQUIRE_NO_FAIL(tester.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(tester.Query("CREATE TABLE test(a INTEGER)"));
		for (size_t i = 0; i < 500; i++) {
			REQUIRE_NO_FAIL(
			    tester.Query("INSERT INTO test VALUES (1); INSERT INTO test VALUES (2); INSERT INTO test VALUES "
			                 "(3); INSERT INTO test VALUES (4); INSERT INTO test VALUES (5);"));
		}
		REQUIRE_NO_FAIL(tester.Query("COMMIT"));

		REQUIRE(duckdb_query_arrow(tester.connection, "SELECT CAST(a AS INTEGER) AS a FROM test ORDER BY a",
		                           &arrow_result) == DuckDBSuccess);

		ArrowSchema *arrow_schema = new ArrowSchema();
		REQUIRE(duckdb_query_arrow_schema(arrow_result, (duckdb_arrow_schema *)&arrow_schema) == DuckDBSuccess);
		REQUIRE(arrow_schema->release != nullptr);
		arrow_schema->release(arrow_schema);
		delete arrow_schema;

		int total_count = 0;
		while (true) {
			ArrowArray *arrow_array = new ArrowArray();
			REQUIRE(duckdb_query_arrow_array(arrow_result, (duckdb_arrow_array *)&arrow_array) == DuckDBSuccess);
			if (arrow_array->length == 0) {
				delete arrow_array;
				REQUIRE(total_count == 2500);
				break;
			}
			REQUIRE(arrow_array->length > 0);
			total_count += arrow_array->length;
			arrow_array->release(arrow_array);
			delete arrow_array;
		}
		duckdb_destroy_arrow(&arrow_result);
		REQUIRE_NO_FAIL(tester.Query("drop table test"));
	}

	// test prepare query arrow
	{
		REQUIRE(duckdb_prepare(tester.connection, "SELECT CAST($1 AS BIGINT)", &stmt) == DuckDBSuccess);
		REQUIRE(stmt != nullptr);
		REQUIRE(duckdb_bind_int64(stmt, 1, 42) == DuckDBSuccess);
		REQUIRE(duckdb_execute_prepared_arrow(stmt, nullptr) == DuckDBError);
		REQUIRE(duckdb_execute_prepared_arrow(stmt, &arrow_result) == DuckDBSuccess);

		ArrowSchema *arrow_schema = new ArrowSchema();
		REQUIRE(duckdb_query_arrow_schema(arrow_result, (duckdb_arrow_schema *)&arrow_schema) == DuckDBSuccess);
		REQUIRE(string(arrow_schema->format) == "+s");
		arrow_schema->release(arrow_schema);
		delete arrow_schema;

		ArrowArray *arrow_array = new ArrowArray();
		REQUIRE(duckdb_query_arrow_array(arrow_result, (duckdb_arrow_array *)&arrow_array) == DuckDBSuccess);
		REQUIRE(arrow_array->length == 1);
		arrow_array->release(arrow_array);
		delete arrow_array;

		duckdb_destroy_arrow(&arrow_result);
		duckdb_destroy_prepare(&stmt);
	}
}
