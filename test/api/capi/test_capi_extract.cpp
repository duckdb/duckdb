#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test extract statements in C API", "[capi]") {
	CAPITester tester;
	duckdb_result res;
	duckdb_extracted_statements stmts = nullptr;
	duckdb_state status;
	const char *error;
	duckdb_prepared_statement prepared = nullptr;

	REQUIRE(tester.OpenDatabase(nullptr));

	idx_t size = duckdb_extract_statements(tester.connection,
	                                       "CREATE TABLE tbl (col INT); INSERT INTO tbl VALUES (1), (2), (3), (4); "
	                                       "SELECT COUNT(col) FROM tbl WHERE col > $1",
	                                       &stmts);

	REQUIRE(size == 3);
	REQUIRE(stmts != nullptr);

	for (idx_t i = 0; i + 1 < size; i++) {
		status = duckdb_prepare_extracted_statement(tester.connection, stmts, i, &prepared);
		REQUIRE(status == DuckDBSuccess);
		status = duckdb_execute_prepared(prepared, &res);
		REQUIRE(status == DuckDBSuccess);
		duckdb_destroy_prepare(&prepared);
		duckdb_destroy_result(&res);
	}

	duckdb_prepared_statement stmt = nullptr;
	status = duckdb_prepare_extracted_statement(tester.connection, stmts, size - 1, &stmt);
	REQUIRE(status == DuckDBSuccess);
	duckdb_bind_int32(stmt, 1, 1);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_value_int64(&res, 0, 0) == 3);
	duckdb_destroy_prepare(&stmt);
	duckdb_destroy_result(&res);
	duckdb_destroy_extracted(&stmts);

	//	 test empty statement is not an error
	size = duckdb_extract_statements(tester.connection, "", &stmts);
	REQUIRE(size == 0);
	error = duckdb_extract_statements_error(stmts);
	REQUIRE(error == nullptr);
	duckdb_destroy_extracted(&stmts);

	//	 test incorrect statement cannot be extracted
	size = duckdb_extract_statements(tester.connection, "This is not valid SQL", &stmts);
	REQUIRE(size == 0);
	error = duckdb_extract_statements_error(stmts);
	REQUIRE(error != nullptr);
	duckdb_destroy_extracted(&stmts);

	// test out of bounds
	size = duckdb_extract_statements(tester.connection, "SELECT CAST($1 AS BIGINT)", &stmts);
	REQUIRE(size == 1);
	status = duckdb_prepare_extracted_statement(tester.connection, stmts, 2, &prepared);
	REQUIRE(status == DuckDBError);
	duckdb_destroy_extracted(&stmts);
}
