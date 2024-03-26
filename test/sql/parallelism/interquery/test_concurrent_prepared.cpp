#include "catch.hpp"
#include "test_helpers.hpp"
#include <iostream>
#include <thread>

using namespace duckdb;
using namespace std;

static void SelectTable(Connection con) {
	for (idx_t i = 0; i < 1000; i++) {
		auto prepare = con.Prepare("select * from foo");
		auto result = prepare->Execute();
		if (result->HasError()) {
			FAIL();
		}
	}
}

static void RecreateTable(Connection con) {
	for (idx_t i = 0; i < 1000; i++) {
		auto prepare = con.Prepare("create or replace table foo as select * from foo");
		auto result = prepare->Execute();
		if (result->HasError()) {
			FAIL();
		}
	}
}

TEST_CASE("Test concurrent prepared", "[api][.]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("create table foo as select unnest(generate_series(1, 10));"));

	Connection select_conn(db);
	Connection recreate_conn(db);
	select_conn.EnableQueryVerification();

	std::thread select_function(SelectTable, std::move(select_conn));
	std::thread recreate_function(RecreateTable, std::move(recreate_conn));

	select_function.join();
	recreate_function.join();
}
