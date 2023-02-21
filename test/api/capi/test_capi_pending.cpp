#include "capi_tester.hpp"
#include "duckdb.h"

using namespace duckdb;
using namespace std;

struct CAPIPrepared {
	CAPIPrepared() {
	}
	~CAPIPrepared() {
		if (!prepared) {
			return;
		}
		duckdb_destroy_prepare(&prepared);
	}

	bool Prepare(CAPITester &tester, const string &query) {
		auto state = duckdb_prepare(tester.connection, query.c_str(), &prepared);
		return state == DuckDBSuccess;
	}

	duckdb_prepared_statement prepared = nullptr;
};

struct CAPIPending {
	CAPIPending() {
	}
	~CAPIPending() {
		if (!pending) {
			return;
		}
		duckdb_destroy_pending(&pending);
	}

	bool Pending(CAPIPrepared &prepared) {
		auto state = duckdb_pending_prepared(prepared.prepared, &pending);
		return state == DuckDBSuccess;
	}

	duckdb_pending_state ExecuteTask() {
		REQUIRE(pending);
		return duckdb_pending_execute_task(pending);
	}

	duckdb::unique_ptr<CAPIResult> Execute() {
		duckdb_result result;
		auto success = duckdb_execute_pending(pending, &result) == DuckDBSuccess;
		return make_uniq<CAPIResult>(result, success);
	}

	duckdb_pending_result pending = nullptr;
};

TEST_CASE("Test pending statements in C API", "[capi]") {
	CAPITester tester;
	CAPIPrepared prepared;
	CAPIPending pending;
	duckdb::unique_ptr<CAPIResult> result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE(prepared.Prepare(tester, "SELECT SUM(i) FROM range(1000000) tbl(i)"));
	REQUIRE(pending.Pending(prepared));

	while (true) {
		auto state = pending.ExecuteTask();
		REQUIRE(state != DUCKDB_PENDING_ERROR);
		if (state == DUCKDB_PENDING_RESULT_READY) {
			break;
		}
	}

	result = pending.Execute();
	REQUIRE(result);
	REQUIRE(!result->HasError());
	REQUIRE(result->Fetch<int64_t>(0, 0) == 499999500000LL);
}
