#include "capi_tester.hpp"
#include "duckdb.h"

using namespace duckdb;

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
		if (duckdb_pending_execution_is_finished(state)) {
			break;
		}
	}

	result = pending.Execute();
	REQUIRE(result);
	REQUIRE(!result->HasError());
	REQUIRE(result->Fetch<int64_t>(0, 0) == 499999500000LL);
}

TEST_CASE("Test polling a pending statement that the workers already finished", "[capi]") {
	// A statement whose store is retained at submission finishes without the consumer stepping it.
	// Polling it then reports the result as ready, not as an error
	CAPITester tester;
	CAPIPrepared prepared;
	CAPIPending pending;

	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE(prepared.Prepare(tester, "CREATE TABLE polled AS SELECT i FROM range(10) t(i)"));
	REQUIRE(pending.Pending(prepared));

	// Bounded: a state that never becomes ready fails the test instead of hanging the suite
	duckdb_pending_state state = DUCKDB_PENDING_RESULT_NOT_READY;
	for (idx_t i = 0; i < 1000000 && state != DUCKDB_PENDING_RESULT_READY; i++) {
		state = pending.CheckState();
		REQUIRE(state != DUCKDB_PENDING_ERROR);
	}
	REQUIRE(state == DUCKDB_PENDING_RESULT_READY);

	auto result = pending.Execute();
	REQUIRE(result);
	REQUIRE(!result->HasError());

	result = tester.Query("SELECT count(*) FROM polled");
	REQUIRE(result->Fetch<int64_t>(0, 0) == 10);
}
