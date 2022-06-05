#include "catch.hpp"
#include "test_helpers.hpp"

#include <thread>

using namespace duckdb;
using namespace std;

TEST_CASE("Test Pending Query API", "[api]") {
	DuckDB db;
	Connection con(db);

	SECTION("Materialized result") {
		auto pending_query = con.PendingQuery("SELECT SUM(i) FROM range(1000000) tbl(i)");
		REQUIRE(pending_query->success);
		auto result = pending_query->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(499999500000)}));

		// cannot fetch twice from the same pending query
		REQUIRE_THROWS(pending_query->Execute());
		REQUIRE_THROWS(pending_query->Execute());

		// query the connection as normal after
		result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
	SECTION("Streaming result") {
		auto pending_query = con.PendingQuery("SELECT SUM(i) FROM range(1000000) tbl(i)", true);
		REQUIRE(pending_query->success);
		auto result = pending_query->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(499999500000)}));

		// cannot fetch twice from the same pending query
		REQUIRE_THROWS(pending_query->Execute());
		REQUIRE_THROWS(pending_query->Execute());

		// query the connection as normal after
		result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
	SECTION("Execute tasks") {
		auto pending_query = con.PendingQuery("SELECT SUM(i) FROM range(1000000) tbl(i)", true);
		while (pending_query->ExecuteTask() == PendingExecutionResult::RESULT_NOT_READY)
			;
		REQUIRE(pending_query->success);
		auto result = pending_query->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(499999500000)}));

		// cannot fetch twice from the same pending query
		REQUIRE_THROWS(pending_query->Execute());

		// query the connection as normal after
		result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
	SECTION("Create pending query while another pending query exists") {
		auto pending_query = con.PendingQuery("SELECT SUM(i) FROM range(1000000) tbl(i)");
		auto pending_query2 = con.PendingQuery("SELECT SUM(i) FROM range(1000000) tbl(i)", true);

		// first pending query is now closed
		REQUIRE_THROWS(pending_query->ExecuteTask());
		REQUIRE_THROWS(pending_query->Execute());

		// we can execute the second one
		auto result = pending_query2->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(499999500000)}));

		// query the connection as normal after
		result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
	SECTION("Binding error in pending query") {
		auto pending_query = con.PendingQuery("SELECT XXXSUM(i) FROM range(1000000) tbl(i)");
		REQUIRE(!pending_query->success);
		REQUIRE_THROWS(pending_query->ExecuteTask());
		REQUIRE_THROWS(pending_query->Execute());

		// query the connection as normal after
		auto result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
	SECTION("Runtime error in pending query (materialized)") {
		// this succeeds initially
		auto pending_query =
		    con.PendingQuery("SELECT concat(SUM(i)::varchar, 'hello')::INT FROM range(1000000) tbl(i)");
		REQUIRE(pending_query->success);
		// we only encounter the failure later on as we are executing the query
		auto result = pending_query->Execute();
		REQUIRE_FAIL(result);

		// query the connection as normal after
		result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
	SECTION("Runtime error in pending query (streaming)") {
		// this succeeds initially
		auto pending_query =
		    con.PendingQuery("SELECT concat(SUM(i)::varchar, 'hello')::INT FROM range(1000000) tbl(i)", true);
		REQUIRE(pending_query->success);
		// still succeeds...
		auto result = pending_query->Execute();
		REQUIRE(result->success);
		auto chunk = result->Fetch();
		REQUIRE(!chunk);
		REQUIRE(!result->success);

		// query the connection as normal after
		result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
}

static void parallel_pending_query(Connection *conn, bool *correct, size_t threadnr) {
	correct[threadnr] = true;
	for (size_t i = 0; i < 100; i++) {
		// run pending query and then execute it
		auto executor = conn->PendingQuery("SELECT * FROM integers ORDER BY i");
		try {
			// this will randomly throw an exception if another thread calls pending query first
			auto result = executor->Execute();
			if (!CHECK_COLUMN(result, 0, {Value(), 1, 2, 3})) {
				correct[threadnr] = false;
			}
		} catch (...) {
			continue;
		}
	}
}

TEST_CASE("Test parallel usage of pending query API", "[api][.]") {
	auto db = make_unique<DuckDB>(nullptr);
	auto conn = make_unique<Connection>(*db);

	REQUIRE_NO_FAIL(conn->Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(conn->Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	bool correct[20];
	thread threads[20];
	for (size_t i = 0; i < 20; i++) {
		threads[i] = thread(parallel_pending_query, conn.get(), correct, i);
	}
	for (size_t i = 0; i < 20; i++) {
		threads[i].join();
		REQUIRE(correct[i]);
	}
}

TEST_CASE("Test Pending Query Prepared Statements API", "[api]") {
	DuckDB db;
	Connection con(db);

	SECTION("Standard prepared") {
		auto prepare = con.Prepare("SELECT SUM(i) FROM range(1000000) tbl(i) WHERE i>=$1");
		REQUIRE(prepare->success);

		auto pending_query = prepare->PendingQuery(0);
		REQUIRE(pending_query->success);

		auto result = pending_query->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(499999500000)}));

		// cannot fetch twice from the same pending query
		REQUIRE_THROWS(pending_query->Execute());
		REQUIRE_THROWS(pending_query->Execute());

		// we can use the prepared query again, however
		pending_query = prepare->PendingQuery(500000);
		REQUIRE(pending_query->success);

		result = pending_query->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(374999750000)}));

		// cannot fetch twice from the same pending query
		REQUIRE_THROWS(pending_query->Execute());
		REQUIRE_THROWS(pending_query->Execute());
	}
	SECTION("Error during prepare") {
		auto prepare = con.Prepare("SELECT SUM(i+X) FROM range(1000000) tbl(i) WHERE i>=$1");
		REQUIRE(!prepare->success);

		REQUIRE_THROWS(prepare->PendingQuery(0));
	}
	SECTION("Error during execution") {
		vector<Value> parameters;
		auto prepared = con.Prepare("SELECT concat(SUM(i)::varchar, CASE WHEN SUM(i) IS NULL THEN 0 ELSE 'hello' "
		                            "END)::INT FROM range(1000000) tbl(i) WHERE i>$1");
		// this succeeds initially
		parameters = {Value::INTEGER(0)};
		auto pending_query = prepared->PendingQuery(parameters, true);
		REQUIRE(pending_query->success);
		// still succeeds...
		auto result = pending_query->Execute();
		REQUIRE(result->success);
		//! fail!
		auto chunk = result->Fetch();
		REQUIRE(!chunk);
		REQUIRE(!result->success);

		// query the connection as normal after
		result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));

		// if we change the parameter this works
		parameters = {Value::INTEGER(2000000)};
		pending_query = prepared->PendingQuery(parameters, true);
		REQUIRE(pending_query->success);
		// still succeeds...
		result = pending_query->Execute();
		REQUIRE(result->success);
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(0)}));
	}
	SECTION("Multiple prepared statements") {
		auto prepare1 = con.Prepare("SELECT SUM(i) FROM range(1000000) tbl(i) WHERE i>=$1");
		auto prepare2 = con.Prepare("SELECT SUM(i) FROM range(1000000) tbl(i) WHERE i<=$1");
		REQUIRE(prepare1->success);
		REQUIRE(prepare2->success);

		// we can execute from both prepared statements individually
		auto pending_query = prepare1->PendingQuery(500000);
		REQUIRE(pending_query->success);

		auto result = pending_query->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(374999750000)}));

		pending_query = prepare2->PendingQuery(500000);
		REQUIRE(pending_query->success);

		result = pending_query->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(125000250000)}));

		// we can overwrite pending queries all day long
		for (idx_t i = 0; i < 10; i++) {
			pending_query = prepare1->PendingQuery(500000);
			pending_query = prepare2->PendingQuery(500000);
		}

		result = pending_query->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(125000250000)}));

		// however, we can't mix and match...
		pending_query = prepare1->PendingQuery(500000);
		auto pending_query2 = prepare2->PendingQuery(500000);

		// this result is no longer open
		REQUIRE_THROWS(pending_query->Execute());
	}
}
