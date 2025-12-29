#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/exception.hpp"

#include <atomic>
#include <random>
#include <thread>

namespace duckdb {

constexpr int STRESS_TEST_NUM_WORKERS = 8;
constexpr int STRESS_TEST_NUM_ROWS = 1000;

// Simple JSON payload for testing
constexpr const char *JSON_PAYLOAD = R"({"name":"Test","id":1,"version":1.0})";

void WorkerTask(DuckDB *db, int worker_id, atomic<int> *success_count) {
	Connection con(*db);
	std::mt19937 generator(worker_id); // Different seed per worker
	std::uniform_int_distribution<int> val_distribution(0, 99999);

	for (int id = 0; id < STRESS_TEST_NUM_ROWS; id++) {
		int val = val_distribution(generator);

		// UPDATE the row
		auto update_result =
		    con.Query("UPDATE test_stress_update_issue_19688 SET val=$1, payload=$2 WHERE id = $3", val, JSON_PAYLOAD, id);
		if (update_result->HasError()) {
			REQUIRE(update_result->GetErrorType() != ExceptionType::FATAL);
			return;
		}
		(*success_count)++;

		// DELETE the row
		auto delete_result = con.Query("DELETE FROM test_stress_update_issue_19688 WHERE id = $1", id);
		if (delete_result->HasError()) {
			REQUIRE(delete_result->GetErrorType() != ExceptionType::FATAL);
			return;
		}
		(*success_count)++;

		// INSERT it back
		auto insert_result = con.Query("INSERT INTO test_stress_update_issue_19688 VALUES ($1, $2, $3)", id, val, JSON_PAYLOAD);
		if (insert_result->HasError()) {
			REQUIRE(insert_result->GetErrorType() != ExceptionType::FATAL);
			return;
		}
		(*success_count)++;
	}
}

TEST_CASE("Concurrent stress test: UPDATE/DELETE/INSERT", "[api][concurrent]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Create the test table
	REQUIRE_NO_FAIL(con.Query("DROP TABLE IF EXISTS test_stress_update_issue_19688"));
	REQUIRE_NO_FAIL(con.Query(R"(
		CREATE TABLE test_stress_update_issue_19688 (
			id INTEGER,
			val INTEGER,
			payload JSON
		);
	)"));

	// Seed 1000 rows and verify the initial count.
	for (int idx = 0; idx < STRESS_TEST_NUM_ROWS; idx++) {
		REQUIRE_NO_FAIL(
		    con.Query("INSERT INTO test_stress_update_issue_19688 (id, val, payload) VALUES ($1, $2, $3)", idx, idx * 1000, JSON_PAYLOAD));
	}
	auto count_result = con.Query("SELECT COUNT(*) FROM test_stress_update_issue_19688");
	REQUIRE(CHECK_COLUMN(count_result, 0, {STRESS_TEST_NUM_ROWS}));

	// Launch concurrent workers to perform UPDATE/DELETE/INSERT operations on the same rows, and check for fatal
	// errors and successful operations.
	vector<std::thread> workers;
	atomic<int> total_success(0);
	for (int w = 0; w < STRESS_TEST_NUM_WORKERS; w++) {
		workers.emplace_back(WorkerTask, &db, w, &total_success);
	}
	for (auto &worker : workers) {
		worker.join();
	}

	// Make sure some successful operations were performed.
	// For concurrent updates on the same rows, it's acceptable and normal to have transient errors (i.e., conflict
	// updates), but we should not have any fatal errors (i.e., invalid database state).
	REQUIRE(total_success > 0);
}

} // namespace duckdb
