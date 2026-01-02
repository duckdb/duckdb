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

void WorkerTask(DuckDB *db, int worker_id, std::atomic<int> *success_count, std::atomic<bool> *fatal_error_occurred) {
	Connection con(*db);
	std::mt19937 generator(worker_id); // Different seed per worker
	std::uniform_int_distribution<int> val_distribution(0, 99999);

	for (int id = 0; id < STRESS_TEST_NUM_ROWS; id++) {
		int val = val_distribution(generator);

		// UPDATE the row
		auto update_result = con.Query("UPDATE test_stress_update_issue_19688 SET val=$1 WHERE id = $2", val, id);
		if (update_result->HasError()) {
			if (update_result->GetErrorType() == ExceptionType::FATAL) {
				fatal_error_occurred->store(true);
			}
			return;
		}
		(*success_count)++;

		// DELETE the row
		auto delete_result = con.Query("DELETE FROM test_stress_update_issue_19688 WHERE id = $1", id);
		if (delete_result->HasError()) {
			if (delete_result->GetErrorType() == ExceptionType::FATAL) {
				fatal_error_occurred->store(true);
			}
			return;
		}
		(*success_count)++;

		// INSERT it back
		auto insert_result = con.Query("INSERT INTO test_stress_update_issue_19688 VALUES ($1, $2)", id, val);
		if (insert_result->HasError()) {
			if (insert_result->GetErrorType() == ExceptionType::FATAL) {
				fatal_error_occurred->store(true);
			}
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
			val INTEGER
		);
	)"));

	// Seed 1000 rows and verify the initial count.
	for (int idx = 0; idx < STRESS_TEST_NUM_ROWS; idx++) {
		REQUIRE_NO_FAIL(
		    con.Query("INSERT INTO test_stress_update_issue_19688 (id, val) VALUES ($1, $2)", idx, idx * 1000));
	}
	auto count_result = con.Query("SELECT COUNT(*) FROM test_stress_update_issue_19688");
	REQUIRE(CHECK_COLUMN(count_result, 0, {STRESS_TEST_NUM_ROWS}));

	// Launch concurrent workers to perform UPDATE/DELETE/INSERT operations on the same rows, and check for fatal
	// errors and successful operations.
	vector<std::thread> workers;
	std::atomic<int> total_success(0);
	std::atomic<bool> fatal_error_occurred(false);
	for (int w = 0; w < STRESS_TEST_NUM_WORKERS; w++) {
		workers.emplace_back(WorkerTask, &db, w, &total_success, &fatal_error_occurred);
	}
	for (auto &worker : workers) {
		worker.join();
	}

	// Make sure some successful operations were performed.
	// For concurrent updates on the same rows, it's acceptable and normal to have transient errors (i.e., conflict
	// updates), but we should not have any fatal errors (i.e., invalid database state).
	REQUIRE(!fatal_error_occurred);
	REQUIRE(total_success > 0);
}

} // namespace duckdb
