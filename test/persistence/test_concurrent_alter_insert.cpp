#include "catch.hpp"
#include "duckdb.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <sys/mman.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

using namespace duckdb;
using namespace std;

namespace {

struct ConcurrentAlterInsertState {
	idx_t expected_count;
};

static bool IsConcurrentAlterInsertConflict(const ErrorData &error) {
	return error.Type() == ExceptionType::TRANSACTION || error.Type() == ExceptionType::BINDER ||
	       error.Type() == ExceptionType::CATALOG;
}

static bool RunConcurrentAlterInsertQuery(Connection &connection, const string &query, atomic<bool> &unexpected_error) {
	for (idx_t retry = 0; retry < 10; retry++) {
		auto result = connection.Query(query);
		if (!result->HasError()) {
			return true;
		}
		if (!IsConcurrentAlterInsertConflict(result->GetErrorObject())) {
			unexpected_error = true;
			return false;
		}
	}
	return false;
}

static void RunConcurrentAlterInsertChild(const string &database_path, bool large_wal_autocheckpoint,
                                          ConcurrentAlterInsertState &state) {
	try {
		DuckDB db(database_path);
		Connection setup_connection(db);
		if (large_wal_autocheckpoint) {
			auto result = setup_connection.Query("SET wal_autocheckpoint = '10GB'");
			if (result->HasError()) {
				_exit(2);
			}
		}
		auto result = setup_connection.Query(
		    "CREATE TABLE t AS SELECT range::INTEGER AS int_col, md5(range::VARCHAR)::VARCHAR AS varchar_col "
		    "FROM range(200)");
		if (result->HasError()) {
			_exit(2);
		}

		Connection alter_connection(db);
		Connection insert_connection(db);
		idx_t successful_inserts = 0;
		for (idx_t round = 0; round < 25; round++) {
			atomic<bool> unexpected_error(false);
			bool insert_succeeded = false;
			thread alter_thread([&]() {
				RunConcurrentAlterInsertQuery(
				    alter_connection, "ALTER TABLE t ADD COLUMN IF NOT EXISTS int_col__tmp INTEGER", unexpected_error);
				RunConcurrentAlterInsertQuery(alter_connection, "UPDATE t SET int_col__tmp = int_col",
				                              unexpected_error);
				RunConcurrentAlterInsertQuery(alter_connection, "ALTER TABLE t DROP COLUMN int_col__tmp",
				                              unexpected_error);
			});
			thread insert_thread([&]() {
				insert_succeeded = RunConcurrentAlterInsertQuery(
				    insert_connection,
				    "INSERT INTO t BY NAME SELECT 1::INTEGER AS int_col, 'x' AS varchar_col FROM range(10)",
				    unexpected_error);
			});
			alter_thread.join();
			insert_thread.join();

			if (unexpected_error) {
				_exit(3);
			}
			if (insert_succeeded) {
				successful_inserts++;
			}
		}

		state.expected_count = 200 + successful_inserts * 10;
		result = setup_connection.Query("SELECT count(*) FROM t");
		if (result->HasError() || result->GetValue(0, 0).GetValue<idx_t>() != state.expected_count) {
			_exit(4);
		}

		// Do not run the DuckDB destructor: the parent must recover the database from the WAL.
		_exit(0);
	} catch (...) {
		_exit(5);
	}
}

} // namespace

TEST_CASE("Concurrent ALTER and INSERT survive WAL replay", "[persistence][interquery]") {
	auto large_wal_autocheckpoint = GENERATE(false, true);
	CAPTURE(large_wal_autocheckpoint);
	auto database_path = TestCreatePath(large_wal_autocheckpoint ? "concurrent_alter_insert_large_wal.db"
	                                                             : "concurrent_alter_insert_default_wal.db");
	DeleteDatabase(database_path);

	auto state = static_cast<ConcurrentAlterInsertState *>(
	    mmap(nullptr, sizeof(ConcurrentAlterInsertState), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, 0, 0));
	REQUIRE(state != MAP_FAILED);
	state->expected_count = 0;

	auto pid = fork();
	REQUIRE(pid >= 0);
	if (pid == 0) {
		RunConcurrentAlterInsertChild(database_path, large_wal_autocheckpoint, *state);
	}

	int child_status;
	REQUIRE(waitpid(pid, &child_status, 0) == pid);
	REQUIRE(WIFEXITED(child_status));
	REQUIRE(WEXITSTATUS(child_status) == 0);

	{
		DuckDB db(database_path);
		Connection connection(db);
		auto result = connection.Query("SELECT count(*) FROM t");
		REQUIRE_NO_FAIL(*result);
		CHECK_COLUMN(result, 0, {Value::BIGINT(NumericCast<int64_t>(state->expected_count))});
	}

	REQUIRE(munmap(state, sizeof(ConcurrentAlterInsertState)) == 0);
	DeleteDatabase(database_path);
}
