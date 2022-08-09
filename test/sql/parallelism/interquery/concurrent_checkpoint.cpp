#include "catch.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/appender.hpp"

#include <atomic>
#include <random>
#include <thread>

using namespace duckdb;
using namespace std;

class ConcurrentCheckpoint {
public:
	static constexpr int CONCURRENT_UPDATE_TRANSACTION_UPDATE_COUNT = 200;
	static constexpr int CONCURRENT_UPDATE_TOTAL_ACCOUNTS = 10;
	static constexpr int CONCURRENT_UPDATE_MONEY_PER_ACCOUNT = 10;

	static atomic<bool> finished;
	static atomic<size_t> finished_threads;

	template <bool FORCE_CHECKPOINT>
	static void CheckpointThread(DuckDB *db, bool *read_correct) {
		Connection con(*db);
		while (!finished) {
			{
				// the total balance should remain constant regardless of updates and checkpoints
				auto result = con.Query("SELECT SUM(money) FROM accounts");
				if (!CHECK_COLUMN(result, 0,
				                  {CONCURRENT_UPDATE_TOTAL_ACCOUNTS * CONCURRENT_UPDATE_MONEY_PER_ACCOUNT})) {
					*read_correct = false;
				}
			}
			while (true) {
				auto result = con.Query(FORCE_CHECKPOINT ? "FORCE CHECKPOINT" : "CHECKPOINT");
				if (result->success) {
					break;
				}
			}
			{
				// the total balance should remain constant regardless of updates and checkpoints
				auto result = con.Query("SELECT SUM(money) FROM accounts");
				if (!CHECK_COLUMN(result, 0,
				                  {CONCURRENT_UPDATE_TOTAL_ACCOUNTS * CONCURRENT_UPDATE_MONEY_PER_ACCOUNT})) {
					*read_correct = false;
				}
			}
		}
	}

	static void WriteRandomNumbers(DuckDB *db, bool *correct, size_t nr) {
		correct[nr] = true;
		Connection con(*db);
		for (size_t i = 0; i < CONCURRENT_UPDATE_TRANSACTION_UPDATE_COUNT; i++) {
			// just make some changes to the total
			// the total amount of money after the commit is the same
			if (!con.Query("BEGIN TRANSACTION")->success) {
				correct[nr] = false;
			}
			if (!con.Query("UPDATE accounts SET money = money + " + to_string(i * 2) + " WHERE id = " + to_string(nr))
			         ->success) {
				correct[nr] = false;
			}
			if (!con.Query("UPDATE accounts SET money = money - " + to_string(i) + " WHERE id = " + to_string(nr))
			         ->success) {
				correct[nr] = false;
			}
			if (!con.Query("UPDATE accounts SET money = money - " + to_string(i * 2) + " WHERE id = " + to_string(nr))
			         ->success) {
				correct[nr] = false;
			}
			if (!con.Query("UPDATE accounts SET money = money + " + to_string(i) + " WHERE id = " + to_string(nr))
			         ->success) {
				correct[nr] = false;
			}
			// we test both commit and rollback
			// the result of both should be the same since the updates have a
			// net-zero effect
			if (!con.Query(nr % 2 == 0 ? "COMMIT" : "ROLLBACK")->success) {
				correct[nr] = false;
			}
		}
		finished_threads++;
		if (finished_threads == CONCURRENT_UPDATE_TOTAL_ACCOUNTS) {
			finished = true;
		}
	}

	static void NopUpdate(DuckDB *db) {
		Connection con(*db);
		for (size_t i = 0; i < 10; i++) {
			con.Query("BEGIN TRANSACTION");
			con.Query("UPDATE accounts SET money = money");
			con.Query("COMMIT");
		}
		finished_threads++;
		if (finished_threads == CONCURRENT_UPDATE_TOTAL_ACCOUNTS) {
			finished = true;
		}
	}
};

atomic<bool> ConcurrentCheckpoint::finished;
atomic<size_t> ConcurrentCheckpoint::finished_threads;

TEST_CASE("Concurrent checkpoint with single updater", "[interquery][.]") {
	auto config = GetTestConfig();
	auto storage_database = TestCreatePath("concurrent_checkpoint");
	DeleteDatabase(storage_database);
	unique_ptr<MaterializedQueryResult> result;
	DuckDB db(storage_database, config.get());
	Connection con(db);

	// fixed seed random numbers
	mt19937 generator;
	generator.seed(42);
	uniform_int_distribution<int> account_distribution(0, ConcurrentCheckpoint::CONCURRENT_UPDATE_TOTAL_ACCOUNTS - 1);
	auto random_account = bind(account_distribution, generator);

	uniform_int_distribution<int> amount_distribution(0, ConcurrentCheckpoint::CONCURRENT_UPDATE_MONEY_PER_ACCOUNT);
	auto random_amount = bind(amount_distribution, generator);

	ConcurrentCheckpoint::finished = false;

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	// initialize the database
	con.Query("BEGIN TRANSACTION");
	con.Query("CREATE TABLE accounts(id INTEGER, money INTEGER)");
	for (size_t i = 0; i < ConcurrentCheckpoint::CONCURRENT_UPDATE_TOTAL_ACCOUNTS; i++) {
		con.Query("INSERT INTO accounts VALUES (" + to_string(i) + ", " +
		          to_string(ConcurrentCheckpoint::CONCURRENT_UPDATE_MONEY_PER_ACCOUNT) + ");");
	}
	con.Query("COMMIT");

	bool read_correct = true;
	// launch separate thread for reading aggregate
	thread read_thread(ConcurrentCheckpoint::CheckpointThread<false>, &db, &read_correct);

	// start vigorously updating balances in this thread
	for (size_t i = 0; i < ConcurrentCheckpoint::CONCURRENT_UPDATE_TRANSACTION_UPDATE_COUNT; i++) {
		int from = random_account();
		int to = random_account();
		while (to == from) {
			to = random_account();
		}
		int amount = random_amount();

		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		result = con.Query("SELECT money FROM accounts WHERE id=" + to_string(from));
		Value money_from = result->GetValue(0, 0);
		result = con.Query("SELECT money FROM accounts WHERE id=" + to_string(to));
		Value money_to = result->GetValue(0, 0);

		REQUIRE_NO_FAIL(
		    con.Query("UPDATE accounts SET money = money - " + to_string(amount) + " WHERE id = " + to_string(from)));
		REQUIRE_NO_FAIL(
		    con.Query("UPDATE accounts SET money = money + " + to_string(amount) + " WHERE id = " + to_string(to)));

		result = con.Query("SELECT money FROM accounts WHERE id=" + to_string(from));
		Value new_money_from = result->GetValue(0, 0);
		result = con.Query("SELECT money FROM accounts WHERE id=" + to_string(to));
		Value new_money_to = result->GetValue(0, 0);

		Value expected_money_from, expected_money_to;

		expected_money_from = Value::INTEGER(IntegerValue::Get(money_from) - amount);
		expected_money_to = Value::INTEGER(IntegerValue::Get(money_to) + amount);

		REQUIRE(new_money_from == expected_money_from);
		REQUIRE(new_money_to == expected_money_to);

		REQUIRE_NO_FAIL(con.Query("COMMIT"));
	}
	ConcurrentCheckpoint::finished = true;
	read_thread.join();
	REQUIRE(read_correct);
}

TEST_CASE("Concurrent checkpoint with multiple updaters", "[interquery][.]") {
	auto config = GetTestConfig();
	auto storage_database = TestCreatePath("concurrent_checkpoint");
	DeleteDatabase(storage_database);
	unique_ptr<MaterializedQueryResult> result;
	DuckDB db(storage_database, config.get());
	Connection con(db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	ConcurrentCheckpoint::finished = false;
	ConcurrentCheckpoint::finished_threads = 0;
	// initialize the database
	con.Query("BEGIN TRANSACTION");
	con.Query("CREATE TABLE accounts(id INTEGER, money INTEGER)");
	for (size_t i = 0; i < ConcurrentCheckpoint::CONCURRENT_UPDATE_TOTAL_ACCOUNTS; i++) {
		con.Query("INSERT INTO accounts VALUES (" + to_string(i) + ", " +
		          to_string(ConcurrentCheckpoint::CONCURRENT_UPDATE_MONEY_PER_ACCOUNT) + ");");
	}
	con.Query("COMMIT");

	bool correct[ConcurrentCheckpoint::CONCURRENT_UPDATE_TOTAL_ACCOUNTS];
	bool read_correct;
	std::thread write_threads[ConcurrentCheckpoint::CONCURRENT_UPDATE_TOTAL_ACCOUNTS];
	// launch a thread for reading and checkpointing the table
	thread read_thread(ConcurrentCheckpoint::CheckpointThread<false>, &db, &read_correct);
	// launch several threads for updating the table
	for (size_t i = 0; i < ConcurrentCheckpoint::CONCURRENT_UPDATE_TOTAL_ACCOUNTS; i++) {
		write_threads[i] = thread(ConcurrentCheckpoint::WriteRandomNumbers, &db, correct, i);
	}
	read_thread.join();
	for (size_t i = 0; i < ConcurrentCheckpoint::CONCURRENT_UPDATE_TOTAL_ACCOUNTS; i++) {
		write_threads[i].join();
		REQUIRE(correct[i]);
	}
}

TEST_CASE("Force concurrent checkpoint with single updater", "[interquery][.]") {
	auto config = GetTestConfig();
	auto storage_database = TestCreatePath("concurrent_checkpoint");
	DeleteDatabase(storage_database);
	unique_ptr<MaterializedQueryResult> result;
	DuckDB db(storage_database, config.get());
	Connection con(db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	ConcurrentCheckpoint::finished = false;
	// initialize the database
	con.Query("BEGIN TRANSACTION");
	con.Query("CREATE TABLE accounts(id INTEGER, money INTEGER)");
	for (size_t i = 0; i < ConcurrentCheckpoint::CONCURRENT_UPDATE_TOTAL_ACCOUNTS; i++) {
		con.Query("INSERT INTO accounts VALUES (" + to_string(i) + ", " +
		          to_string(ConcurrentCheckpoint::CONCURRENT_UPDATE_MONEY_PER_ACCOUNT) + ");");
	}
	con.Query("COMMIT");

	bool read_correct = true;
	// launch separate thread for reading aggregate
	thread read_thread(ConcurrentCheckpoint::CheckpointThread<true>, &db, &read_correct);

	// start vigorously updating balances in this thread
	for (size_t i = 0; i < ConcurrentCheckpoint::CONCURRENT_UPDATE_TRANSACTION_UPDATE_COUNT; i++) {
		con.Query("BEGIN TRANSACTION");
		con.Query("UPDATE accounts SET money = money");
		con.Query("UPDATE accounts SET money = money");
		con.Query("UPDATE accounts SET money = money");
		con.Query("ROLLBACK");
	}
	ConcurrentCheckpoint::finished = true;
	read_thread.join();
	REQUIRE(read_correct);
}

TEST_CASE("Concurrent commits on persistent database with automatic checkpoints", "[interquery][.]") {
	auto config = GetTestConfig();
	auto storage_database = TestCreatePath("concurrent_checkpoint");
	DeleteDatabase(storage_database);
	unique_ptr<MaterializedQueryResult> result;
	config->options.checkpoint_wal_size = 1;
	DuckDB db(storage_database, config.get());
	Connection con(db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	const int ACCOUNTS = 20000;
	ConcurrentCheckpoint::finished = false;
	ConcurrentCheckpoint::finished_threads = 0;
	// initialize the database
	con.Query("BEGIN TRANSACTION");
	con.Query("CREATE TABLE accounts(id INTEGER, money INTEGER)");
	Appender appender(con, "accounts");
	for (size_t i = 0; i < ACCOUNTS; i++) {
		appender.AppendRow(int(i), int(ConcurrentCheckpoint::CONCURRENT_UPDATE_MONEY_PER_ACCOUNT));
	}
	appender.Close();
	con.Query("COMMIT");

	REQUIRE_NO_FAIL(con.Query("UPDATE accounts SET money = money"));
	REQUIRE_NO_FAIL(con.Query("UPDATE accounts SET money = money"));
	REQUIRE_NO_FAIL(con.Query("UPDATE accounts SET money = money"));
	result = con.Query("SELECT SUM(money) FROM accounts");
	REQUIRE(CHECK_COLUMN(result, 0, {ACCOUNTS * ConcurrentCheckpoint::CONCURRENT_UPDATE_MONEY_PER_ACCOUNT}));

	std::thread write_threads[ConcurrentCheckpoint::CONCURRENT_UPDATE_TOTAL_ACCOUNTS];
	// launch several threads for updating the table
	for (size_t i = 0; i < ConcurrentCheckpoint::CONCURRENT_UPDATE_TOTAL_ACCOUNTS; i++) {
		write_threads[i] = thread(ConcurrentCheckpoint::NopUpdate, &db);
	}
	for (size_t i = 0; i < ConcurrentCheckpoint::CONCURRENT_UPDATE_TOTAL_ACCOUNTS; i++) {
		write_threads[i].join();
	}
	result = con.Query("SELECT SUM(money) FROM accounts");
	REQUIRE(CHECK_COLUMN(result, 0, {ACCOUNTS * ConcurrentCheckpoint::CONCURRENT_UPDATE_MONEY_PER_ACCOUNT}));
	REQUIRE_NO_FAIL(con.Query("UPDATE accounts SET money = money"));
	result = con.Query("SELECT SUM(money) FROM accounts");
	REQUIRE(CHECK_COLUMN(result, 0, {ACCOUNTS * ConcurrentCheckpoint::CONCURRENT_UPDATE_MONEY_PER_ACCOUNT}));
}
