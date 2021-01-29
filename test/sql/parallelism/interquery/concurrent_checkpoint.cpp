#include "catch.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <random>
#include <thread>

using namespace duckdb;
using namespace std;

class ConcurrentCheckpoint {
public:
	static constexpr int CONCURRENT_UPDATE_TRANSACTION_UPDATE_COUNT = 1000;
	static constexpr int CONCURRENT_UPDATE_TOTAL_ACCOUNTS = 20;
	static constexpr int CONCURRENT_UPDATE_MONEY_PER_ACCOUNT = 10;

	static atomic<bool> finished;

	static void CheckpointThread(DuckDB *db, bool *read_correct) {
		Connection con(*db);
		while (!finished) {
			while(true) {
				auto result = con.Query("CHECKPOINT");
				if (result->success) {
					break;
				}
			}
			{
				// the total balance should remain constant regardless of updates and checkpoints
				auto result = con.Query("SELECT SUM(money) FROM accounts");
				if (!CHECK_COLUMN(result, 0, {CONCURRENT_UPDATE_TOTAL_ACCOUNTS * CONCURRENT_UPDATE_MONEY_PER_ACCOUNT})) {
					*read_correct = false;
				}
			}
		}
	}
};

atomic<bool> ConcurrentCheckpoint::finished;

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
	// initialize the database
	con.Query("CREATE TABLE accounts(id INTEGER, money INTEGER)");
	for (size_t i = 0; i < ConcurrentCheckpoint::CONCURRENT_UPDATE_TOTAL_ACCOUNTS; i++) {
		con.Query("INSERT INTO accounts VALUES (" + to_string(i) + ", " + to_string(ConcurrentCheckpoint::CONCURRENT_UPDATE_MONEY_PER_ACCOUNT) + ");");
	}

	bool read_correct = true;
	// launch separate thread for reading aggregate
	thread read_thread(ConcurrentCheckpoint::CheckpointThread, &db, &read_correct);

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
		Value money_from = result->collection.GetValue(0, 0);
		result = con.Query("SELECT money FROM accounts WHERE id=" + to_string(to));
		Value money_to = result->collection.GetValue(0, 0);

		REQUIRE_NO_FAIL(
		    con.Query("UPDATE accounts SET money = money - " + to_string(amount) + " WHERE id = " + to_string(from)));
		REQUIRE_NO_FAIL(
		    con.Query("UPDATE accounts SET money = money + " + to_string(amount) + " WHERE id = " + to_string(to)));

		result = con.Query("SELECT money FROM accounts WHERE id=" + to_string(from));
		Value new_money_from = result->collection.GetValue(0, 0);
		result = con.Query("SELECT money FROM accounts WHERE id=" + to_string(to));
		Value new_money_to = result->collection.GetValue(0, 0);

		Value expected_money_from, expected_money_to;

		expected_money_from = money_from - amount;
		expected_money_to = money_to + amount;

		REQUIRE(new_money_from == expected_money_from);
		REQUIRE(new_money_to == expected_money_to);

		REQUIRE_NO_FAIL(con.Query("COMMIT"));
	}
	ConcurrentCheckpoint::finished = true;
	read_thread.join();
	REQUIRE(read_correct);
}

