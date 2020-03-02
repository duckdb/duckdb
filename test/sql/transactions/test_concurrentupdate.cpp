#include "catch.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <random>
#include <thread>

using namespace duckdb;
using namespace std;

namespace test_concurrent_update {

static constexpr int TRANSACTION_UPDATE_COUNT = 1000;
static constexpr int TOTAL_ACCOUNTS = 20;
static constexpr int MONEY_PER_ACCOUNT = 10;

TEST_CASE("Single thread update", "[transactions]") {
	unique_ptr<MaterializedQueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// initialize the database
	con.Query("CREATE TABLE integers(i INTEGER);");
	int sum = 0;
	for (size_t i = 0; i < TOTAL_ACCOUNTS; i++) {
		for (size_t j = 0; j < 10; j++) {
			con.Query("INSERT INTO integers VALUES (" + to_string(j + 1) + ");");
			sum += j + 1;
		}
	}

	// check the sum
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {sum}));

	// simple update, we should update INSERT_ELEMENTS elements
	result = con.Query("UPDATE integers SET i=4 WHERE i=2");
	REQUIRE(CHECK_COLUMN(result, 0, {TOTAL_ACCOUNTS}));

	// check updated sum
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {sum + 2 * TOTAL_ACCOUNTS}));
}

static volatile bool finished_updating = false;
static void read_total_balance(DuckDB *db, bool *read_correct) {
	*read_correct = true;
	Connection con(*db);
	while (!finished_updating) {
		// the total balance should remain constant regardless of updates
		auto result = con.Query("SELECT SUM(money) FROM accounts");
		if (!CHECK_COLUMN(result, 0, {TOTAL_ACCOUNTS * MONEY_PER_ACCOUNT})) {
			*read_correct = false;
		}
	}
}

TEST_CASE("Concurrent update", "[updates][.]") {
	unique_ptr<MaterializedQueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// fixed seed random numbers
	mt19937 generator;
	generator.seed(42);
	uniform_int_distribution<int> account_distribution(0, TOTAL_ACCOUNTS - 1);
	auto random_account = bind(account_distribution, generator);

	uniform_int_distribution<int> amount_distribution(0, MONEY_PER_ACCOUNT);
	auto random_amount = bind(amount_distribution, generator);

	finished_updating = false;
	// initialize the database
	con.Query("CREATE TABLE accounts(id INTEGER, money INTEGER)");
	for (size_t i = 0; i < TOTAL_ACCOUNTS; i++) {
		con.Query("INSERT INTO accounts VALUES (" + to_string(i) + ", " + to_string(MONEY_PER_ACCOUNT) + ");");
	}

	bool read_correct;
	// launch separate thread for reading aggregate
	thread read_thread(read_total_balance, &db, &read_correct);

	// start vigorously updating balances in this thread
	for (size_t i = 0; i < TRANSACTION_UPDATE_COUNT; i++) {
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
	finished_updating = true;
	read_thread.join();
	REQUIRE(read_correct);
}

static std::atomic<size_t> finished_threads;

static void write_random_numbers_to_account(DuckDB *db, bool *correct, size_t nr) {
	correct[nr] = true;
	Connection con(*db);
	for (size_t i = 0; i < TRANSACTION_UPDATE_COUNT; i++) {
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
	if (finished_threads == TOTAL_ACCOUNTS) {
		finished_updating = true;
	}
}

TEST_CASE("Multiple concurrent updaters", "[updates][.]") {
	unique_ptr<MaterializedQueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	finished_updating = false;
	finished_threads = 0;
	// initialize the database
	con.Query("CREATE TABLE accounts(id INTEGER, money INTEGER)");
	for (size_t i = 0; i < TOTAL_ACCOUNTS; i++) {
		con.Query("INSERT INTO accounts VALUES (" + to_string(i) + ", " + to_string(MONEY_PER_ACCOUNT) + ");");
	}

	bool correct[TOTAL_ACCOUNTS];
	bool read_correct;
	std::thread write_threads[TOTAL_ACCOUNTS];
	// launch a thread for reading the table
	thread read_thread(read_total_balance, &db, &read_correct);
	// launch several threads for updating the table
	for (size_t i = 0; i < TOTAL_ACCOUNTS; i++) {
		write_threads[i] = thread(write_random_numbers_to_account, &db, correct, i);
	}
	read_thread.join();
	for (size_t i = 0; i < TOTAL_ACCOUNTS; i++) {
		write_threads[i].join();
		REQUIRE(correct[i]);
	}
}

} // namespace test_concurrent_update
