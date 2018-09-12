#include "catch.hpp"
#include "test_helpers.hpp"

#include <random>
#include <thread>

using namespace duckdb;
using namespace std;

#define TRANSACTION_UPDATE_COUNT 10000
#define TOTAL_ACCOUNTS 20
#define MONEY_PER_ACCOUNT 10

TEST_CASE("Single thread update", "[transactions]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	std::vector<std::unique_ptr<DuckDBConnection>> connections;

	// initialize the database
	con.Query("CREATE TABLE integers(i INTEGER);");
	int sum = 0;
	for (size_t i = 0; i < TOTAL_ACCOUNTS; i++) {
		for (size_t j = 0; j < 10; j++) {
			con.Query("INSERT INTO integers VALUES (" + to_string(j + 1) +
			          ");");
			sum += j + 1;
		}
	}

	// check the sum
	result = con.Query("SELECT SUM(i) FROM integers");
	CHECK_COLUMN(result, 0, {sum});

	// simple update, we should update INSERT_ELEMENTS elements
	result = con.Query("UPDATE integers SET i=4 WHERE i=2");
	CHECK_COLUMN(result, 0, {TOTAL_ACCOUNTS});

	// check updated sum
	result = con.Query("SELECT SUM(i) FROM integers");
	CHECK_COLUMN(result, 0, {sum + 2 * TOTAL_ACCOUNTS});
}

static volatile bool finished_updating = false;

static void read_total_balance(DuckDB *db) {
	REQUIRE(db);
	DuckDBConnection con(*db);
	while (!finished_updating) {
		// the total balance should remain constant regardless of updates
		auto result = con.Query("SELECT SUM(money) FROM accounts");
		CHECK_COLUMN(result, 0, {TOTAL_ACCOUNTS * MONEY_PER_ACCOUNT});
	}
}

TEST_CASE("[SLOW] Concurrent update", "[transactions]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

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
		con.Query("INSERT INTO accounts VALUES (" + to_string(i) + ", " +
		          to_string(MONEY_PER_ACCOUNT) + ");");
	}

	// launch separate thread for reading aggregate
	thread read_thread(read_total_balance, &db);

	// start vigorously updating balances in this thread
	for (size_t i = 0; i < TRANSACTION_UPDATE_COUNT; i++) {
		int from = random_account();
		int to = random_account();
		int amount = random_amount();

		con.Query("BEGIN TRANSACTION");
		con.Query("UPDATE accounts SET money = money - " + to_string(amount) +
		          " WHERE id = " + to_string(from));
		con.Query("UPDATE accounts SET money = money + " + to_string(amount) +
		          " WHERE id = " + to_string(to));
		con.Query("COMMIT");
	}
	finished_updating = true;
	read_thread.join();
}
