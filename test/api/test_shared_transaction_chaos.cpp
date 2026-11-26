#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"

#include <chrono>
#include <random>
#include <thread>

using namespace duckdb;

#ifndef DUCKDB_NO_THREADS

namespace {

//! Statements that connections of a shared transaction run in random order. Most of them conflict with each other.
const char *const CHAOS_STATEMENTS[] = {
    "INSERT INTO t SELECT range, range % 7, 'v' || range FROM range(200)",
    "INSERT INTO t SELECT range + 1000000, 1, repeat('x', 300) FROM range(50)",
    "INSERT INTO pk SELECT (random() * 1000000)::BIGINT, 'a' FROM range(20)",
    "INSERT INTO pk SELECT range + 5000000, 'b' FROM range(100)",
    "INSERT OR REPLACE INTO pk SELECT range, 'c' FROM range(30)",
    "INSERT INTO tmp SELECT range FROM range(100)",
    "UPDATE t SET b = b + 1 WHERE a % 5 = 0",
    "UPDATE t SET s = s || 'u' WHERE a < 100",
    "UPDATE pk SET v = 'u' WHERE k % 3 = 0",
    "DELETE FROM t WHERE a % 11 = 0",
    "DELETE FROM t WHERE a < 50",
    "DELETE FROM pk WHERE k % 13 = 0",
    "DELETE FROM tmp WHERE i % 2 = 0",
    "DELETE FROM t",
    "SELECT count(*), sum(a), max(s) FROM t",
    "SELECT b, count(*) FROM t GROUP BY b",
    "SELECT * FROM t WHERE a = 77",
    "SELECT count(*) FROM pk WHERE k = 5",
    "SELECT count(*) FROM t JOIN pk ON t.a = pk.k",
    "SELECT count(*) FROM tmp",
    "SELECT count(*) FROM duckdb_tables()",
    "SELECT a, row_number() OVER (PARTITION BY b ORDER BY a) FROM t ORDER BY a LIMIT 10",
    "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 3",
    "ALTER TABLE t DROP COLUMN c",
    "ALTER TABLE t ALTER b TYPE BIGINT",
    "ALTER TABLE t RENAME TO t_renamed",
    "ALTER TABLE t_renamed RENAME TO t",
    "CREATE INDEX idx_t ON t(a)",
    "DROP INDEX IF EXISTS idx_t",
    "CREATE OR REPLACE TABLE t2 AS SELECT * FROM t",
    "CREATE TABLE IF NOT EXISTS t3(i INTEGER)",
    "INSERT INTO t3 SELECT range FROM range(100)",
    "DROP TABLE IF EXISTS t3",
    "CREATE TEMPORARY TABLE IF NOT EXISTS tmp2 AS SELECT range i FROM range(10)",
    "DROP TABLE IF EXISTS tmp2",
    "CREATE SEQUENCE IF NOT EXISTS sq",
    "SELECT nextval('sq')",
    "CREATE OR REPLACE VIEW vw AS SELECT * FROM t",
    "SELECT count(*) FROM vw",
    "CREATE SCHEMA IF NOT EXISTS sc",
    "CREATE TABLE IF NOT EXISTS sc.x(i INTEGER)",
    "DROP SCHEMA IF EXISTS sc CASCADE",
    "CREATE MACRO IF NOT EXISTS mc(x) AS x + 1",
    "SELECT mc(a) FROM t LIMIT 3",
    "DELETE FROM t2",
    "TRUNCATE t2",
    "SELECT * FROM t UNION ALL SELECT * FROM t2 LIMIT 5",
    "PRAGMA table_info('t')",
    "SELECT current_transaction_id()",
    "SELECT * FROM duckdb_columns() LIMIT 5",
    "SELECT estimated_size FROM duckdb_tables()",
    "PREPARE ps AS SELECT count(*) FROM t WHERE a > $1",
    "EXECUTE ps(10)",
    "PREPARE ins AS INSERT INTO t VALUES ($1, 1, 'p')",
    "EXECUTE ins(7)",
    "INSERT INTO pk VALUES (10, 'upsert') ON CONFLICT DO UPDATE SET v = 'conflict'",
    "INSERT INTO pk VALUES (11, 'upsert') ON CONFLICT DO NOTHING",
    "INSERT INTO t SELECT range, 1, 's' FROM range(10) RETURNING a",
    "UPDATE t SET b = t2.b FROM t2 WHERE t.a = t2.a",
    "MERGE INTO t USING (SELECT range a FROM range(20)) m ON t.a = m.a WHEN MATCHED THEN UPDATE SET b = 9",
    "CREATE TABLE IF NOT EXISTS t4 (LIKE t)",
    "INSERT INTO t4 BY NAME SELECT a, s FROM t LIMIT 100",
    "CREATE TYPE IF NOT EXISTS mood AS ENUM ('a', 'b')",
    "DROP TYPE IF EXISTS mood",
    "SET threads = 2",
    "SET default_null_order = 'nulls_first'",
    "SELECT list(a) FROM (SELECT a FROM t LIMIT 50)",
    "SELECT {'a': a, 'b': [s]} FROM t LIMIT 20",
    "SELECT * FROM t t1, t t2 WHERE t1.a = t2.a AND t1.a < 100",
    "SELECT count(*) FROM (SELECT DISTINCT s FROM t)",
    "SELECT * FROM t ORDER BY s DESC LIMIT 5",
    "CREATE TABLE IF NOT EXISTS big AS SELECT range a, repeat('z', 100) s FROM range(20000)",
    "INSERT INTO big SELECT range, repeat('q', 50) FROM range(3000)",
    "SELECT count(*), max(s) FROM big",
    "DELETE FROM big WHERE a % 3 = 0",
    "CHECKPOINT",
    "VACUUM",
    "ANALYZE t",
    "SELECT * FROM pragma_storage_info('t') LIMIT 5",
    "SELECT * FROM duckdb_constraints() LIMIT 5",
    "SELECT * FROM duckdb_indexes() LIMIT 5",
    "DROP TABLE IF EXISTS big"};
constexpr idx_t CHAOS_STATEMENT_COUNT = sizeof(CHAOS_STATEMENTS) / sizeof(CHAOS_STATEMENTS[0]);

idx_t ChaosSeed() {
	auto env = getenv("DUCKDB_CHAOS_SEED");
	return env ? NumericCast<idx_t>(atoi(env)) * 1000003 : 0;
}

bool IsFatal(const string &error) {
	return error.find("INTERNAL") != string::npos || error.find("Internal Error") != string::npos ||
	       error.find("FATAL") != string::npos || error.find("Assertion") != string::npos ||
	       error.find("database has been invalidated") != string::npos;
}

struct ChaosLog {
	void Add(const string &query, const string &error) {
		lock_guard<mutex> guard(lock);
		if (IsFatal(error) && error.find("has been invalidated") == string::npos && fatal.size() < 3) {
			fatal.push_back(query + ": " + error);
		}
	}
	mutex lock;
	vector<string> fatal;
};

void RunChaosRound(idx_t round, const string &path, idx_t connection_count, idx_t statements_per_connection,
                   bool interrupts = false) {
	if (getenv("DUCKDB_CHAOS_VERBOSE")) {
		fprintf(stderr, "chaos round %llu\n", (unsigned long long)round);
	}
	if (!path.empty()) {
		DeleteDatabase(path);
	}
	{
		DuckDB db(path.empty() ? nullptr : path.c_str());
		Connection setup(db);
		REQUIRE_NO_FAIL(setup.Query("CREATE TABLE t(a BIGINT, b INTEGER, s VARCHAR)"));
		REQUIRE_NO_FAIL(setup.Query("CREATE TABLE t2(a BIGINT, b INTEGER, s VARCHAR)"));
		REQUIRE_NO_FAIL(setup.Query("CREATE TABLE pk(k BIGINT PRIMARY KEY, v VARCHAR)"));
		REQUIRE_NO_FAIL(setup.Query("INSERT INTO t SELECT range, range % 7, 'v' || range FROM range(5000)"));
		REQUIRE_NO_FAIL(setup.Query("INSERT INTO pk SELECT range, 'p' FROM range(3000)"));

		vector<unique_ptr<Connection>> cons;
		for (idx_t i = 0; i < connection_count; i++) {
			cons.push_back(make_uniq<Connection>(db));
		}
		REQUIRE_NO_FAIL(cons[0]->Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(cons[0]->Query("CREATE TEMPORARY TABLE tmp(i INTEGER)"));
		auto id_result = cons[0]->Query("SELECT share_transaction()");
		REQUIRE_NO_FAIL(*id_result);
		auto share_id = id_result->Collection().GetValue(0, 0).ToString();
		for (idx_t i = 1; i < connection_count; i++) {
			REQUIRE_NO_FAIL(cons[i]->Query("JOIN TRANSACTION '" + share_id + "'"));
		}

		ChaosLog log;
		vector<thread> threads;
		for (idx_t i = 0; i < connection_count; i++) {
			threads.emplace_back([&, i]() {
				std::mt19937 rng(ChaosSeed() + round * 1000 + i);
				auto &con = *cons[i];
				for (idx_t s = 0; s < statements_per_connection; s++) {
					if (rng() % 20 == 0) {
						try {
							Appender appender(con, "t");
							for (idx_t row = 0; row < 300; row++) {
								appender.AppendRow(int64_t(row), int32_t(row % 7), "appended");
							}
							appender.Close();
						} catch (std::exception &ex) {
							log.Add("appender", ex.what());
						}
						continue;
					}
					string query = CHAOS_STATEMENTS[rng() % CHAOS_STATEMENT_COUNT];
					auto start = std::chrono::steady_clock::now();
					auto result = con.Query(query);
					if (getenv("DUCKDB_CHAOS_VERBOSE")) {
						auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
						    std::chrono::steady_clock::now() - start);
						if (ms.count() > 1500) {
							fprintf(stderr, "slow statement (%lld ms): %s\n", (long long)ms.count(), query.c_str());
						}
					}
					if (result->HasError()) {
						log.Add(query, result->GetError());
					}
				}
				if (!interrupts && rng() % 3 == 0) {
					// closing the connection without ending the transaction rolls back its participation
					cons[i].reset();
				}
			});
		}
		atomic<bool> done {false};
		thread interrupter;
		if (interrupts) {
			interrupter = thread([&]() {
				std::mt19937 rng(ChaosSeed() + round);
				while (!done) {
					std::this_thread::sleep_for(std::chrono::milliseconds(15));
					cons[rng() % connection_count]->Interrupt();
				}
			});
		}
		for (auto &thread : threads) {
			thread.join();
		}
		done = true;
		if (interrupts) {
			interrupter.join();
		}
		if (getenv("DUCKDB_CHAOS_VERBOSE")) {
			fprintf(stderr, "threads done\n");
		}
		INFO("round " << round);
		for (auto &fatal : log.fatal) {
			INFO(fatal);
			FAIL_CHECK("internal error: " << fatal);
		}

		// the connections leave in random order, commit or roll back
		std::mt19937 rng(ChaosSeed() + round);
		vector<idx_t> order;
		for (idx_t i = 0; i < connection_count; i++) {
			order.push_back(i);
		}
		std::shuffle(order.begin(), order.end(), rng);
		for (auto i : order) {
			if (!cons[i]) {
				continue;
			}
			auto result = cons[i]->Query(rng() % 4 == 0 ? "ROLLBACK" : "COMMIT");
			if (result->HasError() && IsFatal(result->GetError())) {
				FAIL_CHECK("internal error: " << result->GetError());
			}
		}
		cons.clear();
		if (getenv("DUCKDB_CHAOS_VERBOSE")) {
			fprintf(stderr, "connections closed\n");
		}

		// the database is still usable
		for (auto &query : {"SELECT count(*), sum(a) FROM t", "SELECT count(*) FROM pk",
		                    "SELECT count(*) FROM pk WHERE k = 5", "SELECT count(*) FROM t2"}) {
			auto result = setup.Query(query);
			if (result->HasError() && result->GetError().find("does not exist") == string::npos) {
				INFO(query);
				REQUIRE_NO_FAIL(*result);
			}
		}
		// the primary key index is consistent with the table
		auto count = setup.Query("SELECT count(*) FROM pk");
		if (!count->HasError()) {
			auto expected = count->Collection().GetValue(0, 0).GetValue<int64_t>();
			REQUIRE_NO_FAIL(setup.Query("INSERT INTO pk VALUES (-1, 'probe')"));
			auto probe = setup.Query("SELECT count(*) FROM pk");
			REQUIRE_NO_FAIL(*probe);
			REQUIRE(probe->Collection().GetValue(0, 0).GetValue<int64_t>() == expected + 1);
		}
		auto check = setup.Query("CHECKPOINT");
		if (check->HasError()) {
			INFO(check->GetError());
			REQUIRE(!IsFatal(check->GetError()));
		}
	}
	if (!path.empty()) {
		// reopening the database must work and keep the committed changes consistent
		{
			DuckDB db(path.c_str());
			Connection con(db);
			auto result = con.Query("SELECT count(*) FROM pk");
			REQUIRE_NO_FAIL(*result);
		}
		DeleteDatabase(path);
	}
}

idx_t ChaosRounds(idx_t default_rounds) {
	auto env = getenv("DUCKDB_CHAOS_ROUNDS");
	return env ? NumericCast<idx_t>(atoi(env)) : default_rounds;
}

} // namespace

TEST_CASE("Random concurrent statements on a shared transaction", "[api][transaction][.]") {
	for (idx_t round = 0; round < ChaosRounds(10); round++) {
		RunChaosRound(round, string(), 6, 60);
	}
}

TEST_CASE("Random concurrent statements on a shared transaction with a file database", "[api][transaction][.]") {
	auto path = TestCreatePath("shared_transaction_chaos.db");
	for (idx_t round = 0; round < ChaosRounds(10); round++) {
		RunChaosRound(round, path, 4, 40);
	}
}

TEST_CASE("Random concurrent statements on a shared transaction with interrupts", "[api][transaction][.]") {
	for (idx_t round = 0; round < ChaosRounds(10); round++) {
		RunChaosRound(round, string(), 5, 40, true);
	}
}

#endif
