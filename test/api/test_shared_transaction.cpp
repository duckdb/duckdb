#include "catch.hpp"
#include "duckdb.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/thread.hpp"

using namespace duckdb;

static string ShareTransaction(Connection &con) {
	auto result = con.Query("SELECT share_transaction()");
	REQUIRE_NO_FAIL(*result);
	return result->Collection().GetValue(0, 0).ToString();
}

static void RequireValue(Connection &con, const string &query, int64_t expected) {
	auto result = con.Query(query);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected)}));
}

TEST_CASE("Test sharing a transaction between connections", "[api][transaction]") {
	DuckDB db(nullptr);
	Connection con1(db);
	Connection con2(db);
	Connection con3(db);
	REQUIRE_NO_FAIL(con1.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con1.Query("INSERT INTO tbl VALUES (1)"));

	SECTION("local changes and temporary tables are shared, changes flow both ways") {
		REQUIRE_NO_FAIL(con1.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con1.Query("INSERT INTO tbl VALUES (2)"));
		REQUIRE_NO_FAIL(con1.Query("CREATE TEMPORARY TABLE tmp(i INTEGER)"));
		REQUIRE_NO_FAIL(con1.Query("INSERT INTO tmp VALUES (10)"));
		auto share_id = ShareTransaction(con1);

		// not visible outside of the transaction
		RequireValue(con3, "SELECT SUM(i) FROM tbl", 1);
		REQUIRE_FAIL(con3.Query("SELECT * FROM tmp"));

		REQUIRE_NO_FAIL(con2.Query("JOIN TRANSACTION '" + share_id + "'"));
		RequireValue(con2, "SELECT SUM(i) FROM tbl", 3);
		RequireValue(con2, "SELECT SUM(i) FROM tmp", 10);

		// changes by the second connection are visible to the first
		REQUIRE_NO_FAIL(con2.Query("INSERT INTO tbl VALUES (3)"));
		REQUIRE_NO_FAIL(con2.Query("INSERT INTO tmp VALUES (20)"));
		REQUIRE_NO_FAIL(con2.Query("CREATE TEMPORARY TABLE tmp2 AS SELECT 5 AS i"));
		RequireValue(con1, "SELECT SUM(i) FROM tbl", 6);
		RequireValue(con1, "SELECT SUM(i) FROM tmp", 30);
		RequireValue(con1, "SELECT SUM(i) FROM tmp2", 5);

		// the second connection commits: the transaction is still running for the first connection
		REQUIRE_NO_FAIL(con2.Query("COMMIT"));
		RequireValue(con3, "SELECT SUM(i) FROM tbl", 1);
		RequireValue(con1, "SELECT SUM(i) FROM tbl", 6);
		REQUIRE_NO_FAIL(con2.Query("SELECT 42"));
		REQUIRE_FAIL(con2.Query("COMMIT"));

		// the last connection to commit commits the transaction
		REQUIRE_NO_FAIL(con1.Query("COMMIT"));
		RequireValue(con3, "SELECT SUM(i) FROM tbl", 6);
		// the share_id is no longer valid
		REQUIRE_FAIL(con3.Query("JOIN TRANSACTION '" + share_id + "'"));
	}
	SECTION("the owner can commit before the joined connection") {
		REQUIRE_NO_FAIL(con1.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con1.Query("INSERT INTO tbl VALUES (2)"));
		auto share_id = ShareTransaction(con1);
		REQUIRE_NO_FAIL(con2.Query("JOIN TRANSACTION '" + share_id + "'"));
		REQUIRE_NO_FAIL(con1.Query("COMMIT"));
		RequireValue(con3, "SELECT SUM(i) FROM tbl", 1);
		REQUIRE_NO_FAIL(con2.Query("INSERT INTO tbl VALUES (3)"));
		// the share_id can still be joined while a participant remains
		REQUIRE_NO_FAIL(con3.Query("JOIN TRANSACTION '" + share_id + "'"));
		RequireValue(con3, "SELECT SUM(i) FROM tbl", 6);
		REQUIRE_NO_FAIL(con3.Query("COMMIT"));
		RequireValue(con2, "SELECT SUM(i) FROM tbl", 6);
		REQUIRE_NO_FAIL(con2.Query("COMMIT"));
		RequireValue(con1, "SELECT SUM(i) FROM tbl", 6);
	}
	SECTION("a rollback by any participant rolls the whole transaction back") {
		REQUIRE_NO_FAIL(con1.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con1.Query("INSERT INTO tbl VALUES (2)"));
		auto share_id = ShareTransaction(con1);
		REQUIRE_NO_FAIL(con2.Query("JOIN TRANSACTION '" + share_id + "'"));
		REQUIRE_NO_FAIL(con2.Query("INSERT INTO tbl VALUES (3)"));
		REQUIRE_NO_FAIL(con2.Query("ROLLBACK"));
		// the transaction is still running for the first connection
		RequireValue(con1, "SELECT SUM(i) FROM tbl", 6);
		// the last statement is a COMMIT, but the transaction is rolled back
		REQUIRE_FAIL(con1.Query("COMMIT"));
		RequireValue(con3, "SELECT SUM(i) FROM tbl", 1);
		RequireValue(con1, "SELECT SUM(i) FROM tbl", 1);
	}
	SECTION("a rollback by the last participant after a commit rolls back") {
		REQUIRE_NO_FAIL(con1.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con1.Query("INSERT INTO tbl VALUES (2)"));
		auto share_id = ShareTransaction(con1);
		REQUIRE_NO_FAIL(con2.Query("JOIN TRANSACTION '" + share_id + "'"));
		REQUIRE_NO_FAIL(con2.Query("COMMIT"));
		REQUIRE_NO_FAIL(con1.Query("ROLLBACK"));
		RequireValue(con3, "SELECT SUM(i) FROM tbl", 1);
	}
	SECTION("exporting the same transaction twice returns the same share_id") {
		REQUIRE_NO_FAIL(con1.Query("BEGIN TRANSACTION"));
		auto share_id = ShareTransaction(con1);
		REQUIRE(share_id == ShareTransaction(con1));
		REQUIRE_NO_FAIL(con1.Query("COMMIT"));
	}
	SECTION("the owner closing the connection counts as a rollback, the others keep working") {
		auto owner = make_uniq<Connection>(db);
		REQUIRE_NO_FAIL(owner->Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(owner->Query("INSERT INTO tbl VALUES (2)"));
		REQUIRE_NO_FAIL(owner->Query("CREATE TEMPORARY TABLE tmp AS SELECT 7 AS i"));
		auto share_id = ShareTransaction(*owner);
		REQUIRE_NO_FAIL(con2.Query("JOIN TRANSACTION '" + share_id + "'"));
		owner.reset();
		RequireValue(con2, "SELECT SUM(i) FROM tbl", 3);
		RequireValue(con2, "SELECT SUM(i) FROM tmp", 7);
		REQUIRE_NO_FAIL(con2.Query("INSERT INTO tbl VALUES (3)"));
		REQUIRE_FAIL(con2.Query("COMMIT"));
		RequireValue(con3, "SELECT SUM(i) FROM tbl", 1);
	}
	SECTION("a joined connection closing counts as a rollback") {
		REQUIRE_NO_FAIL(con1.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con1.Query("INSERT INTO tbl VALUES (2)"));
		auto share_id = ShareTransaction(con1);
		auto joined = make_uniq<Connection>(db);
		REQUIRE_NO_FAIL(joined->Query("JOIN TRANSACTION '" + share_id + "'"));
		REQUIRE_NO_FAIL(joined->Query("INSERT INTO tbl VALUES (3)"));
		joined.reset();
		RequireValue(con1, "SELECT SUM(i) FROM tbl", 6);
		REQUIRE_FAIL(con1.Query("COMMIT"));
		RequireValue(con3, "SELECT SUM(i) FROM tbl", 1);
	}
	SECTION("concurrent inserts into a shared transaction") {
		REQUIRE_NO_FAIL(con1.Query("BEGIN TRANSACTION"));
		auto share_id = ShareTransaction(con1);
		REQUIRE_NO_FAIL(con2.Query("JOIN TRANSACTION '" + share_id + "'"));
		static constexpr idx_t ROWS_PER_THREAD = 100;
		auto insert = [&](Connection &con, int64_t base) {
			for (idx_t i = 0; i < ROWS_PER_THREAD; i++) {
				auto result = con.Query("INSERT INTO tbl VALUES (" + to_string(base + NumericCast<int64_t>(i)) + ")");
				REQUIRE_NO_FAIL(*result);
			}
		};
		thread t1([&]() { insert(con1, 1000); });
		thread t2([&]() { insert(con2, 2000); });
		t1.join();
		t2.join();
		RequireValue(con1, "SELECT COUNT(*) FROM tbl", 1 + 2 * ROWS_PER_THREAD);
		REQUIRE_NO_FAIL(con1.Query("COMMIT"));
		REQUIRE_NO_FAIL(con2.Query("COMMIT"));
		RequireValue(con3, "SELECT COUNT(*) FROM tbl", 1 + 2 * ROWS_PER_THREAD);
	}
	SECTION("concurrent reads and writes on a shared transaction") {
		REQUIRE_NO_FAIL(con1.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con1.Query("CREATE TEMPORARY TABLE tmp(i INTEGER)"));
		auto share_id = ShareTransaction(con1);
		REQUIRE_NO_FAIL(con2.Query("JOIN TRANSACTION '" + share_id + "'"));
		auto work = [&](Connection &con, int64_t base) {
			for (int64_t i = 0; i < 50; i++) {
				REQUIRE_NO_FAIL(*con.Query("INSERT INTO tmp VALUES (" + to_string(base + i) + ")"));
				REQUIRE_NO_FAIL(*con.Query("SELECT COUNT(*), SUM(i) FROM tmp"));
				REQUIRE_NO_FAIL(*con.Query("SELECT COUNT(*) FROM tbl"));
			}
		};
		thread t1([&]() { work(con1, 0); });
		thread t2([&]() { work(con2, 1000); });
		t1.join();
		t2.join();
		RequireValue(con1, "SELECT COUNT(*) FROM tmp", 100);
		REQUIRE_NO_FAIL(con2.Query("ROLLBACK"));
		REQUIRE_NO_FAIL(con1.Query("ROLLBACK"));
	}
	SECTION("the transaction outlives the connection that created it, with optimistic writes to disk") {
		auto path = TestCreatePath("shared_transaction_outlives_owner.db");
		DeleteDatabase(path);
		{
			DuckDB file_db(path);
			auto owner = make_uniq<Connection>(file_db);
			Connection joined(file_db);
			Connection reader(file_db);
			REQUIRE_NO_FAIL(owner->Query("CREATE TABLE big(i BIGINT)"));
			REQUIRE_NO_FAIL(owner->Query("BEGIN TRANSACTION"));
			REQUIRE_NO_FAIL(owner->Query("INSERT INTO big SELECT range FROM range(300000)"));
			auto share_id = ShareTransaction(*owner);
			REQUIRE_NO_FAIL(joined.Query("JOIN TRANSACTION '" + share_id + "'"));
			// the owner commits and goes away, the joined connection keeps using the transaction
			REQUIRE_NO_FAIL(owner->Query("COMMIT"));
			owner.reset();
			REQUIRE_NO_FAIL(joined.Query("INSERT INTO big SELECT range FROM range(300000)"));
			RequireValue(joined, "SELECT COUNT(*) FROM big", 600000);
			RequireValue(reader, "SELECT COUNT(*) FROM big", 0);
			REQUIRE_NO_FAIL(joined.Query("COMMIT"));
			RequireValue(reader, "SELECT COUNT(*) FROM big", 600000);
		}
		DuckDB reopened(path);
		Connection check(reopened);
		RequireValue(check, "SELECT COUNT(*) FROM big", 600000);
	}
	SECTION("concurrent writes of different kinds are serialized") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s(a BIGINT, b VARCHAR)"));
		REQUIRE_NO_FAIL(con1.Query("INSERT INTO s SELECT i, 'x' || i FROM range(20000) t(i)"));
		REQUIRE_NO_FAIL(con1.Query("BEGIN TRANSACTION"));
		auto share_id = ShareTransaction(con1);
		REQUIRE_NO_FAIL(con2.Query("JOIN TRANSACTION '" + share_id + "'"));
		auto run = [&](Connection &con, const vector<string> &queries) {
			for (idx_t i = 0; i < 50; i++) {
				for (auto &query : queries) {
					auto result = con.Query(query);
					if (result->HasError()) {
						FAIL(result->GetError());
					}
				}
			}
		};
		SECTION("inserts") {
			vector<string> insert {"INSERT INTO s SELECT i, 'y' || i FROM range(5000) t(i)"};
			thread t1([&]() { run(con1, insert); });
			thread t2([&]() { run(con2, insert); });
			t1.join();
			t2.join();
			RequireValue(con1, "SELECT COUNT(*) FROM s", 20000 + 2 * 50 * 5000);
		}
		SECTION("updates and deletes") {
			thread t1([&]() { run(con1, {"UPDATE s SET a = a + 1 WHERE a % 7 = 0"}); });
			thread t2([&]() { run(con2, {"DELETE FROM s WHERE a % 11 = 3"}); });
			t1.join();
			t2.join();
		}
		SECTION("mixed statements") {
			vector<string> mixed {"INSERT INTO s SELECT i, 'y' || i FROM range(1000) t(i)",
			                      "UPDATE s SET a = a + 1 WHERE a % 7 = 0",
			                      "DELETE FROM s WHERE a % 11 = 3",
			                      "INSERT INTO s SELECT a, b FROM s WHERE a < 100 LIMIT 10",
			                      "CREATE OR REPLACE TEMPORARY TABLE tmp AS SELECT * FROM s LIMIT 10",
			                      "SELECT COUNT(*), MAX(b) FROM s"};
			thread t1([&]() { run(con1, mixed); });
			thread t2([&]() { run(con2, mixed); });
			t1.join();
			t2.join();
		}
		REQUIRE_NO_FAIL(con2.Query("COMMIT"));
		REQUIRE_NO_FAIL(con1.Query("COMMIT"));
	}
	SECTION("a read of one connection sees the rows written by another connection grow") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s(a BIGINT)"));
		REQUIRE_NO_FAIL(con1.Query("BEGIN TRANSACTION"));
		auto share_id = ShareTransaction(con1);
		REQUIRE_NO_FAIL(con2.Query("JOIN TRANSACTION '" + share_id + "'"));
		atomic<bool> done {false};
		atomic<int64_t> failures {0};
		thread writer([&]() {
			for (idx_t i = 0; i < 100; i++) {
				if (con1.Query("INSERT INTO s SELECT * FROM range(20000)")->HasError()) {
					failures++;
				}
			}
			done = true;
		});
		int64_t previous = 0;
		bool monotonic = true;
		while (!done) {
			auto result = con2.Query("SELECT COUNT(*) FROM s");
			if (result->HasError()) {
				failures++;
				break;
			}
			auto count = result->Collection().GetValue(0, 0).GetValue<int64_t>();
			monotonic = monotonic && count >= previous && count <= 100 * 20000;
			previous = count;
		}
		writer.join();
		REQUIRE(failures == 0);
		REQUIRE(monotonic);
		RequireValue(con2, "SELECT COUNT(*) FROM s", 100 * 20000);
		REQUIRE_NO_FAIL(con2.Query("COMMIT"));
		REQUIRE_NO_FAIL(con1.Query("COMMIT"));
	}
	SECTION("errors") {
		// sharing requires a transaction
		REQUIRE_FAIL(con1.Query("SELECT share_transaction()"));
		// unknown identifier
		REQUIRE_FAIL(con2.Query("JOIN TRANSACTION 'unknown'"));
		REQUIRE_NO_FAIL(con2.Query("SELECT 42"));

		// cannot join from within a transaction
		REQUIRE_NO_FAIL(con1.Query("BEGIN TRANSACTION"));
		auto share_id = ShareTransaction(con1);
		REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
		REQUIRE_FAIL(con2.Query("JOIN TRANSACTION '" + share_id + "'"));
		REQUIRE_NO_FAIL(con2.Query("ROLLBACK"));

		// cannot join a connection's own transaction
		REQUIRE_FAIL(con1.Query("JOIN TRANSACTION '" + share_id + "'"));

		// cannot join an aborted transaction
		REQUIRE_FAIL(con1.Query("SELECT * FROM nonexistent"));
		REQUIRE_FAIL(con2.Query("JOIN TRANSACTION '" + share_id + "'"));
		REQUIRE_NO_FAIL(con2.Query("SELECT 42"));
		REQUIRE_NO_FAIL(con1.Query("ROLLBACK"));
	}
	SECTION("a joined connection is in a transaction until it commits") {
		REQUIRE_NO_FAIL(con1.Query("BEGIN TRANSACTION"));
		auto share_id = ShareTransaction(con1);
		REQUIRE_NO_FAIL(con2.Query("JOIN TRANSACTION '" + share_id + "'"));
		REQUIRE_NO_FAIL(con2.Query("INSERT INTO tbl VALUES (2)"));
		REQUIRE_NO_FAIL(con2.Query("COMMIT"));
		// back in auto-commit mode
		REQUIRE_FAIL(con2.Query("COMMIT"));
		REQUIRE_NO_FAIL(con2.Query("INSERT INTO tbl VALUES (3)"));
		RequireValue(con3, "SELECT SUM(i) FROM tbl", 4);
		REQUIRE_NO_FAIL(con1.Query("COMMIT"));
		RequireValue(con3, "SELECT SUM(i) FROM tbl", 6);
	}
}
