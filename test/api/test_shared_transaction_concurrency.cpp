#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"

#include <chrono>
#include <regex>

using namespace duckdb;

#ifndef DUCKDB_NO_THREADS

namespace {

//! Collects the errors of statements that run on other threads
class ErrorLog {
public:
	void Add(const string &query, const string &error) {
		lock_guard<mutex> guard(lock);
		errors.push_back(query.substr(0, 80) + ": " + error.substr(0, 160));
	}
	bool Empty() {
		lock_guard<mutex> guard(lock);
		return errors.empty();
	}
	bool Contains(const string &needle) {
		lock_guard<mutex> guard(lock);
		for (auto &error : errors) {
			if (error.find(needle) != string::npos) {
				return true;
			}
		}
		return false;
	}
	//! Whether every error matches the regular expression
	bool AllMatch(const string &pattern) {
		lock_guard<mutex> guard(lock);
		std::regex re(pattern);
		for (auto &error : errors) {
			if (!std::regex_search(error, re)) {
				return false;
			}
		}
		return true;
	}
	string ToString() {
		lock_guard<mutex> guard(lock);
		string result;
		for (auto &error : errors) {
			result += error + "\n";
		}
		return result;
	}

private:
	mutex lock;
	vector<string> errors;
};

void Exec(Connection &con, const string &query, ErrorLog &log) {
	auto result = con.Query(query);
	if (result->HasError()) {
		log.Add(query, result->GetError());
	}
}

int64_t Scalar(Connection &con, const string &query) {
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	return result->Collection().GetValue(0, 0).GetValue<int64_t>();
}

//! A database with `count` connections that all participate in one shared transaction
struct SharedTransaction {
	explicit SharedTransaction(idx_t count = 3, const string &path = string()) {
		db = make_uniq<DuckDB>(path.empty() ? nullptr : path.c_str());
		for (idx_t i = 0; i < count; i++) {
			cons.push_back(make_uniq<Connection>(*db));
		}
	}

	//! Starts the transaction in the first connection and joins the others
	void Share() {
		REQUIRE_NO_FAIL(cons[0]->Query("BEGIN TRANSACTION"));
		auto result = cons[0]->Query("SELECT share_transaction()");
		REQUIRE_NO_FAIL(*result);
		share_id = result->Collection().GetValue(0, 0).ToString();
		for (idx_t i = 1; i < cons.size(); i++) {
			REQUIRE_NO_FAIL(cons[i]->Query("JOIN TRANSACTION '" + share_id + "'"));
		}
	}

	//! Every connection commits, the last one commits the transaction
	void Commit() {
		for (idx_t i = cons.size(); i > 0; i--) {
			REQUIRE_NO_FAIL(cons[i - 1]->Query("COMMIT"));
		}
	}

	Connection &operator[](idx_t index) {
		return *cons[index];
	}

	unique_ptr<DuckDB> db;
	vector<unique_ptr<Connection>> cons;
	string share_id;
};

//! Runs `writes` on the writer connection while the reader connections run `reads` until the writer is done
void WriterWithReaders(Connection &writer, const vector<string> &writes, idx_t iterations,
                       const vector<Connection *> &readers, const vector<string> &reads, ErrorLog &log) {
	atomic<bool> done {false};
	thread writer_thread([&]() {
		for (idx_t i = 0; i < iterations; i++) {
			for (auto &query : writes) {
				Exec(writer, query, log);
			}
		}
		done = true;
	});
	vector<thread> reader_threads;
	for (auto reader : readers) {
		reader_threads.emplace_back([&, reader]() {
			idx_t i = 0;
			while (!done) {
				Exec(*reader, reads[i++ % reads.size()], log);
			}
		});
	}
	writer_thread.join();
	for (auto &reader_thread : reader_threads) {
		reader_thread.join();
	}
}

} // namespace

TEST_CASE("Concurrent reads run alongside writes on a shared transaction", "[api][transaction]") {
	SharedTransaction shared;
	ErrorLog log;
	auto &con1 = shared[0];

	SECTION("inserts of strings") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s(a BIGINT, b VARCHAR)"));
		shared.Share();
		WriterWithReaders(shared[0], {"INSERT INTO s SELECT i, 'value' || i FROM range(3000) t(i)"}, 30,
		                  {&shared[1], &shared[2]},
		                  {"SELECT count(*), max(b) FROM s", "SELECT * FROM s WHERE a = 5 LIMIT 3",
		                   "SELECT b FROM s ORDER BY a DESC LIMIT 5"},
		                  log);
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(shared[1], "SELECT count(*) FROM s") == 90000);
		shared.Commit();
		REQUIRE(Scalar(con1, "SELECT count(*) FROM s") == 90000);
	}
	SECTION("updates") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s AS SELECT i a, 'v' || i b FROM range(50000) t(i)"));
		shared.Share();
		WriterWithReaders(
		    shared[0], {"UPDATE s SET a = a + 1000000 WHERE a % 7 = 0", "UPDATE s SET b = b || 'x' WHERE a % 13 = 0"},
		    5, {&shared[1], &shared[2]},
		    {"SELECT count(*), max(b), min(a) FROM s", "SELECT * FROM s WHERE a % 101 = 0 LIMIT 5"}, log);
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(shared[2], "SELECT count(*) FROM s") == 50000);
		REQUIRE(Scalar(shared[2], "SELECT count(*) FROM s WHERE a >= 1000000") > 0);
		shared.Commit();
	}
	SECTION("deletes") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s AS SELECT i a, 'v' || i b FROM range(100000) t(i)"));
		shared.Share();
		WriterWithReaders(shared[0], {"DELETE FROM s WHERE a % 11 = 3 AND a < 100000"}, 3, {&shared[1], &shared[2]},
		                  {"SELECT count(*) FROM s", "SELECT * FROM s LIMIT 5"}, log);
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(shared[1], "SELECT count(*) FROM s WHERE a % 11 = 3") == 0);
		shared.Commit();
		REQUIRE(Scalar(con1, "SELECT count(*) FROM s") == 100000 - 9091);
	}
	SECTION("nested types") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s(l INTEGER[], st STRUCT(a INT, b VARCHAR), m MAP(VARCHAR, INT), "
		                           "u UNION(i INT, s VARCHAR), arr INTEGER[2])"));
		shared.Share();
		WriterWithReaders(shared[0],
		                  {"INSERT INTO s SELECT [i, i + 1], {'a': i, 'b': 'x' || i}, MAP {'k' || i: i}, "
		                   "union_value(s := 'u' || i), [i, i] FROM range(2000) t(i)",
		                   "UPDATE s SET l = [1] WHERE st.a % 50 = 0"},
		                  10, {&shared[1], &shared[2]},
		                  {"SELECT count(*), max(len(l)) FROM s", "SELECT st.b FROM s WHERE st.a = 3",
		                   "SELECT m['k3'] FROM s LIMIT 3", "SELECT union_tag(u) FROM s LIMIT 3"},
		                  log);
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(shared[1], "SELECT count(*) FROM s") == 20000);
		shared.Commit();
	}
	SECTION("a table with indexes") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s(id BIGINT PRIMARY KEY, v VARCHAR)"));
		REQUIRE_NO_FAIL(con1.Query("CREATE INDEX vi ON s(v)"));
		shared.Share();
		atomic<int64_t> next {0};
		atomic<bool> done {false};
		thread writer([&]() {
			for (idx_t i = 0; i < 20; i++) {
				auto base = next.fetch_add(1000);
				Exec(shared[0], "INSERT INTO s SELECT i + " + to_string(base) + ", 'v' || i FROM range(1000) t(i)",
				     log);
			}
			done = true;
		});
		thread reader1([&]() {
			while (!done) {
				Exec(shared[1], "SELECT * FROM s WHERE id = 500", log);
				Exec(shared[1], "SELECT count(*) FROM s WHERE v = 'v7'", log);
			}
		});
		thread reader2([&]() {
			while (!done) {
				Exec(shared[2], "SELECT count(*) FROM s", log);
			}
		});
		writer.join();
		reader1.join();
		reader2.join();
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(shared[1], "SELECT count(*) FROM s WHERE v = 'v7'") == 20);
		shared.Commit();
		REQUIRE(Scalar(con1, "SELECT count(*) FROM s") == 20000);
	}
	SECTION("inserts that read from the table itself") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s AS SELECT i a, 'v' || i b FROM range(1000) t(i)"));
		shared.Share();
		WriterWithReaders(shared[0], {"INSERT INTO s SELECT a + 1000000, b FROM s WHERE a < 1000"}, 10,
		                  {&shared[1], &shared[2]}, {"SELECT count(*) FROM s", "SELECT max(b) FROM s"}, log);
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(shared[1], "SELECT count(*) FROM s") == 11000);
		shared.Commit();
	}
	SECTION("schema changes") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s(a BIGINT, b VARCHAR)"));
		REQUIRE_NO_FAIL(con1.Query("INSERT INTO s SELECT i, 'v' || i FROM range(1000) t(i)"));
		shared.Share();
		atomic<bool> done {false};
		thread ddl([&]() {
			for (idx_t i = 0; i < 15; i++) {
				auto n = to_string(i);
				Exec(shared[0], "CREATE TABLE d" + n + " AS SELECT * FROM s", log);
				Exec(shared[0], "ALTER TABLE d" + n + " ADD COLUMN c INTEGER DEFAULT 7", log);
				Exec(shared[0], "CREATE INDEX di" + n + " ON d" + n + "(a)", log);
				Exec(shared[1], "CREATE VIEW dv" + n + " AS SELECT * FROM d" + n, log);
				Exec(shared[1], "CREATE TEMPORARY TABLE tt" + n + "(x INTEGER)", log);
				Exec(shared[0], "DROP TABLE tt" + n, log);
				Exec(shared[0], "ALTER TABLE d" + n + " ALTER c SET DEFAULT 9", log);
				Exec(shared[1], "CREATE SEQUENCE sq" + n, log);
				Exec(shared[1], "CREATE MACRO mc" + n + "() AS " + n, log);
			}
			done = true;
		});
		thread reader([&]() {
			while (!done) {
				Exec(shared[2], "SELECT count(*) FROM duckdb_tables()", log);
				Exec(shared[2], "SELECT count(*) FROM s", log);
				Exec(shared[2], "SELECT count(*) FROM duckdb_columns()", log);
			}
		});
		ddl.join();
		reader.join();
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(shared[2], "SELECT count(*) FROM dv14") == 1000);
		shared.Commit();
		REQUIRE(Scalar(con1, "SELECT count(*) FROM d14") == 1000);
	}
	SECTION("an open read does not block writers of other connections") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s AS SELECT i a FROM range(100000) t(i)"));
		shared.Share();
		auto open_read = shared[1].Submit("SELECT a FROM s");
		REQUIRE_NO_FAIL(*open_read);
		for (idx_t step = 0; step < 3; step++) {
			open_read->ExecuteTask();
		}
		REQUIRE_NO_FAIL(shared[0].Query("INSERT INTO s VALUES (-1)"));
		REQUIRE_NO_FAIL(shared[2].Query("INSERT INTO s VALUES (-2)"));
		open_read.reset();
		REQUIRE(Scalar(shared[1], "SELECT count(*) FROM s") == 100002);
		shared.Commit();
	}
}

TEST_CASE("Concurrent writes on a shared transaction", "[api][transaction]") {
	SharedTransaction shared;
	ErrorLog log;
	auto &con1 = shared[0];

	SECTION("inserts of strings") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s(a BIGINT, b VARCHAR)"));
		shared.Share();
		auto insert = [&](Connection &con) {
			for (idx_t i = 0; i < 40; i++) {
				Exec(con, "INSERT INTO s SELECT i, 'y' || i FROM range(5000) t(i)", log);
			}
		};
		thread t0([&]() { insert(shared[0]); });
		thread t1([&]() { insert(shared[1]); });
		thread t2([&]() { insert(shared[2]); });
		t0.join();
		t1.join();
		t2.join();
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(shared[0], "SELECT count(*) FROM s") == 3 * 40 * 5000);
		REQUIRE(Scalar(shared[0], "SELECT count(DISTINCT b) FROM s") == 5000);
		shared.Commit();
		REQUIRE(Scalar(con1, "SELECT count(*) FROM s") == 3 * 40 * 5000);
	}
	SECTION("updates and deletes of the same rows") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s AS SELECT i a, 'x' || i b FROM range(20000) t(i)"));
		shared.Share();
		thread t0([&]() {
			for (idx_t i = 0; i < 50; i++) {
				Exec(shared[0], "UPDATE s SET a = a + 1 WHERE a % 7 = 0", log);
			}
		});
		thread t1([&]() {
			for (idx_t i = 0; i < 50; i++) {
				Exec(shared[1], "DELETE FROM s WHERE a % 11 = 3", log);
			}
		});
		thread t2([&]() {
			for (idx_t i = 0; i < 50; i++) {
				Exec(shared[2], "UPDATE s SET b = b || 'z' WHERE a % 5 = 0", log);
			}
		});
		t0.join();
		t1.join();
		t2.join();
		INFO(log.ToString());
		if (log.Empty()) {
			REQUIRE(Scalar(shared[0], "SELECT count(*) FROM s WHERE a % 11 = 3") == 0);
			shared.Commit();
		} else {
			// a statement wrote a row that another statement of the transaction deleted after the first found it:
			// a conflict (like between transactions) that aborts the transaction, never an internal error
			REQUIRE(log.AllMatch("Conflict on|aborted|invalidated"));
			REQUIRE(!log.Contains("INTERNAL"));
			REQUIRE(!log.Contains("FATAL"));
			for (idx_t i = shared.cons.size(); i > 0; i--) {
				shared[i - 1].Query("ROLLBACK");
			}
			REQUIRE(Scalar(con1, "SELECT count(*) FROM s") == 20000);
		}
	}
	SECTION("create or replace of the same table") {
		shared.Share();
		auto work = [&](Connection &con) {
			for (idx_t i = 0; i < 50; i++) {
				Exec(con, "CREATE OR REPLACE TABLE r AS SELECT i a FROM range(1000) t(i)", log);
			}
		};
		thread t0([&]() { work(shared[0]); });
		thread t1([&]() { work(shared[1]); });
		thread t2([&]() { work(shared[2]); });
		t0.join();
		t1.join();
		t2.join();
		INFO(log.ToString());
		if (log.Empty()) {
			REQUIRE(Scalar(shared[0], "SELECT count(*) FROM r") == 1000);
			shared.Commit();
		} else {
			// the replace of one connection raced with the replace of another: a conflict that aborts the transaction
			REQUIRE(log.AllMatch("Catalog write-write conflict|aborted|invalidated"));
			for (idx_t i = shared.cons.size(); i > 0; i--) {
				shared[i - 1].Query("ROLLBACK");
			}
		}
	}
	SECTION("a mix of statements") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s(a BIGINT, b VARCHAR)"));
		REQUIRE_NO_FAIL(con1.Query("INSERT INTO s SELECT i, 'x' || i FROM range(20000) t(i)"));
		shared.Share();
		auto work = [&](Connection &con, const string &name, idx_t seed) {
			for (idx_t i = 0; i < 60; i++) {
				switch ((i + seed) % 6) {
				case 0:
					Exec(con, "INSERT INTO s SELECT i, 'y' || i FROM range(1500) t(i)", log);
					break;
				case 1:
					Exec(con, "UPDATE s SET a = a + 1 WHERE a % 7 = 0", log);
					break;
				case 2:
					Exec(con, "DELETE FROM s WHERE a % 11 = 3", log);
					break;
				case 3:
					Exec(con, "INSERT INTO s SELECT a, b FROM s WHERE a < 100 LIMIT 10", log);
					break;
				case 4:
					Exec(con, "SELECT count(*), max(b) FROM s", log);
					break;
				default:
					Exec(con, "CREATE OR REPLACE TEMPORARY TABLE tmp_" + name + " AS SELECT * FROM s LIMIT 10", log);
					break;
				}
			}
		};
		thread t0([&]() { work(shared[0], "a", 0); });
		thread t1([&]() { work(shared[1], "b", 2); });
		thread t2([&]() { work(shared[2], "c", 4); });
		t0.join();
		t1.join();
		t2.join();
		INFO(log.ToString());
		REQUIRE(log.Empty());
		shared.Commit();
	}
	SECTION("writers of different tables") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE t1(a BIGINT, b VARCHAR)"));
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE t2(a BIGINT, b VARCHAR)"));
		shared.Share();
		thread t0([&]() {
			for (idx_t i = 0; i < 30; i++) {
				Exec(shared[0], "INSERT INTO t1 SELECT i, 'a' || i FROM range(2000) t(i)", log);
			}
		});
		thread t1([&]() {
			for (idx_t i = 0; i < 30; i++) {
				Exec(shared[1], "INSERT INTO t2 SELECT i, 'b' || i FROM range(2000) t(i)", log);
			}
		});
		thread t2([&]() {
			for (idx_t i = 0; i < 90; i++) {
				Exec(shared[2], "SELECT count(*), (SELECT count(*) FROM t2) FROM t1", log);
			}
		});
		t0.join();
		t1.join();
		t2.join();
		INFO(log.ToString());
		REQUIRE(log.Empty());
		shared.Commit();
		REQUIRE(Scalar(con1, "SELECT count(*) FROM t1") == 60000);
		REQUIRE(Scalar(con1, "SELECT count(*) FROM t2") == 60000);
	}
	SECTION("concurrent inserts into a table with a primary key") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s(id BIGINT PRIMARY KEY, v VARCHAR)"));
		shared.Share();
		thread t0([&]() {
			for (idx_t i = 0; i < 20; i++) {
				Exec(shared[0], "INSERT INTO s SELECT i + " + to_string(i * 2000) + ", 'a' FROM range(1000) t(i)", log);
			}
		});
		thread t1([&]() {
			for (idx_t i = 0; i < 20; i++) {
				Exec(shared[1],
				     "INSERT INTO s SELECT i + " + to_string(i * 2000 + 1000) + ", 'b' FROM range(1000) t(i)", log);
			}
		});
		t0.join();
		t1.join();
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(shared[2], "SELECT count(*) FROM s") == 40000);
		// the constraint is enforced across the connections
		REQUIRE_FAIL(shared[2].Query("INSERT INTO s VALUES (5, 'duplicate')"));
		REQUIRE_NO_FAIL(shared[0].Query("ROLLBACK"));
		REQUIRE_NO_FAIL(shared[1].Query("ROLLBACK"));
		REQUIRE_NO_FAIL(shared[2].Query("ROLLBACK"));
		REQUIRE(Scalar(con1, "SELECT count(*) FROM s") == 0);
	}
	SECTION("conflicting keys are detected between connections") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s(id BIGINT PRIMARY KEY)"));
		shared.Share();
		REQUIRE_NO_FAIL(shared[0].Query("INSERT INTO s VALUES (1)"));
		REQUIRE_FAIL(shared[1].Query("INSERT INTO s VALUES (1)"));
		REQUIRE_NO_FAIL(shared[0].Query("ROLLBACK"));
		REQUIRE_NO_FAIL(shared[1].Query("ROLLBACK"));
		REQUIRE_NO_FAIL(shared[2].Query("ROLLBACK"));
	}
	SECTION("sequences hand out unique values") {
		REQUIRE_NO_FAIL(con1.Query("CREATE SEQUENCE sq"));
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s(a BIGINT)"));
		shared.Share();
		auto work = [&](Connection &con) {
			for (idx_t i = 0; i < 100; i++) {
				Exec(con, "INSERT INTO s VALUES (nextval('sq'))", log);
			}
		};
		thread t0([&]() { work(shared[0]); });
		thread t1([&]() { work(shared[1]); });
		thread t2([&]() { work(shared[2]); });
		t0.join();
		t1.join();
		t2.join();
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(shared[0], "SELECT count(DISTINCT a) FROM s") == 300);
		REQUIRE(Scalar(shared[0], "SELECT count(*) FROM s") == 300);
		shared.Commit();
	}
	SECTION("appenders") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s(a BIGINT, b VARCHAR)"));
		shared.Share();
		auto append = [&](Connection &con, int64_t base) {
			for (idx_t i = 0; i < 20; i++) {
				try {
					Appender appender(con, "s");
					for (idx_t j = 0; j < 500; j++) {
						appender.AppendRow(base + NumericCast<int64_t>(i * 500 + j), string_t("append"));
					}
					appender.Close();
				} catch (std::exception &ex) {
					log.Add("appender", ex.what());
				}
			}
		};
		thread t0([&]() { append(shared[0], 0); });
		thread t1([&]() { append(shared[1], 1000000); });
		thread t2([&]() {
			for (idx_t i = 0; i < 60; i++) {
				Exec(shared[2], "SELECT count(*), max(b) FROM s", log);
			}
		});
		t0.join();
		t1.join();
		t2.join();
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(shared[2], "SELECT count(*) FROM s") == 20000);
		shared.Commit();
		REQUIRE(Scalar(con1, "SELECT count(*) FROM s") == 20000);
	}
	SECTION("prepared statements") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s(a BIGINT, b VARCHAR)"));
		shared.Share();
		auto work = [&](Connection &con, int64_t base) {
			auto insert = con.Prepare("INSERT INTO s SELECT i + $1, 'p' FROM range(500) t(i)");
			auto count = con.Prepare("SELECT count(*) FROM s WHERE a >= $1");
			if (insert->HasError() || count->HasError()) {
				log.Add("prepare", insert->HasError() ? insert->GetError() : count->GetError());
				return;
			}
			for (idx_t i = 0; i < 40; i++) {
				auto result = insert->Execute(base + NumericCast<int64_t>(i) * 1000);
				if (result->HasError()) {
					log.Add("insert", result->GetError());
				}
				result = count->Execute(base);
				if (result->HasError()) {
					log.Add("count", result->GetError());
				}
			}
		};
		thread t0([&]() { work(shared[0], 0); });
		thread t1([&]() { work(shared[1], 10000000); });
		t0.join();
		t1.join();
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(shared[2], "SELECT count(*) FROM s") == 40000);
		shared.Commit();
	}
	SECTION("copy from a file") {
		auto file = TestCreatePath("shared_transaction_copy.csv");
		REQUIRE_NO_FAIL(con1.Query("COPY (SELECT i a, 'c' || i b FROM range(5000) t(i)) TO '" + file + "' (HEADER)"));
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s(a BIGINT, b VARCHAR)"));
		shared.Share();
		auto work = [&](Connection &con) {
			for (idx_t i = 0; i < 10; i++) {
				Exec(con, "COPY s FROM '" + file + "' (HEADER)", log);
			}
		};
		thread t0([&]() { work(shared[0]); });
		thread t1([&]() { work(shared[1]); });
		t0.join();
		t1.join();
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(shared[2], "SELECT count(*) FROM s") == 100000);
		shared.Commit();
		DeleteDatabase(file);
	}
	SECTION("on conflict and merge") {
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE s(id BIGINT PRIMARY KEY, v BIGINT)"));
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE m(id BIGINT, v BIGINT)"));
		REQUIRE_NO_FAIL(con1.Query("INSERT INTO m SELECT i, i FROM range(1000) t(i)"));
		shared.Share();
		thread t0([&]() {
			for (idx_t i = 0; i < 30; i++) {
				Exec(shared[0], "INSERT INTO s SELECT id, v FROM m ON CONFLICT (id) DO UPDATE SET v = excluded.v + 1",
				     log);
			}
		});
		thread t1([&]() {
			for (idx_t i = 0; i < 30; i++) {
				Exec(shared[1],
				     "MERGE INTO m USING (SELECT i id FROM range(1000) t(i)) u ON m.id = u.id "
				     "WHEN MATCHED THEN UPDATE SET v = v + 1",
				     log);
			}
		});
		thread t2([&]() {
			for (idx_t i = 0; i < 60; i++) {
				Exec(shared[2], "SELECT count(*), max(v) FROM s", log);
			}
		});
		t0.join();
		t1.join();
		t2.join();
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(shared[2], "SELECT count(*) FROM s") == 1000);
		shared.Commit();
	}
	SECTION("temporary tables created concurrently") {
		shared.Share();
		thread t0([&]() {
			for (idx_t i = 0; i < 30; i++) {
				Exec(shared[0], "CREATE TEMPORARY TABLE a" + to_string(i) + " AS SELECT range i FROM range(100)", log);
			}
		});
		thread t1([&]() {
			for (idx_t i = 0; i < 30; i++) {
				Exec(shared[1], "CREATE TEMPORARY TABLE b" + to_string(i) + " AS SELECT range i FROM range(100)", log);
			}
		});
		t0.join();
		t1.join();
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(shared[2], "SELECT count(*) FROM a29") == 100);
		REQUIRE(Scalar(shared[2], "SELECT count(*) FROM b29") == 100);
		shared.Commit();
	}
	SECTION("many connections inserting disjoint ranges") {
		SharedTransaction many(8);
		REQUIRE_NO_FAIL(many[0].Query("CREATE TABLE s(a BIGINT)"));
		many.Share();
		vector<thread> threads;
		for (idx_t c = 0; c < 8; c++) {
			threads.emplace_back([&, c]() {
				for (idx_t i = 0; i < 20; i++) {
					Exec(many[c],
					     "INSERT INTO s SELECT i + " + to_string(c * 1000000 + i * 1000) + " FROM range(1000) t(i)",
					     log);
				}
			});
		}
		for (auto &t : threads) {
			t.join();
		}
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(many[3], "SELECT count(DISTINCT a) FROM s") == 8 * 20 * 1000);
		many.Commit();
		REQUIRE(Scalar(many[0], "SELECT count(*) FROM s") == 8 * 20 * 1000);
	}
}

TEST_CASE("Shared transactions with a file database", "[api][transaction]") {
	auto path = TestCreatePath("shared_transaction_concurrency.db");
	DeleteDatabase(path);
	{
		SharedTransaction shared(3, path);
		ErrorLog log;
		REQUIRE_NO_FAIL(shared[0].Query("CREATE TABLE s(a BIGINT, b VARCHAR)"));
		shared.Share();
		// large enough to write blocks to disk before the commit
		WriterWithReaders(shared[0], {"INSERT INTO s SELECT i, 'v' || i FROM range(300000) t(i)"}, 4,
		                  {&shared[1], &shared[2]},
		                  {"SELECT count(*), max(b) FROM s", "SELECT * FROM s WHERE a = 77 LIMIT 3"}, log);
		INFO(log.ToString());
		REQUIRE(log.Empty());
		auto insert_other = [&](Connection &con) {
			for (idx_t i = 0; i < 3; i++) {
				Exec(con, "INSERT INTO s SELECT i, 'w' || i FROM range(300000) t(i)", log);
			}
		};
		thread t1([&]() { insert_other(shared[1]); });
		thread t2([&]() { insert_other(shared[2]); });
		t1.join();
		t2.join();
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(shared[0], "SELECT count(*) FROM s") == 4 * 300000 + 2 * 3 * 300000);
		shared.Commit();
	}
	{
		DuckDB reopened(path);
		Connection check(reopened);
		REQUIRE(Scalar(check, "SELECT count(*) FROM s") == 4 * 300000 + 2 * 3 * 300000);
		REQUIRE(Scalar(check, "SELECT count(*) FROM s WHERE b LIKE 'w%'") == 2 * 3 * 300000);
	}
	DeleteDatabase(path);
}

TEST_CASE("Joining and leaving a shared transaction while it is in use", "[api][transaction]") {
	DuckDB db(nullptr);
	Connection owner(db);
	ErrorLog log;
	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE s(a BIGINT)"));

	SECTION("connections join and commit repeatedly") {
		for (idx_t round = 0; round < 30; round++) {
			REQUIRE_NO_FAIL(owner.Query("BEGIN TRANSACTION"));
			auto result = owner.Query("SELECT share_transaction()");
			REQUIRE_NO_FAIL(*result);
			auto share_id = result->Collection().GetValue(0, 0).ToString();
			vector<unique_ptr<Connection>> joined;
			for (idx_t i = 0; i < 3; i++) {
				joined.push_back(make_uniq<Connection>(db));
			}
			vector<thread> threads;
			for (idx_t i = 0; i < 3; i++) {
				threads.emplace_back([&, i]() {
					Exec(*joined[i], "JOIN TRANSACTION '" + share_id + "'", log);
					Exec(*joined[i], "INSERT INTO s VALUES (" + to_string(round * 10 + i) + ")", log);
					Exec(*joined[i], "COMMIT", log);
				});
			}
			Exec(owner, "INSERT INTO s VALUES (" + to_string(round * 10 + 9) + ")", log);
			for (auto &t : threads) {
				t.join();
			}
			REQUIRE_NO_FAIL(owner.Query("COMMIT"));
		}
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE(Scalar(owner, "SELECT count(*) FROM s") == 30 * 4);
		REQUIRE(Scalar(owner, "SELECT count(DISTINCT a) FROM s") == 30 * 4);
	}
	SECTION("a connection leaves while the others write") {
		for (idx_t round = 0; round < 20; round++) {
			SharedTransaction shared(3);
			REQUIRE_NO_FAIL(shared[0].Query("CREATE TABLE s(a BIGINT, b VARCHAR)"));
			shared.Share();
			thread writer([&]() {
				for (idx_t i = 0; i < 5; i++) {
					Exec(shared[0], "INSERT INTO s SELECT i, 'v' || i FROM range(2000) t(i)", log);
				}
			});
			thread leaver([&]() {
				for (idx_t i = 0; i < 5; i++) {
					Exec(shared[1], "SELECT count(*) FROM s", log);
				}
				Exec(shared[1], "COMMIT", log);
			});
			writer.join();
			leaver.join();
			REQUIRE_NO_FAIL(shared[2].Query("COMMIT"));
			REQUIRE_NO_FAIL(shared[0].Query("COMMIT"));
			REQUIRE(Scalar(shared[0], "SELECT count(*) FROM s") == 10000);
		}
		INFO(log.ToString());
		REQUIRE(log.Empty());
	}
	SECTION("a rolled back connection rolls back writes of the others") {
		SharedTransaction shared(3);
		REQUIRE_NO_FAIL(shared[0].Query("CREATE TABLE s(a BIGINT)"));
		shared.Share();
		thread t0([&]() {
			for (idx_t i = 0; i < 20; i++) {
				Exec(shared[0], "INSERT INTO s SELECT i FROM range(1000) t(i)", log);
			}
		});
		thread t1([&]() {
			for (idx_t i = 0; i < 20; i++) {
				Exec(shared[1], "INSERT INTO s SELECT i FROM range(1000) t(i)", log);
			}
			Exec(shared[1], "ROLLBACK", log);
		});
		t0.join();
		t1.join();
		INFO(log.ToString());
		REQUIRE(log.Empty());
		REQUIRE_NO_FAIL(shared[2].Query("COMMIT"));
		REQUIRE_FAIL(shared[0].Query("COMMIT"));
		Connection observer(*shared.db);
		REQUIRE(Scalar(observer, "SELECT count(*) FROM s") == 0);
	}
	SECTION("a failing statement aborts the transaction of every connection") {
		SharedTransaction shared(3);
		REQUIRE_NO_FAIL(shared[0].Query("CREATE TABLE s(a BIGINT)"));
		shared.Share();
		REQUIRE_NO_FAIL(shared[0].Query("INSERT INTO s VALUES (1)"));
		REQUIRE_FAIL(shared[1].Query("SELECT * FROM does_not_exist"));
		REQUIRE_FAIL(shared[0].Query("INSERT INTO s VALUES (2)"));
		REQUIRE_FAIL(shared[2].Query("SELECT count(*) FROM s"));
		for (idx_t i = 0; i < 3; i++) {
			REQUIRE_NO_FAIL(shared[i].Query("ROLLBACK"));
		}
		Connection observer(*shared.db);
		REQUIRE(Scalar(observer, "SELECT count(*) FROM s") == 0);
	}
	SECTION("connections that did not join keep their own transaction") {
		SharedTransaction shared(2);
		REQUIRE_NO_FAIL(shared[0].Query("CREATE TABLE s(a BIGINT)"));
		shared.Share();
		REQUIRE_NO_FAIL(shared[0].Query("INSERT INTO s VALUES (1)"));
		Connection observer(*shared.db);
		REQUIRE_NO_FAIL(observer.Query("BEGIN TRANSACTION"));
		REQUIRE(Scalar(observer, "SELECT count(*) FROM s") == 0);
		REQUIRE_NO_FAIL(shared[1].Query("INSERT INTO s VALUES (2)"));
		REQUIRE(Scalar(observer, "SELECT count(*) FROM s") == 0);
		REQUIRE_NO_FAIL(observer.Query("COMMIT"));
		shared.Commit();
		REQUIRE(Scalar(observer, "SELECT count(*) FROM s") == 2);
	}
	SECTION("the transaction id is shared, the connection and query ids are not") {
		SharedTransaction shared(2);
		shared.Share();
		REQUIRE(Scalar(shared[0], "SELECT current_transaction_id()") ==
		        Scalar(shared[1], "SELECT current_transaction_id()"));
		REQUIRE(Scalar(shared[0], "SELECT current_connection_id()") !=
		        Scalar(shared[1], "SELECT current_connection_id()"));
		REQUIRE(Scalar(shared[0], "SELECT current_query_id()") != Scalar(shared[1], "SELECT current_query_id()"));
		// sharing again returns the same identifier, also from a joined connection
		REQUIRE(shared[1].Query("SELECT share_transaction()")->Collection().GetValue(0, 0).ToString() ==
		        shared.share_id);
		shared.Commit();
	}
}

#endif
