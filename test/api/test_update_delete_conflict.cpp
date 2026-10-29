#include "catch.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <thread>

using namespace duckdb;

// An UPDATE that touches an indexed column (or uses RETURNING) runs as DELETE + INSERT, any other UPDATE is applied
// in place. Concurrent writes to the same row through the two paths must conflict instead of losing one of them.
namespace {

void ExpectConflict(Connection &con, const string &query) {
	auto result = con.Query(query);
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "Conflict"));
}

void CheckValue(Connection &con, int32_t expected) {
	auto result = con.Query("SELECT v FROM t WHERE id = 1");
	REQUIRE(CHECK_COLUMN(result, 0, {expected}));
}

} // namespace

TEST_CASE("Concurrent in-place and delete+insert updates of the same row conflict", "[transactions]") {
	DuckDB db(nullptr);
	Connection setup(db), con1(db), con2(db);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO t VALUES (1, 0)"));

	const string plain = "UPDATE t SET v = %d WHERE id = 1";
	const string returning = "UPDATE t SET v = %d WHERE id = 1 RETURNING v";
	const string pk_update = "UPDATE t SET id = 1, v = %d WHERE id = 1";
	const string del = "DELETE FROM t WHERE id = 1";
	struct Interleaving {
		string first;
		string second;
		bool conflicts;
	};
	// a plain DELETE does not conflict with an earlier in-place update: the delete wins
	vector<Interleaving> interleavings {
	    {returning, plain, true}, {plain, returning, true}, {pk_update, plain, true},     {plain, pk_update, true},
	    {del, plain, true},       {plain, del, false},      {returning, returning, true}, {plain, plain, true}};
	int32_t value = 0;
	for (auto &interleaving : interleavings) {
		INFO(interleaving.first << " then " << interleaving.second);
		REQUIRE_NO_FAIL(setup.Query("INSERT OR IGNORE INTO t VALUES (1, 0)"));
		REQUIRE_NO_FAIL(con1.Query("BEGIN"));
		REQUIRE_NO_FAIL(con2.Query("BEGIN"));
		value++;
		auto first_value = value;
		REQUIRE_NO_FAIL(con1.Query(StringUtil::Format(interleaving.first, first_value)));
		value++;
		if (!interleaving.conflicts) {
			REQUIRE_NO_FAIL(con2.Query(StringUtil::Format(interleaving.second, value)));
			REQUIRE_NO_FAIL(con2.Query("COMMIT"));
			REQUIRE_NO_FAIL(con1.Query("COMMIT"));
			auto result = setup.Query("SELECT count(*) FROM t");
			REQUIRE(CHECK_COLUMN(result, 0, {0}));
			continue;
		}
		ExpectConflict(con2, StringUtil::Format(interleaving.second, value));
		REQUIRE_NO_FAIL(con2.Query("ROLLBACK"));
		REQUIRE_NO_FAIL(con1.Query("COMMIT"));
		if (interleaving.first == del) {
			auto result = setup.Query("SELECT count(*) FROM t");
			REQUIRE(CHECK_COLUMN(result, 0, {0}));
		} else {
			CheckValue(setup, first_value);
		}
	}

	// a transaction that started before a committed delete+insert cannot update the old row version
	REQUIRE_NO_FAIL(con1.Query("BEGIN"));
	REQUIRE_NO_FAIL(con1.Query("SELECT * FROM t"));
	REQUIRE_NO_FAIL(con2.Query("UPDATE t SET v = 100 WHERE id = 1 RETURNING v"));
	ExpectConflict(con1, "UPDATE t SET v = 101 WHERE id = 1");
	REQUIRE_NO_FAIL(con1.Query("ROLLBACK"));
	CheckValue(setup, 100);

	// and vice versa: an in-place update committed after the transaction started blocks its delete
	REQUIRE_NO_FAIL(con1.Query("BEGIN"));
	REQUIRE_NO_FAIL(con1.Query("SELECT * FROM t"));
	REQUIRE_NO_FAIL(con2.Query("UPDATE t SET v = 200 WHERE id = 1"));
	ExpectConflict(con1, "UPDATE t SET v = 201 WHERE id = 1 RETURNING v");
	REQUIRE_NO_FAIL(con1.Query("ROLLBACK"));
	CheckValue(setup, 200);
}

TEST_CASE("Concurrent increments through in-place and delete+insert updates lose no writes", "[transactions]") {
	DuckDB db(nullptr);
	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO t VALUES (1, 0)"));

	constexpr idx_t THREAD_COUNT = 4;
	constexpr idx_t ITERATIONS = 200;
	std::atomic<idx_t> committed {0};
	std::atomic<idx_t> unexpected_errors {0};
	vector<std::thread> threads;
	for (idx_t thread_idx = 0; thread_idx < THREAD_COUNT; thread_idx++) {
		threads.emplace_back([&, thread_idx]() {
			Connection con(db);
			// half of the threads update in place, the other half through DELETE + INSERT
			auto query = thread_idx % 2 == 0 ? "UPDATE t SET v = v + 1 WHERE id = 1"
			                                 : "UPDATE t SET v = v + 1 WHERE id = 1 RETURNING v";
			for (idx_t i = 0; i < ITERATIONS; i++) {
				if (con.Query("BEGIN")->HasError()) {
					unexpected_errors++;
					return;
				}
				auto result = con.Query(query);
				if (!result->HasError()) {
					result = con.Query("COMMIT");
				}
				if (!result->HasError()) {
					committed++;
					continue;
				}
				if (!StringUtil::Contains(result->GetError(), "Conflict")) {
					unexpected_errors++;
				}
				con.Query("ROLLBACK");
			}
		});
	}
	for (auto &thread : threads) {
		thread.join();
	}
	REQUIRE(unexpected_errors == 0);
	REQUIRE(committed > 0);
	CheckValue(setup, NumericCast<int32_t>(committed.load()));
}
