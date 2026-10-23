#include "catch.hpp"
#include "duckdb.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <thread>

using namespace duckdb;

TEST_CASE("An update racing the rollback of a vector's writer leaves no ids of the rolled back writer",
          "[persistence]") {
	auto path = TestCreatePath("rollback_update_race.db");
	for (idx_t attempt = 0; attempt < 3; attempt++) {
		DeleteDatabase(path);
		DuckDB db(path);
		Connection con(db), con1(db), con2(db);
		REQUIRE_NO_FAIL(con.Query("SET debug_skip_checkpoint_on_commit = true"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT i AS id, 0 AS a FROM range(2048) t(i)"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE w AS SELECT i AS id FROM range(1000000) t(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		// con1 writes all rows of the vector in two updates, with many undo entries in between to widen the rollback
		REQUIRE_NO_FAIL(con1.Query("BEGIN"));
		REQUIRE_NO_FAIL(con1.Query("UPDATE t SET a = 1 WHERE id < 1000"));
		REQUIRE_NO_FAIL(con1.Query("DELETE FROM w WHERE id % 2 = 0"));
		REQUIRE_NO_FAIL(con1.Query("UPDATE t SET a = 1 WHERE id >= 1000"));

		// con2 updates a row as soon as the rollback of the second update releases it
		std::atomic<bool> rolled_back {false};
		bool rollback_failed = false;
		std::thread rollback([&]() {
			rollback_failed = con1.Query("ROLLBACK")->HasError();
			rolled_back = true;
		});
		bool updated = false;
		while (!updated && !rolled_back) {
			updated = !con2.Query("UPDATE t SET a = 2 WHERE id = 1500")->HasError();
		}
		rollback.join();
		REQUIRE(!rollback_failed);
		if (!updated) {
			REQUIRE_NO_FAIL(con2.Query("UPDATE t SET a = 2 WHERE id = 1500"));
		}

		// once the update is compressed away, the rows hold no id of the rolled back transaction
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		REQUIRE_NO_FAIL(con.Query("UPDATE t SET a = 3 WHERE id = 5"));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM t WHERE id = 6"));
		auto result = con.Query("SELECT count(*), sum(a) FROM t");
		REQUIRE(CHECK_COLUMN(result, 0, {2047}));
		REQUIRE(CHECK_COLUMN(result, 1, {5}));
	}
}
