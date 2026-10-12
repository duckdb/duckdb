#include "catch.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "test_helpers.hpp"

#include <thread>

using namespace duckdb;

// a NULL committed while ADD PRIMARY KEY builds its index must not end up in the key column; the race is retried
TEST_CASE("ADD PRIMARY KEY racing a concurrent NULL insert", "[api]") {
	auto attempts = 20;
	for (int attempt = 0; attempt < attempts; attempt++) {
		DuckDB db(nullptr);
		Connection setup(db);
		REQUIRE_NO_FAIL(setup.Query("CREATE TABLE t (id INTEGER)"));
		REQUIRE_NO_FAIL(setup.Query("INSERT INTO t SELECT i FROM range(1, 3000001) tbl(i)"));

		bool alter_succeeded = false;
		std::thread alter([&]() {
			Connection conn(db);
			alter_succeeded = !conn.Query("ALTER TABLE t ADD PRIMARY KEY (id)")->HasError();
		});
		std::thread insert([&]() {
			std::this_thread::sleep_for(std::chrono::milliseconds(attempt % 10));
			Connection conn(db);
			conn.Query("INSERT INTO t VALUES (NULL)");
		});
		alter.join();
		insert.join();

		if (alter_succeeded) {
			// the key was added, so no NULL may be in the column
			auto result = setup.Query("SELECT count(*) FROM t WHERE id IS NULL");
			REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == 0);
		}
	}
}
