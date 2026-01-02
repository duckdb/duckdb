#include "catch.hpp"
#include "duckdb.hpp"
#include "test_helpers.hpp"

#include <sys/stat.h>

using namespace duckdb;
using namespace std;

TEST_CASE("Test optimistic insertion does not trigger a checkpoint", "[persistence]") {
	string test_dir = TestDirectoryPath();
	string db_path = test_dir + "/my_db.db";
	string wal_path = db_path + ".wal";

	DuckDB db(db_path);
	Connection con(db);

	// Configuration to lower the checkpoint_threshold
	REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown;"));
	REQUIRE_NO_FAIL(con.Query("SET checkpoint_threshold = '5 KB';"));

	// Create Table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1 (id INTEGER, c0 INTEGER);"));

	// Optimistic insertion of 122880 Rows
	REQUIRE_NO_FAIL(con.Query("BEGIN"));
	Appender appender(con, "t1");
	for (int i = 1; i <= (int)DEFAULT_ROW_GROUP_SIZE; i++) {
		appender.BeginRow();
		appender.Append<int>(i);
		appender.Append<int>(std::rand());
		appender.EndRow();
	}
	appender.Flush();
	REQUIRE_NO_FAIL(con.Query("COMMIT;"));

	// Optimistic insertions result in very small WAL log size, so checkpointing should not be performed. Here, we check
	// if the WAL file still exists.
	struct stat st;
	REQUIRE((::stat(wal_path.c_str(), &st) == 0 && S_ISREG(st.st_mode)) == true);
}
