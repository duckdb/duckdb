#include "catch.hpp"
#include "test_helpers.hpp"

#include <fstream>
#include <iostream>

using namespace duckdb;
using namespace std;

TEST_CASE("Test replaying mismatching WAL files", "[persistence][.]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// Configuration to avoid checkpointing.
	REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown;"));
	REQUIRE_NO_FAIL(con.Query("SET checkpoint_threshold = '10.0 GB';"));
	REQUIRE_NO_FAIL(con.Query("SET storage_compatibility_version='v1.4.0';"));
	REQUIRE_NO_FAIL(con.Query("SET threads = 1;"));

	auto test_dir = TestDirectoryPath();
	string db_name = "my_db";
	string db_path = test_dir + "/" + db_name + ".db";
	string wal_path = db_path + ".wal";

	// Create initial file and WAL.
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + db_path + "';"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE my_db.tbl AS SELECT range AS id FROM range(100);"));
	REQUIRE_NO_FAIL(con.Query("DETACH my_db;"));

	// Make a copy of the file and the WAL file.
	string too_new_path_file = test_dir + "/" + db_name + "_too_new.db";
	string too_old_path_wal = test_dir + "/" + db_name + "_too_old.db.wal";

	string copy_file_cmd = "cp " + db_path + " " + too_new_path_file;
	REQUIRE(system(copy_file_cmd.c_str()) == 0);
	string copy_wal_cmd = "cp " + wal_path + " " + too_old_path_wal;
	REQUIRE(system(copy_wal_cmd.c_str()) == 0);

	// Replay the WAL and make more changes.
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + db_path + "';"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT my_db;"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO my_db.tbl SELECT range + 400 FROM range(100);"));
	REQUIRE_NO_FAIL(con.Query("DETACH my_db;"));

	// Make a copy of the file and the WAL file.
	string too_old_path_file = test_dir + "/" + db_name + "_too_old.db";
	string too_new_path_wal = test_dir + "/" + db_name + "_too_new.db.wal";

	copy_file_cmd = "cp " + db_path + " " + too_old_path_file;
	REQUIRE(system(copy_file_cmd.c_str()) == 0);
	copy_wal_cmd = "cp " + wal_path + " " + too_new_path_wal;
	REQUIRE(system(copy_wal_cmd.c_str()) == 0);

	result = con.Query("ATTACH '" + too_old_path_file + "';");
	REQUIRE(result->HasError());
	string error_msg = result->GetError();
	REQUIRE(StringUtil::Contains(error_msg, "older"));

	result = con.Query("ATTACH '" + too_new_path_file + "';");
	REQUIRE(result->HasError());
	error_msg = result->GetError();
	REQUIRE(StringUtil::Contains(error_msg, "newer"));

	// Create and initialize a different file.
	string other_db_path = test_dir + "/my_other_db.db";
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + other_db_path + "';"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE my_other_db.tbl AS SELECT range AS id FROM range(100);"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT my_other_db;"));
	REQUIRE_NO_FAIL(con.Query("DETACH my_other_db;"));

	// Also copy this WAL for the file mismatch test.
	string other_path_wal = test_dir + "/my_other_db.db.wal";
	copy_wal_cmd = "cp " + wal_path + " " + other_path_wal;
	REQUIRE(system(copy_wal_cmd.c_str()) == 0);

	result = con.Query("ATTACH '" + other_db_path + "';");
	REQUIRE(result->HasError());
	error_msg = result->GetError();
	REQUIRE(StringUtil::Contains(error_msg, "WAL does not match database file"));
}
