#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "duckdb/main/db_instance_cache.hpp"

using namespace duckdb;

void TestConnectToDatabase(const string &path, bool create_table = false) {
	// connect to the database using the standard syntax
	{
		DuckDB db(path);
		Connection con(db);
		if (create_table) {
			REQUIRE_NO_FAIL(con.Query("CREATE TABLE test AS SELECT * FROM range(10) t(i)"));
		}

		auto result = con.Query("SELECT SUM(i) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {45}));
	}

	// connect to the database using the db instance cache
	{
		DBInstanceCache cache;
		DBConfig config;
		auto db = cache.CreateInstance(path, config);
		Connection con(*db);

		auto result = con.Query("SELECT SUM(i) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {45}));
	}
}

TEST_CASE("Issue #6931 - test windows unicode path", "[windows]") {
	string dirname = "Moseguí_i_González";
	auto test_directory = TestDirectoryPath() + "/" + dirname;
	auto current_directory = TestGetCurrentDirectory();
	// TestDirectoryPath() is absolute whenever the temp-dir-root is, so anchor rather than concatenate.
	auto absolute_directory = TestMakeAbsolute(test_directory, current_directory);
	TestCreateDirectory(test_directory);
	TestChangeDirectory(test_directory);

	// relative path INSIDE folder with accents
	TestConnectToDatabase("test.db", true);

	TestChangeDirectory("..");
	// relative path TOWARDS folder with accents
	TestConnectToDatabase(dirname + "/" + "test.db");

	// Restore before the last leg, which does not need the chdir: a throw while still inside the temp
	// dir leaves every later test resolving relative paths from there.
	TestChangeDirectory(current_directory);

	// absolute path with folder with accents
	TestConnectToDatabase(absolute_directory + "/" + "test.db");
}
