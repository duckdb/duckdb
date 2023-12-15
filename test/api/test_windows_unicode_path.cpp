#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "duckdb/main/db_instance_cache.hpp"

using namespace duckdb;
using namespace std;

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
	TestCreateDirectory(test_directory);
	TestChangeDirectory(test_directory);

	// relative path INSIDE folder with accents
	TestConnectToDatabase("test.db", true);

	TestChangeDirectory("..");
	// relative path TOWARDS folder with accents
	TestConnectToDatabase(dirname + "/" + "test.db");

	// absolute path with folder with accents
	TestConnectToDatabase(current_directory + "/" + test_directory + "/" + "test.db");

	// revert current working directory
	TestChangeDirectory(current_directory);
}
