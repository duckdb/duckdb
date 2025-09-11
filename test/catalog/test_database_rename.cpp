#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/database_manager.hpp"

using namespace duckdb;

TEST_CASE("Test database rename functionality", "[catalog]") {
	DuckDB db(":memory:");
	Connection con(db);

	// Attach a database
	REQUIRE_NO_FAIL(con.Query("ATTACH ':memory:' AS test_db"));

	// Create some data
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test_db.sample AS SELECT i FROM range(100) t(i)"));

	// Verify original name works
	auto result = con.Query("SELECT COUNT(*) FROM test_db.sample");
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 100);

	// Rename the database
	auto &db_manager = DatabaseManager::Get(*con.context);
	REQUIRE_NOTHROW(db_manager.RenameDatabase(*con.context, "test_db", "renamed_db", OnEntryNotFound::THROW_EXCEPTION));

	// Verify new name works
	result = con.Query("SELECT COUNT(*) FROM renamed_db.sample");
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 100);

	// Verify old name no longer works
	REQUIRE_FAIL(con.Query("SELECT COUNT(*) FROM test_db.sample"));

	// Test renaming non-existent database
	REQUIRE_THROWS(
	    db_manager.RenameDatabase(*con.context, "nonexistent", "new_name", OnEntryNotFound::THROW_EXCEPTION));

	// Test renaming with if_not_found = RETURN_NULL
	REQUIRE_NOTHROW(db_manager.RenameDatabase(*con.context, "nonexistent", "new_name", OnEntryNotFound::RETURN_NULL));

	// Test renaming to existing name
	REQUIRE_NO_FAIL(con.Query("ATTACH ':memory:' AS another_db"));
	REQUIRE_THROWS(
	    db_manager.RenameDatabase(*con.context, "renamed_db", "another_db", OnEntryNotFound::THROW_EXCEPTION));

	// Test renaming to reserved name
	REQUIRE_THROWS(db_manager.RenameDatabase(*con.context, "renamed_db", "system", OnEntryNotFound::THROW_EXCEPTION));
	REQUIRE_THROWS(db_manager.RenameDatabase(*con.context, "renamed_db", "temp", OnEntryNotFound::THROW_EXCEPTION));
}

TEST_CASE("Test ALTER DATABASE SQL syntax", "[catalog]") {
	DuckDB db(":memory:");
	Connection con(db);

	// Attach a database
	REQUIRE_NO_FAIL(con.Query("ATTACH ':memory:' AS test_db"));

	// Create some data
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test_db.sample AS SELECT i FROM range(100) t(i)"));

	// Verify original name works
	auto result = con.Query("SELECT COUNT(*) FROM test_db.sample");
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 100);

	// Rename using SQL ALTER DATABASE
	REQUIRE_NO_FAIL(con.Query("ALTER DATABASE test_db RENAME TO renamed_db"));

	// Verify new name works
	result = con.Query("SELECT COUNT(*) FROM renamed_db.sample");
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 100);

	// Verify old name no longer works
	REQUIRE_FAIL(con.Query("SELECT COUNT(*) FROM test_db.sample"));

	// Test ALTER DATABASE with non-existent database
	REQUIRE_FAIL(con.Query("ALTER DATABASE nonexistent RENAME TO new_name"));

	// Test ALTER DATABASE IF EXISTS with non-existent database
	REQUIRE_NO_FAIL(con.Query("ALTER DATABASE IF EXISTS nonexistent RENAME TO new_name"));
}
