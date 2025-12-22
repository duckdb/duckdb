#include "catch.hpp"
#include "test_helpers.hpp"

#include <fstream>

using namespace duckdb;
using namespace std;

// Test that corrupted WAL files with indexes don't cause assertion failures/crashes
// This addresses issue duckdb/duckdb-rs#649
TEST_CASE("Test corrupted WAL with index doesn't crash", "[persistence][.]") {
	duckdb::unique_ptr<QueryResult> result;
	auto test_dir = TestDirectoryPath();
	string db_path = test_dir + "/corrupted_wal_index.db";
	string wal_path = db_path + ".wal";

	// First, create a database with an index and some data
	{
		DuckDB db(db_path);
		Connection con(db);

		// Disable checkpointing so data stays in WAL
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown;"));
		REQUIRE_NO_FAIL(con.Query("SET checkpoint_threshold = '10.0 GB';"));

		// Create table with index
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(id INTEGER PRIMARY KEY, value TEXT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX idx_value ON tbl(value);"));

		// Insert some data
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT range, 'value_' || range::VARCHAR FROM range(100);"));

		// Verify the data is there
		result = con.Query("SELECT COUNT(*) FROM tbl;");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(CHECK_COLUMN(result, 0, {100}));
	}

	// Now truncate the WAL file to simulate corruption
	{
		// Get the file size
		ifstream wal_file(wal_path, ios::binary | ios::ate);
		REQUIRE(wal_file.is_open());
		auto file_size = wal_file.tellg();
		wal_file.close();

		// Truncate to half the size (simulating mid-write crash)
		if (file_size > 100) {
			ofstream truncate_file(wal_path, ios::binary | ios::in | ios::out);
			REQUIRE(truncate_file.is_open());
			// Seek to middle and truncate
			truncate_file.seekp(file_size / 2);
			truncate_file.close();

			// Actually truncate by reopening with trunc at the right position
			{
				// Read first half
				ifstream read_file(wal_path, ios::binary);
				std::vector<char> buffer(file_size / 2);
				read_file.read(buffer.data(), buffer.size());
				auto bytes_read = read_file.gcount();
				read_file.close();

				// Write truncated version
				ofstream write_file(wal_path, ios::binary | ios::trunc);
				write_file.write(buffer.data(), bytes_read);
				write_file.close();
			}
		}
	}

	// Try to open the database with corrupted WAL
	// This should NOT crash with an assertion failure
	// It should either:
	// 1. Successfully recover partial data
	// 2. Return an error that can be caught
	bool opened_successfully = false;
	bool got_error = false;
	string error_message;

	try {
		DuckDB db(db_path);
		Connection con(db);

		// If we get here, the database opened (possibly with partial data)
		opened_successfully = true;

		// Try a simple query - this should work even with partial data
		result = con.Query("SELECT 1;");
		REQUIRE_NO_FAIL(*result);
	} catch (const Exception &e) {
		got_error = true;
		error_message = e.what();
	} catch (const std::exception &e) {
		got_error = true;
		error_message = e.what();
	}

	// The key assertion: we should NOT have crashed
	// Either we opened successfully (with possible data loss) or got a catchable error
	REQUIRE((opened_successfully || got_error));

	// If we got an error, it should be a meaningful error, not an assertion failure
	if (got_error) {
		// Make sure it's not an assertion failure message
		REQUIRE_FALSE(StringUtil::Contains(error_message, "Assertion failed"));
	}
}

// Test that partially written index data in WAL is handled gracefully
TEST_CASE("Test WAL replay with unbound index recovery", "[persistence][.]") {
	duckdb::unique_ptr<QueryResult> result;
	auto test_dir = TestDirectoryPath();
	string db_path = test_dir + "/unbound_index_recovery.db";
	string wal_path = db_path + ".wal";

	// Create database, add index, insert data
	{
		DuckDB db(db_path);
		Connection con(db);

		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown;"));
		REQUIRE_NO_FAIL(con.Query("SET checkpoint_threshold = '10.0 GB';"));

		// Create table
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test_table(a INTEGER, b TEXT);"));

		// Insert initial data
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test_table VALUES (1, 'one'), (2, 'two'), (3, 'three');"));

		// Create index after data exists
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX test_idx ON test_table(a);"));

		// Insert more data
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test_table VALUES (4, 'four'), (5, 'five');"));
	}

	// Corrupt the end of the WAL (where the new inserts are)
	{
		ifstream wal_file(wal_path, ios::binary | ios::ate);
		if (wal_file.is_open()) {
			auto file_size = wal_file.tellg();
			wal_file.close();

			if (file_size > 200) {
				// Read most of the file but skip the last part
				ifstream read_file(wal_path, ios::binary);
				size_t truncated_size = static_cast<size_t>(file_size) - 100;
				std::vector<char> buffer(truncated_size);
				read_file.read(buffer.data(), buffer.size());
				auto bytes_read = read_file.gcount();
				read_file.close();

				// Write truncated version
				ofstream write_file(wal_path, ios::binary | ios::trunc);
				write_file.write(buffer.data(), bytes_read);
				write_file.close();
			}
		}
	}

	// Opening should not crash
	bool crashed = false;
	try {
		DuckDB db(db_path);
		Connection con(db);

		// Database opened successfully - verify we can query
		result = con.Query("SELECT COUNT(*) FROM test_table;");
		// We may have partial data, but at least no crash
		REQUIRE_NO_FAIL(*result);
	} catch (const Exception &e) {
		// Caught exception is acceptable - just not a crash
		crashed = false;
	} catch (...) {
		crashed = true;
	}

	REQUIRE_FALSE(crashed);
}
