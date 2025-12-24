#include "capi_tester.hpp"
#include "duckdb.h"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Simple In-Memory DB Start Up and Shutdown", "[simplestartup]") {
	duckdb_database database;
	duckdb_connection connection;

	// open and close a database in in-memory mode
	REQUIRE(duckdb_open(NULL, &database) == DuckDBSuccess);
	REQUIRE(duckdb_connect(database, &connection) == DuckDBSuccess);
	duckdb_disconnect(&connection);
	duckdb_close(&database);
}

TEST_CASE("Multiple In-Memory DB Start Up and Shutdown", "[multiplestartup]") {
	duckdb_database database[10];
	duckdb_connection connection[100];

	// open and close 10 databases
	// and open 10 connections per database
	for (size_t i = 0; i < 10; i++) {
		REQUIRE(duckdb_open(NULL, &database[i]) == DuckDBSuccess);
		for (size_t j = 0; j < 10; j++) {
			REQUIRE(duckdb_connect(database[i], &connection[i * 10 + j]) == DuckDBSuccess);
		}
	}
	for (size_t i = 0; i < 10; i++) {
		for (size_t j = 0; j < 10; j++) {
			duckdb_disconnect(&connection[i * 10 + j]);
		}
		duckdb_close(&database[i]);
	}
}

TEST_CASE("On Disk DB File Name Case Preserved", "[simplestartup]") {
	// Check that file on disk is created in user-specified case
	// independently whether FS is case-sensitive or not.
	// FS access can be eventually changed to CAPI FS.
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	string work_dir = TestDirectoryPath();
	std::vector<string> db_file_list = {"Test1.db", "TEST2.db", "TeSt3.db"};
	for (const string &db_file : db_file_list) {
		string db_path = TestJoinPath(work_dir, db_file);
		fs->TryRemoveFile(db_path);
		{
			duckdb_database database = nullptr;
			REQUIRE(duckdb_open(db_path.c_str(), &database) == DuckDBSuccess);
			duckdb_connection conn = nullptr;
			REQUIRE(duckdb_connect(database, &conn) == DuckDBSuccess);
			REQUIRE(duckdb_query(conn, "CREATE TABLE tab1 (col1 INT)", nullptr) == DuckDBSuccess);
			duckdb_disconnect(&conn);
			duckdb_close(&database);
		}
		bool found = false;
		fs->ListFiles(work_dir, [&db_file, &found](const string &name, bool) {
			if (name == db_file) {
				found = true;
			}
		});
		REQUIRE(found);
		{
			// Open a connection to a lower-cased version of the path
			string db_file_lower = db_file;
			std::transform(db_file_lower.begin(), db_file_lower.end(), db_file_lower.begin(),
			               [](unsigned char c) { return tolower(c); });
			string db_path_lower = TestJoinPath(work_dir, db_file_lower);
			duckdb_database database;
			REQUIRE(duckdb_open(db_path_lower.c_str(), &database) == DuckDBSuccess);
			// Run a query to check that we've opened the existing DB file or created a new one
			// depending whether the FS is case-sensitive or not
			duckdb_connection conn = nullptr;
			REQUIRE(duckdb_connect(database, &conn) == DuckDBSuccess);
			duckdb_result result;
			memset(&result, '\0', sizeof(result));
			REQUIRE(duckdb_query(conn, "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'tab1'",
			                     &result) == DuckDBSuccess);
			duckdb_data_chunk chunk = duckdb_fetch_chunk(result);
			REQUIRE(chunk != nullptr);
			duckdb_vector vec = duckdb_data_chunk_get_vector(chunk, 0);
			REQUIRE(vec != nullptr);
			int32_t *data = reinterpret_cast<int32_t *>(duckdb_vector_get_data(vec));
			int32_t count = data[0];
#if defined(_WIN32) || defined(__APPLE__) // case insensitive, same file
			REQUIRE(count == 1);
#else  // !(_WIN32 or __APPLE__): case sensitive, different files
			REQUIRE(count == 0);
#endif // _WIN32 or __APPLE__
			duckdb_destroy_data_chunk(&chunk);
			duckdb_destroy_result(&result);
			duckdb_disconnect(&conn);
			duckdb_close(&database);
			if (count == 0) {
				fs->TryRemoveFile(db_path_lower);
			}
		}
		fs->TryRemoveFile(db_path);
	}
}
