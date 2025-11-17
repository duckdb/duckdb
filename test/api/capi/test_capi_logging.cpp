#include "capi_tester.hpp"
#include "util/logging.h"

using namespace duckdb;
using namespace std;

unordered_set<string> LOG_STORE;

void WriteLogEntry(duckdb_timestamp timestamp, const char *level, const char *log_type, const char *log_message) {
	LOG_STORE.insert(log_message);
}

TEST_CASE("Test pluggable log storage in CAPI", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	LOG_STORE.clear();

	REQUIRE(tester.OpenDatabase(nullptr));

	auto storage = duckdb_create_log_storage();
	duckdb_log_storage_set_write_log_entry(storage, WriteLogEntry);

	duckdb_register_log_storage(tester.database, "MyCustomStorage", storage);

	REQUIRE_NO_FAIL(tester.Query("set enable_logging=true;"));
	REQUIRE_NO_FAIL(tester.Query("set logging_storage='MyCustomStorage';"));

	REQUIRE_NO_FAIL(tester.Query("select write_log('HELLO, BRO');"));
	REQUIRE(LOG_STORE.find("HELLO, BRO") != LOG_STORE.end());

	duckdb_destroy_log_storage(storage);
}

// Check that Fatal Error which is otherwise swallowed, is logged
TEST_CASE("Test logging errors using pluggable log storage in CAPI", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	LOG_STORE.clear();

	REQUIRE(tester.OpenDatabase(nullptr));

	auto storage = duckdb_create_log_storage();
	duckdb_log_storage_set_write_log_entry(storage, WriteLogEntry);

	duckdb_register_log_storage(tester.database, "MyCustomStorage", storage);

	REQUIRE_NO_FAIL(tester.Query("CALL enable_logging(level = 'error');"));
	REQUIRE_NO_FAIL(tester.Query("set logging_storage='MyCustomStorage';"));

	auto path = TestCreatePath("log_storage_test.db");
	REQUIRE_NO_FAIL(tester.Query("ATTACH IF NOT EXISTS '" + path + "' (TYPE DUCKDB)"));
	REQUIRE_NO_FAIL(tester.Query("PRAGMA wal_autocheckpoint='1TB';"));
	REQUIRE_NO_FAIL(tester.Query("PRAGMA debug_checkpoint_abort='before_header';"));
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE log_storage_test.integers AS SELECT * FROM range(100) tbl(i);"));
	REQUIRE_NO_FAIL(tester.Query("DETACH log_storage_test;"));

	duckdb_destroy_log_storage(storage);
}
