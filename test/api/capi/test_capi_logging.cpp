#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

unordered_set<string> LOG_STORE;

void WriteLogEntry(duckdb_timestamp timestamp, const char *level, const char *log_type, const char *log_message) {
	LOG_STORE.insert(log_message);
	std::cout << "Writing log entry: " << log_message << std::endl;
}

TEST_CASE("Test pluggable log storage in CAPI", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));

	auto storage = duckdb_create_log_storage();
	duckdb_log_storage_set_write_log_entry(storage, WriteLogEntry);

	duckdb_register_log_storage(tester.database, "MyCustomStorage", storage);

	REQUIRE_NO_FAIL(tester.Query("set enable_logging=true;"));
	REQUIRE_NO_FAIL(tester.Query("set logging_storage='MyCustomStorage';"));

	REQUIRE_NO_FAIL(tester.Query("select write_log('HELLO, BRO');"));
	// REQUIRE(storage.log_store.find("HELLO, BRO") != storage.log_store.end());

	duckdb_destroy_log_storage(storage);
}
