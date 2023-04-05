#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

struct MyBaseNumber {
	int number;
};

void destroy_base_number(void *data) {
	auto num = (MyBaseNumber *)data;
	delete num;
}

void number_scanner(duckdb_replacement_scan_info info, const char *table_name, void *data) {
	// check if the table name is a number
	long long number;
	try {
		number = std::stoll(table_name);
	} catch (...) {
		// not a number!
		return;
	}
	auto num_data = (MyBaseNumber *)data;
	duckdb_replacement_scan_set_function_name(info, "range");
	auto val = duckdb_create_int64(number + num_data->number);
	duckdb_replacement_scan_add_parameter(info, val);
	duckdb_destroy_value(&val);
}

TEST_CASE("Test replacement scans in C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	auto base_number = new MyBaseNumber();
	base_number->number = 3;

	duckdb_add_replacement_scan(tester.database, number_scanner, (void *)base_number, destroy_base_number);

	// 0-4
	result = tester.Query("SELECT * FROM \"2\"");
	REQUIRE(result->row_count() == 5);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 0);
	REQUIRE(result->Fetch<int64_t>(0, 1) == 1);
	REQUIRE(result->Fetch<int64_t>(0, 2) == 2);
	REQUIRE(result->Fetch<int64_t>(0, 3) == 3);
	REQUIRE(result->Fetch<int64_t>(0, 4) == 4);

	base_number->number = 1;
	// 0-2
	result = tester.Query("SELECT * FROM \"2\"");
	REQUIRE(result->row_count() == 3);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 0);
	REQUIRE(result->Fetch<int64_t>(0, 1) == 1);
	REQUIRE(result->Fetch<int64_t>(0, 2) == 2);

	// not a number
	REQUIRE_FAIL(tester.Query("SELECT * FROM nonexistant"));
}

void error_replacement_scan(duckdb_replacement_scan_info info, const char *table_name, void *data) {
	duckdb_replacement_scan_set_error(NULL, NULL);
	duckdb_replacement_scan_set_error(info, NULL);
	duckdb_replacement_scan_set_error(info, "error in replacement scan");
}

TEST_CASE("Test error replacement scan", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	duckdb_add_replacement_scan(tester.database, error_replacement_scan, NULL, NULL);

	// error
	REQUIRE_FAIL(tester.Query("SELECT * FROM nonexistant"));
}
