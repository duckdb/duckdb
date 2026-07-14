#include "capi_tester.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/main/capi/capi_internal.hpp"

using namespace duckdb;

TEST_CASE("Test duckdb_error_data ExtraInfo with no entries", "[capi]") {
	auto error_data = duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, "plain error");
	REQUIRE(error_data != nullptr);
	REQUIRE(duckdb_error_data_has_error(error_data));
	REQUIRE(duckdb_error_data_extra_info_count(error_data) == 0);
	REQUIRE(duckdb_error_data_extra_info_get(error_data, "status_code") == nullptr);
	REQUIRE(duckdb_error_data_extra_info_key(error_data, 0) == nullptr);
	REQUIRE(duckdb_error_data_extra_info_value(error_data, 0) == nullptr);

	duckdb_destroy_error_data(&error_data);
	REQUIRE(error_data == nullptr);

	REQUIRE(duckdb_error_data_extra_info_count(nullptr) == 0);
	REQUIRE(duckdb_error_data_extra_info_get(nullptr, "status_code") == nullptr);
	REQUIRE(duckdb_error_data_extra_info_get(nullptr, nullptr) == nullptr);
}

TEST_CASE("Test duckdb_error_data ExtraInfo HTTP status_code", "[capi]") {
	unordered_map<string, string> headers;
	headers["content-type"] = "application/json";

	auto wrapper = new ErrorDataWrapper();
	try {
		throw HTTPException(409, R"({"error":"conflict"})", headers, "Conflict", "commit failed");
	} catch (std::exception &ex) {
		wrapper->error_data = ErrorData(ex);
	}
	auto error_data = reinterpret_cast<duckdb_error_data>(wrapper);

	REQUIRE(duckdb_error_data_error_type(error_data) == DUCKDB_ERROR_HTTP);
	REQUIRE(duckdb_error_data_extra_info_count(error_data) >= 3);

	auto status_code = duckdb_error_data_extra_info_get(error_data, "status_code");
	REQUIRE(status_code != nullptr);
	REQUIRE(string(status_code) == "409");

	auto reason = duckdb_error_data_extra_info_get(error_data, "reason");
	REQUIRE(reason != nullptr);
	REQUIRE(string(reason) == "Conflict");

	auto response_body = duckdb_error_data_extra_info_get(error_data, "response_body");
	REQUIRE(response_body != nullptr);
	REQUIRE(string(response_body) == R"({"error":"conflict"})");

	auto header = duckdb_error_data_extra_info_get(error_data, "header_content-type");
	REQUIRE(header != nullptr);
	REQUIRE(string(header) == "application/json");

	idx_t count = duckdb_error_data_extra_info_count(error_data);
	bool found_status = false;
	for (idx_t i = 0; i < count; i++) {
		auto key = duckdb_error_data_extra_info_key(error_data, i);
		auto value = duckdb_error_data_extra_info_value(error_data, i);
		REQUIRE(key != nullptr);
		REQUIRE(value != nullptr);
		if (string(key) == "status_code") {
			REQUIRE(string(value) == "409");
			found_status = true;
		}
	}
	REQUIRE(found_status);
	REQUIRE(duckdb_error_data_extra_info_key(error_data, count) == nullptr);
	REQUIRE(duckdb_error_data_extra_info_value(error_data, count) == nullptr);

	duckdb_destroy_error_data(&error_data);
}

TEST_CASE("Test duckdb_result_error_data exposes ExtraInfo", "[capi]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));

	auto ok_result = tester.Query("SELECT 1");
	REQUIRE_NO_FAIL(*ok_result);
	REQUIRE(duckdb_result_error_data(&ok_result->InternalResult()) == nullptr);

	auto result = tester.Query("SELECT * FROM nonexistent_table_for_extra_info");
	REQUIRE(result->HasError());
	REQUIRE(result->ErrorType() == DUCKDB_ERROR_CATALOG);

	auto error_data = duckdb_result_error_data(&result->InternalResult());
	REQUIRE(error_data != nullptr);
	REQUIRE(duckdb_error_data_has_error(error_data));
	REQUIRE(duckdb_error_data_error_type(error_data) == DUCKDB_ERROR_CATALOG);

	auto subtype = duckdb_error_data_extra_info_get(error_data, "error_subtype");
	REQUIRE(subtype != nullptr);
	REQUIRE(string(subtype) == "MISSING_ENTRY");

	auto name = duckdb_error_data_extra_info_get(error_data, "name");
	REQUIRE(name != nullptr);
	REQUIRE(string(name) == "nonexistent_table_for_extra_info");

	REQUIRE(duckdb_error_data_extra_info_count(error_data) > 0);

	duckdb_destroy_error_data(&error_data);
	REQUIRE(duckdb_result_error_data(nullptr) == nullptr);
}
