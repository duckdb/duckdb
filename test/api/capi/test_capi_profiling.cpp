#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

string BuildSettingsString(const duckdb::vector<string> &settings) {
	string result = "'{";
	for (idx_t i = 0; i < settings.size(); i++) {
		result += "\"" + settings[i] + "\": \"true\"";
		if (i < settings.size() - 1) {
			result += ", ";
		}
	}
	result += "}'";
	return result;
}

void RetrieveMetrics(duckdb_profiling_info info, duckdb::map<string, double> &cumulative_counter,
                     duckdb::map<string, double> &cumulative_result) {
	auto map = duckdb_profiling_info_get_metrics(info);
	auto count = duckdb_get_map_size(map);
	for (idx_t i = 0; i < count; i++) {
		auto key = duckdb_get_map_key(map, i);
		auto value = duckdb_get_map_value(map, i);

		auto key_c_str = duckdb_get_varchar(key);
		auto value_c_str = duckdb_get_varchar(value);

		auto key_str = duckdb::string(key_c_str);
		auto value_str = duckdb::string(value_c_str);

		if (key_str == EnumUtil::ToString(MetricsType::QUERY_NAME) ||
		    key_str == EnumUtil::ToString(MetricsType::OPERATOR_TYPE) ||
		    key_str == EnumUtil::ToString(MetricsType::EXTRA_INFO)) {
			REQUIRE(!value_str.empty());
		} else {
			double result = 0;
			try {
				result = std::stod(value_str);
			} catch (std::invalid_argument &e) {
				REQUIRE(false);
			}

			if (cumulative_counter.find(key_str) != cumulative_counter.end()) {
				cumulative_counter[key_str] += result;
			}
			if (cumulative_result.find(key_str) != cumulative_result.end() && cumulative_result[key_str] == 0) {
				cumulative_result[key_str] = result;
			}
		}

		duckdb_destroy_value(&key);
		duckdb_destroy_value(&value);
		duckdb_free(key_c_str);
		duckdb_free(value_c_str);
	}

	duckdb_destroy_value(&map);
}

void TraverseTree(duckdb_profiling_info profiling_info, duckdb::map<string, double> &cumulative_counter,
                  duckdb::map<string, double> &cumulative_result) {

	RetrieveMetrics(profiling_info, cumulative_counter, cumulative_result);

	// Recurse into the child node.
	auto child_count = duckdb_profiling_info_get_child_count(profiling_info);
	for (idx_t i = 0; i < child_count; i++) {
		auto child = duckdb_profiling_info_get_child(profiling_info, i);
		TraverseTree(child, cumulative_counter, cumulative_result);
	}
}

idx_t ConvertToInt(double value) {
	return idx_t(value * 1000);
}

TEST_CASE("Test profiling with a single metric and get_value", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	REQUIRE(tester.OpenDatabase(nullptr));

	REQUIRE_NO_FAIL(tester.Query("PRAGMA enable_profiling = 'no_output'"));

	// test only CPU_TIME profiling
	duckdb::vector<string> settings = {"CPU_TIME"};
	REQUIRE_NO_FAIL(tester.Query("PRAGMA custom_profiling_settings=" + BuildSettingsString(settings)));
	REQUIRE_NO_FAIL(tester.Query("SELECT 42"));

	auto info = duckdb_get_profiling_info(tester.connection);
	REQUIRE(info != nullptr);
	// Retrieve a metric that is not enabled.
	REQUIRE(duckdb_profiling_info_get_value(info, "EXTRA_INFO") == nullptr);

	duckdb::map<string, double> cumulative_counter;
	duckdb::map<string, double> cumulative_result;

	TraverseTree(info, cumulative_counter, cumulative_result);
	tester.Cleanup();
}

TEST_CASE("Test profiling with cumulative metrics", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	REQUIRE(tester.OpenDatabase(nullptr));

	REQUIRE_NO_FAIL(tester.Query("PRAGMA enable_profiling = 'no_output'"));

	// test all profiling metrics
	duckdb::vector<string> settings = {"BLOCKED_THREAD_TIME",  "CPU_TIME",       "CUMULATIVE_CARDINALITY", "EXTRA_INFO",
	                                   "OPERATOR_CARDINALITY", "OPERATOR_TIMING"};
	REQUIRE_NO_FAIL(tester.Query("PRAGMA custom_profiling_settings=" + BuildSettingsString(settings)));
	REQUIRE_NO_FAIL(tester.Query("SELECT 42"));

	auto info = duckdb_get_profiling_info(tester.connection);
	REQUIRE(info != nullptr);

	duckdb::map<string, double> cumulative_counter = {{"OPERATOR_TIMING", 0}, {"OPERATOR_CARDINALITY", 0}};
	duckdb::map<string, double> cumulative_result {
	    {"CPU_TIME", 0},
	    {"CUMULATIVE_CARDINALITY", 0},
	};

	TraverseTree(info, cumulative_counter, cumulative_result);

	REQUIRE(ConvertToInt(cumulative_result["CPU_TIME"]) == ConvertToInt(cumulative_counter["OPERATOR_TIMING"]));
	REQUIRE(ConvertToInt(cumulative_result["CUMULATIVE_CARDINALITY"]) ==
	        ConvertToInt(cumulative_counter["OPERATOR_CARDINALITY"]));
	tester.Cleanup();
}

TEST_CASE("Test profiling without profiling enabled", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	REQUIRE(tester.OpenDatabase(nullptr));

	// Retrieve info without profiling enabled.
	auto info = duckdb_get_profiling_info(tester.connection);
	REQUIRE(info == nullptr);
	tester.Cleanup();
}

TEST_CASE("Test profiling with detailed profiling mode enabled", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	REQUIRE(tester.OpenDatabase(nullptr));

	REQUIRE_NO_FAIL(tester.Query("PRAGMA enable_profiling = 'no_output'"));
	REQUIRE_NO_FAIL(tester.Query("PRAGMA profiling_mode = 'detailed'"));
	REQUIRE_NO_FAIL(tester.Query("SELECT 42"));

	auto info = duckdb_get_profiling_info(tester.connection);
	REQUIRE(info != nullptr);

	duckdb::map<string, double> cumulative_counter;
	duckdb::map<string, double> cumulative_result;
	TraverseTree(info, cumulative_counter, cumulative_result);
	tester.Cleanup();
}

TEST_CASE("Test invalid use of profiling API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	REQUIRE(tester.OpenDatabase(nullptr));

	REQUIRE_NO_FAIL(tester.Query("PRAGMA enable_profiling = 'no_output'"));
	REQUIRE_NO_FAIL(tester.Query("PRAGMA profiling_mode = 'detailed'"));
	REQUIRE_NO_FAIL(tester.Query("SELECT 42"));

	auto info = duckdb_get_profiling_info(tester.connection);
	REQUIRE(info != nullptr);

	// Incorrect usage tests.

	auto map = duckdb_profiling_info_get_metrics(nullptr);
	REQUIRE(map == nullptr);
	map = duckdb_profiling_info_get_metrics(info);

	auto dummy_value = duckdb_create_bool(true);
	auto count = duckdb_get_map_size(nullptr);
	REQUIRE(count == 0);
	count = duckdb_get_map_size(dummy_value);
	REQUIRE(count == 0);
	count = duckdb_get_map_size(map);

	for (idx_t i = 0; i < count; i++) {
		auto key = duckdb_get_map_key(nullptr, i);
		REQUIRE(key == nullptr);
		key = duckdb_get_map_key(map, DConstants::INVALID_INDEX);
		REQUIRE(key == nullptr);
		key = duckdb_get_map_key(dummy_value, i);
		REQUIRE(key == nullptr);

		auto value = duckdb_get_map_value(nullptr, i);
		REQUIRE(value == nullptr);
		value = duckdb_get_map_value(map, DConstants::INVALID_INDEX);
		REQUIRE(value == nullptr);
		value = duckdb_get_map_value(dummy_value, i);
		REQUIRE(value == nullptr);
		break;
	}

	duckdb_destroy_value(&dummy_value);
	duckdb_destroy_value(&map);
	tester.Cleanup();
}
