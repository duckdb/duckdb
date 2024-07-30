#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

string BuildProfilingSettingsString(const std::vector<string> &settings) {
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

void RetrieveAllMetrics(duckdb_profiling_info profiling_info, const std::vector<string> &settings) {
	for (idx_t i = 0; i < settings.size(); i++) {
		auto value = duckdb_profiling_info_get_value(profiling_info, settings[i].c_str());
		if (value != nullptr) {
			if (settings[i] == "EXTRA_INFO") {
				duckdb_destroy_value(&value);
				continue;
			}
			double result = 0;
			try {
				auto str_value = duckdb_get_varchar(value);
				result = std::stod(str_value);
				duckdb_free(str_value);
			} catch (std::invalid_argument &e) {
				REQUIRE(false);
			}

			duckdb_destroy_value(&value);
			REQUIRE(result >= 0);
		}
	}
}

// Traverse the tree and retrieve all metrics
void TraverseTree(duckdb_profiling_info profiling_info, const std::vector<string> &settings, bool is_root = true) {
	if (is_root) {
		// At the root, only the query name is available
		auto query = duckdb_profiling_info_get_query(profiling_info);
		REQUIRE(query != nullptr);
		duckdb_free((void *)query);

		REQUIRE(duckdb_profiling_info_get_name(profiling_info) == nullptr);
	} else {
		// At the child level, only the operator name is available
		auto name = duckdb_profiling_info_get_name(profiling_info);
		REQUIRE(name != nullptr);
		duckdb_free((void *)name);

		REQUIRE(duckdb_profiling_info_get_query(profiling_info) == nullptr);
	}

	RetrieveAllMetrics(profiling_info, settings);

	auto child_count = duckdb_profiling_info_get_child_count(profiling_info);
	for (idx_t i = 0; i < child_count; i++) {
		auto child = duckdb_profiling_info_get_child(profiling_info, i);
		TraverseTree(child, settings, false);
	}
}

TEST_CASE("Test Profiling with Single Metric", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	REQUIRE_NO_FAIL(tester.Query("PRAGMA enable_profiling = 'no_output'"));

	// test only CPU_TIME profiling
	std::vector<string> settings = {"CPU_TIME"};
	REQUIRE_NO_FAIL(tester.Query("PRAGMA custom_profiling_settings=" + BuildProfilingSettingsString(settings)));

	REQUIRE_NO_FAIL(tester.Query("SELECT 42"));

	auto profiling_info = duckdb_get_profiling_info(tester.connection);
	REQUIRE(profiling_info != nullptr);

	// Retrieve metric that is not enabled
	REQUIRE(duckdb_profiling_info_get_value(profiling_info, "EXTRA_INFO") == nullptr);

	TraverseTree(profiling_info, settings);

	// Cleanup
	tester.Cleanup();
}

TEST_CASE("Test Profiling with All Metrics", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	REQUIRE_NO_FAIL(tester.Query("PRAGMA enable_profiling = 'no_output'"));

	// test all profiling metrics
	std::vector<string> settings = {"CPU_TIME", "EXTRA_INFO", "OPERATOR_CARDINALITY", "OPERATOR_TIMING"};
	REQUIRE_NO_FAIL(tester.Query("PRAGMA custom_profiling_settings=" + BuildProfilingSettingsString(settings)));

	REQUIRE_NO_FAIL(tester.Query("SELECT 42"));

	auto profiling_info = duckdb_get_profiling_info(tester.connection);
	REQUIRE(profiling_info != nullptr);

	TraverseTree(profiling_info, settings);

	// Cleanup
	tester.Cleanup();
}

TEST_CASE("Test Profiling without Enabling Profiling", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	// Retrieve profiling info without enabling profiling
	auto profiling_info = duckdb_get_profiling_info(tester.connection);
	REQUIRE(profiling_info == nullptr);

	// Cleanup
	tester.Cleanup();
}
