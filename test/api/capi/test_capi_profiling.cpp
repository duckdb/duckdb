#include "iostream"

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
	for (size_t i = 0; i < settings.size(); i++) {
		auto value = duckdb_profiling_info_get_value(profiling_info, settings[i].c_str());
		if (value != nullptr) {
			if (settings[i] == "EXTRA_INFO") {
				REQUIRE(value[0] == '\"');
				REQUIRE(value[strlen(value) - 1] == '\"');
				continue;
			} else {
				double result = 0;
				try {
					result = std::stod(value);
				} catch (std::invalid_argument &e) {
					REQUIRE(false);
				}

				REQUIRE(result >= 0);
			}
		}
	}
}

// Traverse the tree and retrieve all metrics
void TraverseTree(duckdb_profiling_info profiling_info, const std::vector<string> &settings) {
	static idx_t DEPTH = 0;
	if (DEPTH == 0) {
		REQUIRE(duckdb_profiling_info_get_name(profiling_info) == nullptr);
		REQUIRE(duckdb_profiling_info_get_query(profiling_info) != nullptr);
	} else {
		REQUIRE(duckdb_profiling_info_get_name(profiling_info) != nullptr);
		REQUIRE(duckdb_profiling_info_get_query(profiling_info) == nullptr);
	}

	DEPTH++;

	RetrieveAllMetrics(profiling_info, settings);

	auto child_count = duckdb_profiling_info_get_child_count(profiling_info);
	for (idx_t i = 0; i < child_count; i++) {
		auto child = duckdb_profiling_info_get_child(profiling_info, i);
		TraverseTree(child, settings);
	}
}

TEST_CASE("Test Profiling with Single Metric", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	// test only CPU_TIME profiling
	std::vector<string> settings = {"CPU_TIME"};
	REQUIRE_NO_FAIL(tester.Query("PRAGMA custom_profiling_settings=" + BuildProfilingSettingsString(settings)));

	tester.Query("SELECT 42");

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

	// test all profiling metrics
	std::vector<string> settings = {"CPU_TIME", "EXTRA_INFO", "OPERATOR_CARDINALITY", "OPERATOR_TIMING"};
	REQUIRE_NO_FAIL(tester.Query("PRAGMA custom_profiling_settings=" + BuildProfilingSettingsString(settings)));

	tester.Query("SELECT 42");

	auto profiling_info = duckdb_get_profiling_info(tester.connection);
	REQUIRE(profiling_info != nullptr);

	TraverseTree(profiling_info, settings);

	// Cleanup
	tester.Cleanup();
}

// TODO:
//- retrieve "NAME" and "TYPE" from OperatorProfilingNode and "QUERY" from QueryProfilingNode?
