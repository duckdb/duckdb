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

TEST_CASE("Test Profiling", "[capi]") {
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

	RetrieveAllMetrics(profiling_info, settings);

	// Retrieve metric that is not enabled
	REQUIRE(duckdb_profiling_info_get_value(profiling_info, "EXTRA_INFO") == nullptr);

	// Retrieve child count
	REQUIRE(duckdb_profiling_info_get_child_count(profiling_info) == 1);

	// Retrieve child
	auto child = duckdb_profiling_info_get_child(profiling_info, 0);
	REQUIRE(child != nullptr);

	// Retrieve child's metrics
	RetrieveAllMetrics(child, settings);
}

// TODO:
//- build recursive function to retrieve all metrics from profiling tree
//- test all metrics
//- retrieve "NAME" and "TYPE" from OperatorProfilingNode and "QUERY" from QueryProfilingNode?
