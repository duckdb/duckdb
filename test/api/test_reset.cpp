#include "catch.hpp"
#include "test_helpers.hpp"

#include <set>
#include <map>

#include <iostream>

using namespace duckdb;
using namespace std;

struct OptionValuePair {
	Value input;
	Value output;
};

void RequireValuDiff(ConfigurationOption *op, const Value &left, const Value &right, int line);
#define REQUIRE_VALUE_DIFF(op, lhs, rhs) RequireValueDiff(op, lhs, rhs, __LINE__)

void RequireValueEqual(ConfigurationOption *op, const Value &left, const Value &right, int line);
#define REQUIRE_VALUE_EQUAL(op, lhs, rhs) RequireValueEqual(op, lhs, rhs, __LINE__)

OptionValuePair &GetValueForOption(const string &name) {
	static unordered_map<string, OptionValuePair> value_map = {
	    {"access_mode", {Value("READ_ONLY"), Value("read_only")}},
	    {"threads", {Value::BIGINT(42), Value::BIGINT(42)}},
	    {"checkpoint_threshold", {"4.2GB", "4.2GB"}},
	    {"debug_checkpoint_abort", {"before_header", "before_header"}},
	    {"default_collation", {"nocase", "nocase"}},
	    {"default_order", {"desc", "desc"}},
	    {"default_null_order", {"nulls_last", "nulls_last"}},
	    {"disabled_optimizers", {"extension", "extension"}},
	    {"enable_fsst_vectors", {true, true}},
	    {"enable_object_cache", {true, true}},
	    {"enable_profiling", {"json", "json"}},
	    {"enable_progress_bar", {true, true}},
	    {"experimental_parallel_csv", {true, true}},
	    {"explain_output", {true, true}},
	    {"external_threads", {8, 8}},
	    {"file_search_path", {"test", "test"}},
	    {"force_compression", {"uncompressed", "Uncompressed"}},
	    {"home_directory", {"test", "test"}},
	    {"log_query_path", {"test", "test"}},
	    {"max_expression_depth", {50, 50}},
	    {"max_memory", {"4.2GB", "4.2GB"}},
	    {"memory_limit", {"4.2GB", "4.2GB"}},
	    {"null_order", {"nulls_last", "nulls_last"}},
	    {"perfect_ht_threshold", {0, 0}},
	    {"preserve_identifier_case", {false, false}},
	    {"preserve_insertion_order", {false, false}},
	    {"profiler_history_size", {0, 0}},
	    {"profile_output", {"test", "test"}},
	    {"profiling_mode", {"detailed", "detailed"}},
	    {"enable_progress_bar_print", {false, false}},
	    {"progress_bar_time", {0, 0}},
	    {"temp_directory", {"tmp", "tmp"}},
	    {"wal_autocheckpoint", {"4.2GB", "4.2GB"}},
	    {"worker_threads", {42, 42}},
	    {"enable_http_metadata_cache", {true, true}},
	    {"force_bitpacking_mode", {"constant", "constant"}},
	};
	// Every option that's not excluded has to be part of this map
	if (!value_map.count(name)) {
		REQUIRE(name == "MISSING_FROM_MAP");
	}
	return value_map[name];
}

bool OptionIsExcludedFromTest(const string &name) {
	static unordered_set<string> excluded_options = {
	    "schema",
	    "search_path",
	    "debug_force_external",
	    "debug_force_no_cross_product",
	    "debug_window_mode",
	    "enable_external_access",    // cant change this while db is running
	    "allow_unsigned_extensions", // cant change this while db is running
	    "password",
	    "username",
	    "user",
	    "profiling_output", // just an alias
	};
	return excluded_options.count(name) == 1;
}

bool ValueEqual(const Value &left, const Value &right) {
	if (left.IsNull() != right.IsNull()) {
		// Only one is NULL
		return false;
	}
	if (!left.IsNull() && !right.IsNull()) {
		// Neither are NULL
		return left == right;
	}
	// Both are NULL
	return true;
}

void RequireValueDiff(ConfigurationOption *op, const Value &left, const Value &right, int line) {
	if (!ValueEqual(left, right)) {
		return;
	}
	auto error = StringUtil::Format("\nLINE[%d] (Option:%s) | Expected left:'%s' and right:'%s' to be different", line,
	                                op->name, left.ToString(), right.ToString());
	cerr << error << endl;
	REQUIRE(false);
}

void RequireValueEqual(ConfigurationOption *op, const Value &left, const Value &right, int line) {
	if (ValueEqual(left, right)) {
		return;
	}
	auto error = StringUtil::Format("\nLINE[%d] (Option:%s) | Expected left:'%s' and right:'%s' to be equal", line,
	                                op->name, left.ToString(), right.ToString());
	cerr << error << endl;
	REQUIRE(false);
}

//! New options should be added to the value_map in GetValueForOption
//! Or added to the 'excluded_options' in OptionIsExcludedFromTest
TEST_CASE("Test RESET statement for ClientConfig options", "[api]") {

	// Create a connection
	DuckDB db(nullptr);
	Connection con(db);

	auto &config = DBConfig::GetConfig(*db.instance);
	// Get all configuration options
	auto options = config.GetOptions();

	// Test RESET for every option
	for (auto &option : options) {
		if (OptionIsExcludedFromTest(option.name)) {
			continue;
		}

		auto op = config.GetOptionByName(option.name);
		REQUIRE(op);

		// Get the current value of the option
		auto original_value = op->get_setting(*con.context);
		// Get the new value for the option
		auto &value_pair = GetValueForOption(option.name);

		// Verify that the new value is different, so we can test RESET
		REQUIRE_VALUE_DIFF(op, original_value, value_pair.output);

		if (!op->set_global) {
			// TODO: add testing for local (client-config) settings
			continue;
		}
		// Set the new option
		op->set_global(db.instance.get(), config, value_pair.input);

		// Get the value of the option again
		auto changed_value = op->get_setting(*con.context);
		REQUIRE_VALUE_EQUAL(op, changed_value, value_pair.output);

		op->reset_global(db.instance.get(), config);

		// Get the reset value of the option
		auto reset_value = op->get_setting(*con.context);
		REQUIRE_VALUE_EQUAL(op, reset_value, original_value);
	}
}
