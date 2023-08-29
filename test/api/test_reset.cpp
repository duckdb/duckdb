#include "catch.hpp"
#include "test_helpers.hpp"

#include <iostream>
#include <map>
#include <set>

using namespace duckdb;
using namespace std;

struct OptionValuePair {
	OptionValuePair() {
	}
	OptionValuePair(Value val) : input(val), output(val) {
	}
	OptionValuePair(Value input, Value output) : input(std::move(input)), output(std::move(output)) {
	}

	Value input;
	Value output;
};

struct OptionValueSet {
	OptionValueSet() {
	}
	OptionValueSet(Value val) {
		pairs.emplace_back(std::move(val));
	}
	OptionValueSet(Value input, Value output) {
		pairs.emplace_back(std::move(input), std::move(output));
	}
	OptionValueSet(duckdb::vector<std::string> pairs_p) {
		for (auto &pair : pairs_p) {
			pairs.emplace_back(pair);
		}
	}
	OptionValueSet(duckdb::vector<OptionValuePair> pairs_p) : pairs(std::move(pairs_p)) {
	}

	duckdb::vector<OptionValuePair> pairs;
};

void RequireValueEqual(ConfigurationOption *op, const Value &left, const Value &right, int line);
#define REQUIRE_VALUE_EQUAL(op, lhs, rhs) RequireValueEqual(op, lhs, rhs, __LINE__)

OptionValueSet &GetValueForOption(const string &name) {
	static unordered_map<string, OptionValueSet> value_map = {
	    {"access_mode", {Value("READ_ONLY"), Value("read_only")}},
	    {"threads", {Value::BIGINT(42), Value::BIGINT(42)}},
	    {"checkpoint_threshold", {"4.2GB"}},
	    {"debug_checkpoint_abort", {{"none", "before_truncate", "before_header", "after_free_list_write"}}},
	    {"default_collation", {"nocase"}},
	    {"default_order", {"desc"}},
	    {"default_null_order", {"nulls_first"}},
	    {"disabled_optimizers", {"extension"}},
	    {"debug_asof_iejoin", {Value(true)}},
	    {"debug_force_external", {Value(true)}},
	    {"debug_force_no_cross_product", {Value(true)}},
	    {"debug_force_external", {Value(true)}},
	    {"prefer_range_joins", {Value(true)}},
	    {"custom_extension_repository", {"duckdb.org/no-extensions-here", "duckdb.org/no-extensions-here"}},
	    {"autoinstall_extension_repository", {"duckdb.org/no-extensions-here", "duckdb.org/no-extensions-here"}},
#ifdef DUCKDB_EXTENSION_AUTOLOAD_DEFAULT
	    {"autoload_known_extensions", {!DUCKDB_EXTENSION_AUTOLOAD_DEFAULT}},
#else
	    {"autoload_known_extensions", {true}},
#endif
#ifdef DUCKDB_EXTENSION_AUTOINSTALL_DEFAULT
	    {"autoinstall_known_extensions", {!DUCKDB_EXTENSION_AUTOINSTALL_DEFAULT}},
#else
	    {"autoinstall_known_extensions", {true}},
#endif
	    {"enable_fsst_vectors", {true}},
	    {"enable_object_cache", {true}},
	    {"enable_profiling", {"json"}},
	    {"enable_progress_bar", {true}},
	    {"explain_output", {{"all", "optimized_only", "physical_only"}}},
	    {"external_threads", {8}},
	    {"file_search_path", {"test"}},
	    {"force_compression", {"uncompressed", "Uncompressed"}},
	    {"home_directory", {"test"}},
	    {"integer_division", {true}},
	    {"extension_directory", {"test"}},
	    {"immediate_transaction_mode", {true}},
	    {"max_expression_depth", {50}},
	    {"max_memory", {"4.2GB"}},
	    {"memory_limit", {"4.2GB"}},
	    {"ordered_aggregate_threshold", {Value::UBIGINT(idx_t(1) << 12)}},
	    {"null_order", {"nulls_first"}},
	    {"perfect_ht_threshold", {0}},
	    {"pivot_filter_threshold", {999}},
	    {"pivot_limit", {999}},
	    {"preserve_identifier_case", {false}},
	    {"preserve_insertion_order", {false}},
	    {"profile_output", {"test"}},
	    {"profiling_mode", {"detailed"}},
	    {"enable_progress_bar_print", {false}},
	    {"progress_bar_time", {0}},
	    {"temp_directory", {"tmp"}},
	    {"wal_autocheckpoint", {"4.2GB"}},
	    {"worker_threads", {42}},
	    {"enable_http_metadata_cache", {true}},
	    {"force_bitpacking_mode", {"constant"}},
	    {"allocator_flush_threshold", {"4.2GB"}},
	    {"arrow_large_buffer_size", {true}}};
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
	    "debug_window_mode",
	    "experimental_parallel_csv",
	    "lock_configuration",        // cant change this while db is running
	    "disabled_filesystems",      // cant change this while db is running
	    "enable_external_access",    // cant change this while db is running
	    "allow_unsigned_extensions", // cant change this while db is running
	    "log_query_path",
	    "password",
	    "username",
	    "user",
	    "profiling_output", // just an alias
	    "profiler_history_size"};
	return excluded_options.count(name) == 1;
}

bool ValueEqual(const Value &left, const Value &right) {
	return Value::NotDistinctFrom(left, right);
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
	con.Query("BEGIN TRANSACTION");
	con.Query("PRAGMA disable_profiling");

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

		auto &value_set = GetValueForOption(option.name);
		// verify that at least one value is different
		bool any_different = false;
		string options;
		for (auto &value_pair : value_set.pairs) {
			if (!ValueEqual(original_value, value_pair.output)) {
				any_different = true;
			} else {
				if (!options.empty()) {
					options += ", ";
				}
				options += value_pair.output.ToString();
			}
		}
		if (!any_different) {
			auto error = StringUtil::Format(
			    "\n(Option:%s) | Expected original value '%s' and provided option '%s' to be different", op->name,
			    original_value.ToString(), options);
			cerr << error << endl;
			REQUIRE(false);
		}
		for (auto &value_pair : value_set.pairs) {
			// Get the new value for the option
			auto input = value_pair.input.DefaultCastAs(op->parameter_type);
			// Set the new option
			if (op->set_local) {
				op->set_local(*con.context, input);
			} else {
				op->set_global(db.instance.get(), config, input);
			}
			// Get the value of the option again
			auto changed_value = op->get_setting(*con.context);
			REQUIRE_VALUE_EQUAL(op, changed_value, value_pair.output);

			if (op->reset_local) {
				op->reset_local(*con.context);
			} else {
				op->reset_global(db.instance.get(), config);
			}

			// Get the reset value of the option
			auto reset_value = op->get_setting(*con.context);
			REQUIRE_VALUE_EQUAL(op, reset_value, original_value);
		}
	}
}
