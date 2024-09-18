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

OptionValueSet GetValueForOption(const string &name, LogicalTypeId type) {
	static unordered_map<string, OptionValueSet> value_map = {
	    {"threads", {Value::BIGINT(42), Value::BIGINT(42)}},
	    {"checkpoint_threshold", {"4.0 GiB"}},
	    {"debug_checkpoint_abort", {{"none", "before_truncate", "before_header", "after_free_list_write"}}},
	    {"default_collation", {"nocase"}},
	    {"default_order", {"desc"}},
	    {"default_null_order", {"nulls_first"}},
	    {"disabled_optimizers", {"extension"}},
	    {"debug_force_external", {Value(true)}},
	    {"old_implicit_casting", {Value(true)}},
	    {"prefer_range_joins", {Value(true)}},
	    {"allow_persistent_secrets", {Value(false)}},
	    {"secret_directory", {"/tmp/some/path"}},
	    {"default_secret_storage", {"custom_storage"}},
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
	    {"enable_profiling", {"json"}},
	    {"explain_output", {{"all", "optimized_only", "physical_only"}}},
	    {"file_search_path", {"test"}},
	    {"force_compression", {"uncompressed", "Uncompressed"}},
	    {"home_directory", {"test"}},
	    {"allow_extensions_metadata_mismatch", {"true"}},
	    {"extension_directory", {"test"}},
	    {"max_expression_depth", {50}},
	    {"max_memory", {"4.0 GiB"}},
	    {"max_temp_directory_size", {"10.0 GiB"}},
	    {"merge_join_threshold", {73}},
	    {"nested_loop_join_threshold", {73}},
	    {"memory_limit", {"4.0 GiB"}},
	    {"storage_compatibility_version", {"v0.10.0"}},
	    {"ordered_aggregate_threshold", {Value::UBIGINT(idx_t(1) << 12)}},
	    {"null_order", {"nulls_first"}},
	    {"perfect_ht_threshold", {0}},
	    {"pivot_filter_threshold", {999}},
	    {"pivot_limit", {999}},
	    {"partitioned_write_flush_threshold", {123}},
	    {"preserve_identifier_case", {false}},
	    {"preserve_insertion_order", {false}},
	    {"profile_output", {"test"}},
	    {"profiling_mode", {"detailed"}},
	    {"enable_progress_bar_print", {false}},
	    {"scalar_subquery_error_on_multiple_rows", {false}},
	    {"ieee_floating_point_ops", {false}},
	    {"progress_bar_time", {0}},
	    {"temp_directory", {"tmp"}},
	    {"wal_autocheckpoint", {"4.0 GiB"}},
	    {"force_bitpacking_mode", {"constant"}},
	    {"http_proxy", {"localhost:80"}},
	    {"http_proxy_username", {"john"}},
	    {"http_proxy_password", {"doe"}},
	    {"http_logging_output", {"my_cool_outputfile"}},
	    {"allocator_flush_threshold", {"4.0 GiB"}},
	    {"allocator_bulk_deallocation_flush_threshold", {"4.0 GiB"}}};
	// Every option that's not excluded has to be part of this map
	if (!value_map.count(name)) {
		switch (type) {
		case LogicalTypeId::BOOLEAN:
			return OptionValueSet(Value::BOOLEAN(true));
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
			return OptionValueSet(Value::Numeric(type, 42));
		default:
			break;
		}
		REQUIRE(name == "MISSING_FROM_MAP");
	}
	return value_map[name];
}

bool OptionIsExcludedFromTest(const string &name) {
	static unordered_set<string> excluded_options = {
	    "access_mode",
	    "schema",
	    "search_path",
	    "debug_window_mode",
	    "experimental_parallel_csv",
	    "lock_configuration",         // cant change this while db is running
	    "disabled_filesystems",       // cant change this while db is running
	    "enable_external_access",     // cant change this while db is running
	    "allow_unsigned_extensions",  // cant change this while db is running
	    "allow_community_extensions", // cant change this while db is running
	    "allow_unredacted_secrets",   // cant change this while db is running
	    "streaming_buffer_size",
	    "log_query_path",
	    "password",
	    "username",
	    "user",
	    "external_threads", // tested in test_threads.cpp
	    "profiling_output", // just an alias
	    "duckdb_api",
	    "custom_user_agent",
	    "custom_profiling_settings",
	    "custom_user_agent",
	    "default_block_size",
	    "index_scan_percentage",
	    "index_scan_max_count"};
	return excluded_options.count(name) == 1;
}

bool ValueEqual(const Value &left, const Value &right) {
	return Value::NotDistinctFrom(left, right);
}

void RequireValueEqual(const ConfigurationOption &op, const Value &left, const Value &right, int line) {
	if (ValueEqual(left, right)) {
		return;
	}
	auto error = StringUtil::Format("\nLINE[%d] (Option:%s) | Expected left:'%s' and right:'%s' to be equal", line,
	                                op.name, left.ToString(), right.ToString());
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

		auto value_set = GetValueForOption(option.name, option.parameter_type);
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
			REQUIRE_VALUE_EQUAL(*op, changed_value, value_pair.output);

			if (op->reset_local) {
				op->reset_local(*con.context);
			} else {
				op->reset_global(db.instance.get(), config);
			}

			// Get the reset value of the option
			auto reset_value = op->get_setting(*con.context);
			REQUIRE_VALUE_EQUAL(*op, reset_value, original_value);
		}
	}
}
