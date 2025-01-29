//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_config.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/output_type.hpp"
#include "duckdb/common/enums/profiler_format.hpp"
#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/profiling_info.hpp"

namespace duckdb {

class ClientContext;
class PhysicalResultCollector;
class PreparedStatementData;
class HTTPLogger;

typedef std::function<unique_ptr<PhysicalResultCollector>(ClientContext &context, PreparedStatementData &data)>
    get_result_collector_t;

struct ClientConfig {
	//! The home directory used by the system (if any)
	string home_directory;
	//! If the query profiler is enabled or not.
	bool enable_profiler = false;
	//! If detailed query profiling is enabled
	bool enable_detailed_profiling = false;
	//! The format to print query profiling information in (default: query_tree), if enabled.
	ProfilerPrintFormat profiler_print_format = ProfilerPrintFormat::QUERY_TREE;
	//! The file to save query profiling information to, instead of printing it to the console
	//! (empty = print to console)
	string profiler_save_location;
	//! The custom settings for the profiler
	//! (empty = use the default settings)
	profiler_settings_t profiler_settings = ProfilingInfo::DefaultSettings();

	//! Allows suppressing profiler output, even if enabled. We turn on the profiler on all test runs but don't want
	//! to output anything
	bool emit_profiler_output = true;

	//! system-wide progress bar disable.
	const char *system_progress_bar_disable_reason = nullptr;
	//! If the progress bar is enabled or not.
	bool enable_progress_bar = false;
	//! If the print of the progress bar is enabled
	bool print_progress_bar = true;
	//! The wait time before showing the progress bar
	int wait_time = 2000;

	//! Preserve identifier case while parsing.
	//! If false, all unquoted identifiers are lower-cased (e.g. "MyTable" -> "mytable").
	bool preserve_identifier_case = true;
	//! The maximum expression depth limit in the parser
	idx_t max_expression_depth = 1000;

	//! Whether or not aggressive query verification is enabled
	bool query_verification_enabled = false;
	//! Whether or not verification of external operators is enabled, used for testing
	bool verify_external = false;
	//! Whether or not verification of fetch row code is enabled, used for testing
	bool verify_fetch_row = false;
	//! Whether or not we should verify the serializer
	bool verify_serializer = false;
	//! Enable the running of optimizers
	bool enable_optimizer = true;
	//! Enable caching operators
	bool enable_caching_operators = true;
	//! Force parallelism of small tables, used for testing
	bool verify_parallelism = false;
	//! Force out-of-core computation for operators that support it, used for testing
	bool force_external = false;
	//! Force disable cross product generation when hyper graph isn't connected, used for testing
	bool force_no_cross_product = false;
	//! Force use of IEJoin to implement AsOfJoin, used for testing
	bool force_asof_iejoin = false;
	//! Force use of fetch row instead of scan, used for testing
	bool force_fetch_row = false;
	//! Use range joins for inequalities, even if there are equality predicates
	bool prefer_range_joins = false;
	//! If this context should also try to use the available replacement scans
	//! True by default
	bool use_replacement_scans = true;
	//! Maximum bits allowed for using a perfect hash table (i.e. the perfect HT can hold up to 2^perfect_ht_threshold
	//! elements)
	idx_t perfect_ht_threshold = 12;
	//! The maximum number of rows to accumulate before sorting ordered aggregates.
	idx_t ordered_aggregate_threshold = (idx_t(1) << 18);
	//! The number of rows to accumulate before flushing during a partitioned write
	idx_t partitioned_write_flush_threshold = idx_t(1) << idx_t(19);
	//! The amount of rows we can keep open before we close and flush them during a partitioned write
	idx_t partitioned_write_max_open_files = idx_t(100);
	//! The number of rows we need on either table to choose a nested loop join
	idx_t nested_loop_join_threshold = 5;
	//! The number of rows we need on either table to choose a merge join over an IE join
	idx_t merge_join_threshold = 1000;

	//! The maximum amount of memory to keep buffered in a streaming query result. Default: 1mb.
	idx_t streaming_buffer_size = 1000000;

	//! Callback to create a progress bar display
	progress_bar_display_create_func_t display_create_func = nullptr;

	//! The explain output type used when none is specified (default: PHYSICAL_ONLY)
	ExplainOutputType explain_output_type = ExplainOutputType::PHYSICAL_ONLY;

	//! The maximum amount of pivot columns
	idx_t pivot_limit = 100000;

	//! The threshold at which we switch from using filtered aggregates to LIST with a dedicated pivot operator
	idx_t pivot_filter_threshold = 20;

	//! The maximum amount of OR filters we generate dynamically from a hash join
	idx_t dynamic_or_filter_threshold = 50;

	//! The maximum amount of rows in the LIMIT/SAMPLE for which we trigger late materialization
	idx_t late_materialization_max_rows = 50;

	//! Whether the "/" division operator defaults to integer division or floating point division
	bool integer_division = false;
	//! When a scalar subquery returns multiple rows - return a random row instead of returning an error
	bool scalar_subquery_error_on_multiple_rows = true;
	//! Use IEE754-compliant floating point operations (returning NAN instead of errors/NULL)
	bool ieee_floating_point_ops = true;
	//! Allow ordering by non-integer literals - ordering by such literals has no effect
	bool order_by_non_integer_literal = false;

	//! Output error messages as structured JSON instead of as a raw string
	bool errors_as_json = false;

	//! Generic options
	case_insensitive_map_t<Value> set_variables;

	//! Variables set by the user
	case_insensitive_map_t<Value> user_variables;

	//! Function that is used to create the result collector for a materialized result
	//! Defaults to PhysicalMaterializedCollector
	get_result_collector_t result_collector = nullptr;

	//! If HTTP logging is enabled or not.
	bool enable_http_logging = false;
	//! The file to save query HTTP logging information to, instead of printing it to the console
	//! (empty = print to console)
	string http_logging_output;

public:
	static ClientConfig &GetConfig(ClientContext &context);
	static const ClientConfig &GetConfig(const ClientContext &context);

	bool AnyVerification() {
		return query_verification_enabled || verify_external || verify_serializer || verify_fetch_row;
	}

	void SetUserVariable(const string &name, Value value) {
		user_variables[name] = std::move(value);
	}

	bool GetUserVariable(const string &name, Value &result) {
		auto entry = user_variables.find(name);
		if (entry == user_variables.end()) {
			return false;
		}
		result = entry->second;
		return true;
	}

	void ResetUserVariable(const string &name) {
		user_variables.erase(name);
	}

	template <class OP>
	static typename OP::RETURN_TYPE GetSetting(const ClientContext &context) {
		return OP::GetSetting(context).template GetValue<typename OP::RETURN_TYPE>();
	}

	template <class OP>
	static Value GetSettingValue(const ClientContext &context) {
		return OP::GetSetting(context);
	}

public:
	void SetDefaultStreamingBufferSize();
};

struct ScopedConfigSetting {
public:
	using config_modify_func_t = std::function<void(ClientConfig &config)>;

public:
	explicit ScopedConfigSetting(ClientConfig &config, config_modify_func_t set_f = nullptr,
	                             config_modify_func_t unset_f = nullptr)
	    : config(config), set(std::move(set_f)), unset(std::move(unset_f)) {
		if (set) {
			set(config);
		}
	}
	~ScopedConfigSetting() {
		if (unset) {
			unset(config);
		}
	}

public:
	ClientConfig &config;
	config_modify_func_t set;
	config_modify_func_t unset;
};

} // namespace duckdb
