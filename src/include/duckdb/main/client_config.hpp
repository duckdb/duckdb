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
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/enums/output_type.hpp"
#include "duckdb/common/enums/profiler_format.hpp"
#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/profiling_info.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/user_settings.hpp"

namespace duckdb {

class ClientContext;
class PhysicalResultCollector;
class PreparedStatementData;

typedef std::function<PhysicalOperator &(ClientContext &context, PreparedStatementData &data)> get_result_collector_t;

struct ClientConfig {
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
	profiler_settings_t profiler_settings = MetricsUtils::GetDefaultMetrics();
	//! The input format type of the profiler settings
	LogicalTypeId profiler_settings_type = LogicalTypeId::VARCHAR;

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
	//! Force use of fetch row instead of scan, used for testing
	bool force_fetch_row = false;
	//! If this context should also try to use the available replacement scans
	//! True by default
	bool use_replacement_scans = true;

	//! The maximum amount of memory to keep buffered in a streaming query result. Default: 1mb.
	idx_t streaming_buffer_size = 1000000;

	//! The maximum memory for query intermediates (sorts, hash tables) per connection (in bytes). Default: Global
	//! memory limit.
	optional_idx operator_memory_limit;

	//! Callback to create a progress bar display
	progress_bar_display_create_func_t display_create_func = nullptr;

	//! The profiling coverage. SELECT is the default behavior, and ALL emits profiling information for all operator
	//! types.
	ProfilingCoverage profiling_coverage = ProfilingCoverage::SELECT;

	//! Generic options
	LocalUserSettings user_settings;

	//! Variables set by the user
	case_insensitive_map_t<Value> user_variables;

	//! Function that is used to create the result collector for a materialized result.
	get_result_collector_t get_result_collector = nullptr;

	//! If HTTP logging is enabled or not.
	bool enable_http_logging = true;

	//! **DEPRECATED** The file to save query HTTP logging information to, instead of printing it to the console
	//! (empty = output to the DuckDB logger)
	string http_logging_output;

public:
	static ClientConfig &GetConfig(ClientContext &context);
	static const ClientConfig &GetConfig(const ClientContext &context);

	bool AnyVerification() const;

	void SetUserVariable(const String &name, Value value);
	bool GetUserVariable(const string &name, Value &result);
	void ResetUserVariable(const String &name);

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
