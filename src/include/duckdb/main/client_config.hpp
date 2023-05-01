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
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/progress_bar/progress_bar.hpp"

namespace duckdb {
class ClientContext;
class PhysicalResultCollector;
class PreparedStatementData;

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
	//! Whether or not we should verify the serializer
	bool verify_serializer = false;
	//! Enable the running of optimizers
	bool enable_optimizer = true;
	//! Enable caching operators
	bool enable_caching_operators = true;
	//! Force parallelism of small tables, used for testing
	bool verify_parallelism = false;
	//! Force index join independent of table cardinality, used for testing
	bool force_index_join = false;
	//! Force out-of-core computation for operators that support it, used for testing
	bool force_external = false;
	//! Force disable cross product generation when hyper graph isn't connected, used for testing
	bool force_no_cross_product = false;
	//! Force use of IEJoin to implement AsOfJoin, used for testing
	bool force_asof_iejoin = false;
	//! Force every sink & source in a pipeline to block asynchronously briefly, used for testing
	// TODO: move to ci job
	bool force_async_pipelines = true;
	//! If this context should also try to use the available replacement scans
	//! True by default
	bool use_replacement_scans = true;
	//! Maximum bits allowed for using a perfect hash table (i.e. the perfect HT can hold up to 2^perfect_ht_threshold
	//! elements)
	idx_t perfect_ht_threshold = 12;
	//! The maximum number of rows to accumulate before sorting ordered aggregates.
	idx_t ordered_aggregate_threshold = (idx_t(1) << 18);

	//! Callback to create a progress bar display
	progress_bar_display_create_func_t display_create_func = nullptr;

	//! Override for the default extension repository
	string custom_extension_repo = "";

	//! The explain output type used when none is specified (default: PHYSICAL_ONLY)
	ExplainOutputType explain_output_type = ExplainOutputType::PHYSICAL_ONLY;

	//! The maximum amount of pivot columns
	idx_t pivot_limit = 100000;

	//! Whether or not the "/" division operator defaults to integer division or floating point division
	bool integer_division = false;

	//! Generic options
	case_insensitive_map_t<Value> set_variables;

	//! Function that is used to create the result collector for a materialized result
	//! Defaults to PhysicalMaterializedCollector
	get_result_collector_t result_collector = nullptr;

public:
	static ClientConfig &GetConfig(ClientContext &context);
	static const ClientConfig &GetConfig(const ClientContext &context);

	string ExtractTimezone() const;

	bool AnyVerification() {
		return query_verification_enabled || verify_external || verify_serializer;
	}
};

} // namespace duckdb
