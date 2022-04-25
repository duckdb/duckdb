//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"

#include <functional>

namespace duckdb {

class BaseStatistics;
class LogicalGet;
struct ParallelState;
class TableFilterSet;

struct FunctionOperatorData {
	DUCKDB_API virtual ~FunctionOperatorData();
};

struct TableFunctionInfo {
	DUCKDB_API virtual ~TableFunctionInfo();
};

struct TableFilterCollection {
	DUCKDB_API explicit TableFilterCollection(TableFilterSet *table_filters);

	TableFilterSet *table_filters;
};

struct TableFunctionBindInput {
	TableFunctionBindInput(vector<Value> &inputs, named_parameter_map_t &named_parameters,
	                       vector<LogicalType> &input_table_types, vector<string> &input_table_names,
	                       TableFunctionInfo *info)
	    : inputs(inputs), named_parameters(named_parameters), input_table_types(input_table_types),
	      input_table_names(input_table_names), info(info) {
	}

	vector<Value> &inputs;
	named_parameter_map_t &named_parameters;
	vector<LogicalType> &input_table_types;
	vector<string> &input_table_names;
	TableFunctionInfo *info;
};

typedef unique_ptr<FunctionData> (*table_function_bind_t)(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names);
typedef unique_ptr<FunctionOperatorData> (*table_function_init_t)(ClientContext &context, const FunctionData *bind_data,
                                                                  const vector<column_t> &column_ids,
                                                                  TableFilterCollection *filters);
typedef unique_ptr<BaseStatistics> (*table_statistics_t)(ClientContext &context, const FunctionData *bind_data,
                                                         column_t column_index);
typedef void (*table_function_t)(ClientContext &context, const FunctionData *bind_data,
                                 FunctionOperatorData *operator_state, DataChunk &output);

typedef OperatorResultType (*table_in_out_function_t)(ClientContext &context, const FunctionData *bind_data,
                                                      FunctionOperatorData *operator_state, DataChunk &input,
                                                      DataChunk &output);

typedef void (*table_function_parallel_t)(ClientContext &context, const FunctionData *bind_data,
                                          FunctionOperatorData *operator_state, DataChunk &output,
                                          ParallelState *parallel_state);

typedef void (*table_function_cleanup_t)(ClientContext &context, const FunctionData *bind_data,
                                         FunctionOperatorData *operator_state);
typedef idx_t (*table_function_max_threads_t)(ClientContext &context, const FunctionData *bind_data);
typedef unique_ptr<ParallelState> (*table_function_init_parallel_state_t)(ClientContext &context,
                                                                          const FunctionData *bind_data,
                                                                          const vector<column_t> &column_ids,
                                                                          TableFilterCollection *filters);
typedef unique_ptr<FunctionOperatorData> (*table_function_init_parallel_t)(ClientContext &context,
                                                                           const FunctionData *bind_data,
                                                                           ParallelState *state,
                                                                           const vector<column_t> &column_ids,
                                                                           TableFilterCollection *filters);
typedef bool (*table_function_parallel_state_next_t)(ClientContext &context, const FunctionData *bind_data,
                                                     FunctionOperatorData *state, ParallelState *parallel_state);
typedef double (*table_function_progress_t)(ClientContext &context, const FunctionData *bind_data);
typedef void (*table_function_dependency_t)(unordered_set<CatalogEntry *> &dependencies, const FunctionData *bind_data);
typedef unique_ptr<NodeStatistics> (*table_function_cardinality_t)(ClientContext &context,
                                                                   const FunctionData *bind_data);
typedef void (*table_function_pushdown_complex_filter_t)(ClientContext &context, LogicalGet &get,
                                                         FunctionData *bind_data,
                                                         vector<unique_ptr<Expression>> &filters);
typedef string (*table_function_to_string_t)(const FunctionData *bind_data);

class TableFunction : public SimpleNamedParameterFunction {
public:
	DUCKDB_API
	TableFunction(string name, vector<LogicalType> arguments, table_function_t function,
	              table_function_bind_t bind = nullptr, table_function_init_t init = nullptr,
	              table_statistics_t statistics = nullptr, table_function_cleanup_t cleanup = nullptr,
	              table_function_dependency_t dependency = nullptr, table_function_cardinality_t cardinality = nullptr,
	              table_function_pushdown_complex_filter_t pushdown_complex_filter = nullptr,
	              table_function_to_string_t to_string = nullptr, table_function_max_threads_t max_threads = nullptr,
	              table_function_init_parallel_state_t init_parallel_state = nullptr,
	              table_function_parallel_t parallel_function = nullptr,
	              table_function_init_parallel_t parallel_init = nullptr,
	              table_function_parallel_state_next_t parallel_state_next = nullptr, bool projection_pushdown = false,
	              bool filter_pushdown = false, table_function_progress_t query_progress = nullptr,
	              table_in_out_function_t in_out_function = nullptr);
	DUCKDB_API
	TableFunction(const vector<LogicalType> &arguments, table_function_t function, table_function_bind_t bind = nullptr,
	              table_function_init_t init = nullptr, table_statistics_t statistics = nullptr,
	              table_function_cleanup_t cleanup = nullptr, table_function_dependency_t dependency = nullptr,
	              table_function_cardinality_t cardinality = nullptr,
	              table_function_pushdown_complex_filter_t pushdown_complex_filter = nullptr,
	              table_function_to_string_t to_string = nullptr, table_function_max_threads_t max_threads = nullptr,
	              table_function_init_parallel_state_t init_parallel_state = nullptr,
	              table_function_parallel_t parallel_function = nullptr,
	              table_function_init_parallel_t parallel_init = nullptr,
	              table_function_parallel_state_next_t parallel_state_next = nullptr, bool projection_pushdown = false,
	              bool filter_pushdown = false, table_function_progress_t query_progress = nullptr,
	              table_in_out_function_t in_out_function = nullptr);
	DUCKDB_API TableFunction();

	//! Bind function
	//! This function is used for determining the return type of a table producing function and returning bind data
	//! The returned FunctionData object should be constant and should not be changed during execution.
	table_function_bind_t bind;
	//! (Optional) init function
	//! Initialize the operator state of the function. The operator state is used to keep track of the progress in the
	//! table function.
	table_function_init_t init;
	//! The main function
	table_function_t function;
	//! The table in-out function (if this is an in-out function)
	table_in_out_function_t in_out_function;
	//! (Optional) statistics function
	//! Returns the statistics of a specified column
	table_statistics_t statistics;
	//! (Optional) cleanup function
	//! The final cleanup function, called after all data is exhausted from the main function
	table_function_cleanup_t cleanup;
	//! (Optional) dependency function
	//! Sets up which catalog entries this table function depend on
	table_function_dependency_t dependency;
	//! (Optional) cardinality function
	//! Returns the expected cardinality of this scan
	table_function_cardinality_t cardinality;
	//! (Optional) pushdown a set of arbitrary filter expressions, rather than only simple comparisons with a constant
	//! Any functions remaining in the expression list will be pushed as a regular filter after the scan
	table_function_pushdown_complex_filter_t pushdown_complex_filter;
	//! (Optional) function for rendering the operator to a string in profiling output
	table_function_to_string_t to_string;
	//! (Optional) function that returns the maximum amount of threads that can work on this task
	table_function_max_threads_t max_threads;
	//! (Optional) initialize the parallel scan state, called once in total.
	table_function_init_parallel_state_t init_parallel_state;
	//! (Optional) Parallel version of the main function
	table_function_parallel_t parallel_function;
	//! (Optional) initialize the parallel scan given the parallel state. Called once per task. Return nullptr if there
	//! is nothing left to scan.
	table_function_init_parallel_t parallel_init;
	//! (Optional) return the next chunk to process in the parallel scan, or return nullptr if there is none
	table_function_parallel_state_next_t parallel_state_next;
	//! (Optional) return how much of the table we have scanned up to this point (% of the data)
	table_function_progress_t table_scan_progress;
	//! Whether or not the table function supports projection pushdown. If not supported a projection will be added
	//! that filters out unused columns.
	bool projection_pushdown;
	//! Whether or not the table function supports filter pushdown. If not supported a filter will be added
	//! that applies the table filter directly.
	bool filter_pushdown;
	//! Additional function info, passed to the bind
	shared_ptr<TableFunctionInfo> function_info;
};

} // namespace duckdb
