//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/common/unordered_map.hpp"

#include <functional>

namespace duckdb {
class LogicalGet;
struct ParallelState;
class TableFilter;

struct FunctionOperatorData {
	virtual ~FunctionOperatorData() {
	}
};

//! TableFilter represents a filter pushed down into the table scan.
class TableFilter {
public:
	TableFilter(Value constant, ExpressionType comparison_type, idx_t column_index)
	    : constant(constant), comparison_type(comparison_type), column_index(column_index){};
	Value constant;
	ExpressionType comparison_type;
	idx_t column_index;
};

typedef unique_ptr<FunctionData> (*table_function_bind_t)(ClientContext &context, vector<Value> &inputs,
                                                          unordered_map<string, Value> &named_parameters,
                                                          vector<LogicalType> &return_types, vector<string> &names);
typedef unique_ptr<FunctionOperatorData> (*table_function_init_t)(
    ClientContext &context, const FunctionData *bind_data, ParallelState *state, vector<column_t> &column_ids,
    unordered_map<idx_t, vector<TableFilter>> &table_filters);
typedef void (*table_function_t)(ClientContext &context, const FunctionData *bind_data,
                                 FunctionOperatorData *operator_state, DataChunk &output);
typedef void (*table_function_cleanup_t)(ClientContext &context, const FunctionData *bind_data,
                                         FunctionOperatorData *operator_state);
typedef idx_t (*table_function_max_threads_t)(ClientContext &context, const FunctionData *bind_data);
typedef unique_ptr<ParallelState> (*table_function_init_parallel_state_t)(ClientContext &context,
                                                                          const FunctionData *bind_data);
typedef bool (*table_function_parallel_state_next_t)(ClientContext &context, const FunctionData *bind_data,
                                                     FunctionOperatorData *state, ParallelState *parallel_state);
typedef void (*table_function_dependency_t)(unordered_set<CatalogEntry *> &dependencies, const FunctionData *bind_data);
typedef idx_t (*table_function_cardinality_t)(const FunctionData *bind_data);
typedef void (*table_function_pushdown_complex_filter_t)(ClientContext &context, LogicalGet &get,
                                                         FunctionData *bind_data,
                                                         vector<unique_ptr<Expression>> &filters);
typedef string (*table_function_to_string_t)(const FunctionData *bind_data);

class TableFunction : public SimpleFunction {
public:
	TableFunction(string name, vector<LogicalType> arguments, table_function_t function,
	              table_function_bind_t bind = nullptr, table_function_init_t init = nullptr,
	              table_function_cleanup_t cleanup = nullptr, table_function_dependency_t dependency = nullptr,
	              table_function_cardinality_t cardinality = nullptr,
	              table_function_pushdown_complex_filter_t pushdown_complex_filter = nullptr,
	              table_function_to_string_t to_string = nullptr, table_function_max_threads_t max_threads = nullptr,
	              table_function_init_parallel_state_t init_parallel_state = nullptr,
	              table_function_parallel_state_next_t parallel_state_next = nullptr, bool projection_pushdown = false,
	              bool filter_pushdown = false)
	    : SimpleFunction(name, move(arguments)), bind(bind), init(init), function(function), cleanup(cleanup),
	      dependency(dependency), cardinality(cardinality), pushdown_complex_filter(pushdown_complex_filter),
	      to_string(to_string), max_threads(max_threads), init_parallel_state(init_parallel_state),
	      parallel_state_next(parallel_state_next), projection_pushdown(projection_pushdown),
	      filter_pushdown(filter_pushdown) {
	}
	TableFunction(vector<LogicalType> arguments, table_function_t function, table_function_bind_t bind = nullptr,
	              table_function_init_t init = nullptr, table_function_cleanup_t cleanup = nullptr,
	              table_function_dependency_t dependency = nullptr, table_function_cardinality_t cardinality = nullptr,
	              table_function_pushdown_complex_filter_t pushdown_complex_filter = nullptr,
	              table_function_to_string_t to_string = nullptr, table_function_max_threads_t max_threads = nullptr,
	              table_function_init_parallel_state_t init_parallel_state = nullptr,
	              table_function_parallel_state_next_t parallel_state_next = nullptr, bool projection_pushdown = false,
	              bool filter_pushdown = false)
	    : TableFunction(string(), move(arguments), function, bind, init, cleanup, dependency, cardinality,
	                    pushdown_complex_filter, to_string, max_threads, init_parallel_state, parallel_state_next,
	                    projection_pushdown, filter_pushdown) {
	}
	TableFunction() : SimpleFunction("", {}) {}

	//! (Optional) Bind function
	//! This function is used for determining the return type of a table producing function and returning bind data
	//! The returned FunctionData object should be constant and should not be changed during execution.
	table_function_bind_t bind;
	//! (Optional) init function
	//! Initialize the operator state of the function. The operator state is used to keep track of the progress in the
	//! table function.
	table_function_init_t init;
	//! The main function
	table_function_t function;
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
	//! (Optional) initialize the parallel scan state
	table_function_init_parallel_state_t init_parallel_state;
	//! (Optional) return the next chunk to process in the parallel scan, or return nullptr if there is none
	table_function_parallel_state_next_t parallel_state_next;

	//! Supported named parameters by the function
	unordered_map<string, LogicalType> named_parameters;
	//! Whether or not the table function supports projection pushdown. If not supported a projection will be added
	//! that filters out unused columns.
	bool projection_pushdown;
	//! Whether or not the table function supports filter pushdown. If not supported a filter will be added
	//! that applies the table filter directly.
	bool filter_pushdown;

	string ToString();
};

} // namespace duckdb
