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
class OperatorTaskInfo;

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
typedef unique_ptr<FunctionOperatorData> (*table_function_init_t)(ClientContext &context, const FunctionData *bind_data, OperatorTaskInfo *task_info, vector<column_t> &column_ids, unordered_map<idx_t, vector<TableFilter>> &table_filters);
typedef void (*table_function_t)(ClientContext &context, const FunctionData *bind_data, FunctionOperatorData *operator_state, DataChunk &output);
typedef void (*table_function_cleanup_t)(ClientContext &context, const FunctionData *bind_data, FunctionOperatorData *operator_state);
typedef void (*table_function_parallel_t)(ClientContext &context, const FunctionData *bind_data, vector<column_t> &column_ids, unordered_map<idx_t, vector<TableFilter>> &table_filters, std::function<void(unique_ptr<OperatorTaskInfo>)> callback);
typedef string (*table_function_to_string_t)(const FunctionData *bind_data);

class TableFunction : public SimpleFunction {
public:
	TableFunction(string name, vector<LogicalType> arguments, table_function_t function)
	    : SimpleFunction(name, move(arguments)), bind(nullptr), init(nullptr), function(function), cleanup(nullptr),
		  parallel_tasks(nullptr), to_string(nullptr), projection_pushdown(false), filter_pushdown(false) {
	}
	TableFunction(vector<LogicalType> arguments, table_function_t function)
	    : TableFunction(string(), move(arguments), function) {
	}

	//! (Optional) Bind function
	//! This function is used for determining the return type of a table producing function and returning bind data
	//! The returned FunctionData object should be constant and should not be changed during execution.
	table_function_bind_t bind;
	//! (Optional) init function
	//! Initialize the operator state of the function. The operator state is used to keep track of the progress in the table function.
	table_function_init_t init;
	//! The main function
	table_function_t function;
	//! (Optional) cleanup function
	//! The final cleanup function, called after all data is exhausted from the main function
	table_function_cleanup_t cleanup;
	//! (Optional) parallel task split
	//! The function used to split the table-producing function into parallel tasks
	table_function_parallel_t parallel_tasks;
	//! (Optional) function for rendering the operator to a string in profiling output
	table_function_to_string_t to_string;

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
