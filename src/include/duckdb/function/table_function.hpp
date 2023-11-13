//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

#include <functional>

namespace duckdb {

class BaseStatistics;
class DependencyList;
class LogicalGet;
class TableFilterSet;

struct TableFunctionInfo {
	DUCKDB_API virtual ~TableFunctionInfo();

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct GlobalTableFunctionState {
public:
	// value returned from MaxThreads when as many threads as possible should be used
	constexpr static const int64_t MAX_THREADS = 999999999;

public:
	DUCKDB_API virtual ~GlobalTableFunctionState();

	virtual idx_t MaxThreads() const {
		return 1;
	}

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct LocalTableFunctionState {
	DUCKDB_API virtual ~LocalTableFunctionState();

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct TableFunctionBindInput {
	TableFunctionBindInput(vector<Value> &inputs, named_parameter_map_t &named_parameters,
	                       vector<LogicalType> &input_table_types, vector<string> &input_table_names,
	                       optional_ptr<TableFunctionInfo> info)
	    : inputs(inputs), named_parameters(named_parameters), input_table_types(input_table_types),
	      input_table_names(input_table_names), info(info) {
	}

	vector<Value> &inputs;
	named_parameter_map_t &named_parameters;
	vector<LogicalType> &input_table_types;
	vector<string> &input_table_names;
	optional_ptr<TableFunctionInfo> info;
};

struct TableFunctionInitInput {
	TableFunctionInitInput(optional_ptr<const FunctionData> bind_data_p, const vector<column_t> &column_ids_p,
	                       const vector<idx_t> &projection_ids_p, optional_ptr<TableFilterSet> filters_p)
	    : bind_data(bind_data_p), column_ids(column_ids_p), projection_ids(projection_ids_p), filters(filters_p) {
	}

	optional_ptr<const FunctionData> bind_data;
	const vector<column_t> &column_ids;
	const vector<idx_t> projection_ids;
	optional_ptr<TableFilterSet> filters;

	bool CanRemoveFilterColumns() const {
		if (projection_ids.empty()) {
			// Not set, can't remove filter columns
			return false;
		} else if (projection_ids.size() == column_ids.size()) {
			// Filter column is used in remainder of plan, can't remove
			return false;
		} else {
			// Less columns need to be projected out than that we scan
			return true;
		}
	}
};

struct TableFunctionInput {
public:
	TableFunctionInput(optional_ptr<const FunctionData> bind_data_p,
	                   optional_ptr<LocalTableFunctionState> local_state_p,
	                   optional_ptr<GlobalTableFunctionState> global_state_p)
	    : bind_data(bind_data_p), local_state(local_state_p), global_state(global_state_p) {
	}

public:
	optional_ptr<const FunctionData> bind_data;
	optional_ptr<LocalTableFunctionState> local_state;
	optional_ptr<GlobalTableFunctionState> global_state;
};

enum ScanType { TABLE, PARQUET };

struct BindInfo {
public:
	explicit BindInfo(ScanType type_p) : type(type_p) {};
	unordered_map<string, Value> options;
	ScanType type;
	void InsertOption(const string &name, Value value) {
		if (options.find(name) != options.end()) {
			throw InternalException("This option already exists");
		}
		options[name] = std::move(value);
	}
	template <class T>
	T GetOption(const string &name) {
		if (options.find(name) == options.end()) {
			throw InternalException("This option does not exist");
		}
		return options[name].GetValue<T>();
	}
	template <class T>
	vector<T> GetOptionList(const string &name) {
		if (options.find(name) == options.end()) {
			throw InternalException("This option does not exist");
		}
		auto option = options[name];
		if (option.type().id() != LogicalTypeId::LIST) {
			throw InternalException("This option is not a list");
		}
		vector<T> result;
		auto list_children = ListValue::GetChildren(option);
		for (auto &child : list_children) {
			result.emplace_back(child.GetValue<T>());
		}
		return result;
	}
};

typedef unique_ptr<FunctionData> (*table_function_bind_t)(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names);
typedef unique_ptr<TableRef> (*table_function_bind_replace_t)(ClientContext &context, TableFunctionBindInput &input);
typedef unique_ptr<GlobalTableFunctionState> (*table_function_init_global_t)(ClientContext &context,
                                                                             TableFunctionInitInput &input);
typedef unique_ptr<LocalTableFunctionState> (*table_function_init_local_t)(ExecutionContext &context,
                                                                           TableFunctionInitInput &input,
                                                                           GlobalTableFunctionState *global_state);
typedef unique_ptr<BaseStatistics> (*table_statistics_t)(ClientContext &context, const FunctionData *bind_data,
                                                         column_t column_index);
typedef void (*table_function_t)(ClientContext &context, TableFunctionInput &data, DataChunk &output);
typedef OperatorResultType (*table_in_out_function_t)(ExecutionContext &context, TableFunctionInput &data,
                                                      DataChunk &input, DataChunk &output);
typedef OperatorFinalizeResultType (*table_in_out_function_final_t)(ExecutionContext &context, TableFunctionInput &data,
                                                                    DataChunk &output);
typedef idx_t (*table_function_get_batch_index_t)(ClientContext &context, const FunctionData *bind_data,
                                                  LocalTableFunctionState *local_state,
                                                  GlobalTableFunctionState *global_state);

typedef BindInfo (*table_function_get_bind_info)(const FunctionData *bind_data);

typedef double (*table_function_progress_t)(ClientContext &context, const FunctionData *bind_data,
                                            const GlobalTableFunctionState *global_state);
typedef void (*table_function_dependency_t)(DependencyList &dependencies, const FunctionData *bind_data);
typedef unique_ptr<NodeStatistics> (*table_function_cardinality_t)(ClientContext &context,
                                                                   const FunctionData *bind_data);
typedef void (*table_function_pushdown_complex_filter_t)(ClientContext &context, LogicalGet &get,
                                                         FunctionData *bind_data,
                                                         vector<unique_ptr<Expression>> &filters);
typedef string (*table_function_to_string_t)(const FunctionData *bind_data);

typedef void (*table_function_serialize_t)(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                           const TableFunction &function);
typedef unique_ptr<FunctionData> (*table_function_deserialize_t)(Deserializer &deserializer, TableFunction &function);

class TableFunction : public SimpleNamedParameterFunction {
public:
	DUCKDB_API
	TableFunction(string name, vector<LogicalType> arguments, table_function_t function,
	              table_function_bind_t bind = nullptr, table_function_init_global_t init_global = nullptr,
	              table_function_init_local_t init_local = nullptr);
	DUCKDB_API
	TableFunction(const vector<LogicalType> &arguments, table_function_t function, table_function_bind_t bind = nullptr,
	              table_function_init_global_t init_global = nullptr, table_function_init_local_t init_local = nullptr);
	DUCKDB_API TableFunction();

	//! Bind function
	//! This function is used for determining the return type of a table producing function and returning bind data
	//! The returned FunctionData object should be constant and should not be changed during execution.
	table_function_bind_t bind;
	//! (Optional) Bind replace function
	//! This function is called before the regular bind function. It allows returning a TableRef will be used to
	//! to generate a logical plan that replaces the LogicalGet of a regularly bound TableFunction. The BindReplace can
	//! also return a nullptr to indicate a regular bind needs to be performed instead.
	table_function_bind_replace_t bind_replace;
	//! (Optional) global init function
	//! Initialize the global operator state of the function.
	//! The global operator state is used to keep track of the progress in the table function and is shared between
	//! all threads working on the table function.
	table_function_init_global_t init_global;
	//! (Optional) local init function
	//! Initialize the local operator state of the function.
	//! The local operator state is used to keep track of the progress in the table function and is thread-local.
	table_function_init_local_t init_local;
	//! The main function
	table_function_t function;
	//! The table in-out function (if this is an in-out function)
	table_in_out_function_t in_out_function;
	//! The table in-out final function (if this is an in-out function)
	table_in_out_function_final_t in_out_function_final;
	//! (Optional) statistics function
	//! Returns the statistics of a specified column
	table_statistics_t statistics;
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
	//! (Optional) return how much of the table we have scanned up to this point (% of the data)
	table_function_progress_t table_scan_progress;
	//! (Optional) returns the current batch index of the current scan operator
	table_function_get_batch_index_t get_batch_index;
	//! (Optional) returns the extra batch info, currently only used for the substrait extension
	table_function_get_bind_info get_batch_info;

	table_function_serialize_t serialize;
	table_function_deserialize_t deserialize;
	bool verify_serialization = true;

	//! Whether or not the table function supports projection pushdown. If not supported a projection will be added
	//! that filters out unused columns.
	bool projection_pushdown;
	//! Whether or not the table function supports filter pushdown. If not supported a filter will be added
	//! that applies the table filter directly.
	bool filter_pushdown;
	//! Whether or not the table function can immediately prune out filter columns that are unused in the remainder of
	//! the query plan, e.g., "SELECT i FROM tbl WHERE j = 42;" - j does not need to leave the table function at all
	bool filter_prune;
	//! Additional function info, passed to the bind
	shared_ptr<TableFunctionInfo> function_info;

	DUCKDB_API bool Equal(const TableFunction &rhs) const;
};

} // namespace duckdb
