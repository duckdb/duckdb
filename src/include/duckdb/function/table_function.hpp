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
#include "duckdb/common/column_index.hpp"

#include <functional>

namespace duckdb {

class BaseStatistics;
class LogicalDependencyList;
class LogicalGet;
class TableFunction;
class TableFilterSet;
class TableCatalogEntry;
struct MultiFileReader;
struct OperatorPartitionData;
struct OperatorPartitionInfo;

struct TableFunctionInfo {
	DUCKDB_API virtual ~TableFunctionInfo();

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
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
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct LocalTableFunctionState {
	DUCKDB_API virtual ~LocalTableFunctionState();

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct TableFunctionBindInput {
	TableFunctionBindInput(vector<Value> &inputs, named_parameter_map_t &named_parameters,
	                       vector<LogicalType> &input_table_types, vector<string> &input_table_names,
	                       optional_ptr<TableFunctionInfo> info, optional_ptr<Binder> binder,
	                       TableFunction &table_function, const TableFunctionRef &ref)
	    : inputs(inputs), named_parameters(named_parameters), input_table_types(input_table_types),
	      input_table_names(input_table_names), info(info), binder(binder), table_function(table_function), ref(ref) {
	}

	vector<Value> &inputs;
	named_parameter_map_t &named_parameters;
	vector<LogicalType> &input_table_types;
	vector<string> &input_table_names;
	optional_ptr<TableFunctionInfo> info;
	optional_ptr<Binder> binder;
	TableFunction &table_function;
	const TableFunctionRef &ref;
};

struct TableFunctionInitInput {
	TableFunctionInitInput(optional_ptr<const FunctionData> bind_data_p, vector<column_t> column_ids_p,
	                       const vector<idx_t> &projection_ids_p, optional_ptr<TableFilterSet> filters_p,
	                       optional_ptr<SampleOptions> sample_options_p = nullptr)
	    : bind_data(bind_data_p), column_ids(std::move(column_ids_p)), projection_ids(projection_ids_p),
	      filters(filters_p), sample_options(sample_options_p) {
		for (auto &col_id : column_ids) {
			column_indexes.emplace_back(col_id);
		}
	}
	TableFunctionInitInput(optional_ptr<const FunctionData> bind_data_p, vector<ColumnIndex> column_indexes_p,
	                       const vector<idx_t> &projection_ids_p, optional_ptr<TableFilterSet> filters_p,
	                       optional_ptr<SampleOptions> sample_options_p = nullptr)
	    : bind_data(bind_data_p), column_indexes(std::move(column_indexes_p)), projection_ids(projection_ids_p),
	      filters(filters_p), sample_options(sample_options_p) {
		for (auto &col_id : column_indexes) {
			column_ids.emplace_back(col_id.GetPrimaryIndex());
		}
	}

	optional_ptr<const FunctionData> bind_data;
	vector<column_t> column_ids;
	vector<ColumnIndex> column_indexes;
	const vector<idx_t> projection_ids;
	optional_ptr<TableFilterSet> filters;
	optional_ptr<SampleOptions> sample_options;

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

struct TableFunctionPartitionInput {
	TableFunctionPartitionInput(optional_ptr<const FunctionData> bind_data_p, const vector<column_t> &partition_ids)
	    : bind_data(bind_data_p), partition_ids(partition_ids) {
	}

	optional_ptr<const FunctionData> bind_data;
	const vector<column_t> &partition_ids;
};

struct TableFunctionToStringInput {
	TableFunctionToStringInput(const TableFunction &table_function_p, optional_ptr<const FunctionData> bind_data_p)
	    : table_function(table_function_p), bind_data(bind_data_p) {
	}
	const TableFunction &table_function;
	optional_ptr<const FunctionData> bind_data;
};

struct TableFunctionGetPartitionInput {
public:
	TableFunctionGetPartitionInput(optional_ptr<const FunctionData> bind_data_p,
	                               optional_ptr<LocalTableFunctionState> local_state_p,
	                               optional_ptr<GlobalTableFunctionState> global_state_p,
	                               const OperatorPartitionInfo &partition_info_p)
	    : bind_data(bind_data_p), local_state(local_state_p), global_state(global_state_p),
	      partition_info(partition_info_p) {
	}

public:
	optional_ptr<const FunctionData> bind_data;
	optional_ptr<LocalTableFunctionState> local_state;
	optional_ptr<GlobalTableFunctionState> global_state;
	const OperatorPartitionInfo &partition_info;
};

enum class ScanType : uint8_t { TABLE, PARQUET, EXTERNAL };

struct BindInfo {
public:
	explicit BindInfo(ScanType type_p) : type(type_p) {};
	explicit BindInfo(TableCatalogEntry &table) : type(ScanType::TABLE), table(&table) {};

	unordered_map<string, Value> options;
	ScanType type;
	optional_ptr<TableCatalogEntry> table;

	void InsertOption(const string &name, Value value) { // NOLINT: work-around bug in clang-tidy
		if (options.find(name) != options.end()) {
			throw InternalException("This option already exists");
		}
		options.emplace(name, std::move(value));
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

//! How a table is partitioned by a given set of columns
enum class TablePartitionInfo : uint8_t {
	NOT_PARTITIONED,         // the table is not partitioned by the given set of columns
	SINGLE_VALUE_PARTITIONS, // each partition has exactly one unique value (e.g. bounds = [1,1][2,2][3,3])
	OVERLAPPING_PARTITIONS,  // the partitions overlap **only** at the boundaries (e.g. bounds = [1,2][2,3][3,4]
	DISJOINT_PARTITIONS      // the partitions are disjoint (e.g. bounds = [1,2][3,4][5,6])
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
typedef OperatorPartitionData (*table_function_get_partition_data_t)(ClientContext &context,
                                                                     TableFunctionGetPartitionInput &input);

typedef BindInfo (*table_function_get_bind_info_t)(const optional_ptr<FunctionData> bind_data);

typedef unique_ptr<MultiFileReader> (*table_function_get_multi_file_reader_t)(const TableFunction &);

typedef bool (*table_function_supports_pushdown_type_t)(const LogicalType &type);

typedef double (*table_function_progress_t)(ClientContext &context, const FunctionData *bind_data,
                                            const GlobalTableFunctionState *global_state);
typedef void (*table_function_dependency_t)(LogicalDependencyList &dependencies, const FunctionData *bind_data);
typedef unique_ptr<NodeStatistics> (*table_function_cardinality_t)(ClientContext &context,
                                                                   const FunctionData *bind_data);
typedef void (*table_function_pushdown_complex_filter_t)(ClientContext &context, LogicalGet &get,
                                                         FunctionData *bind_data,
                                                         vector<unique_ptr<Expression>> &filters);
typedef InsertionOrderPreservingMap<string> (*table_function_to_string_t)(TableFunctionToStringInput &input);

typedef void (*table_function_serialize_t)(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                           const TableFunction &function);
typedef unique_ptr<FunctionData> (*table_function_deserialize_t)(Deserializer &deserializer, TableFunction &function);

typedef void (*table_function_type_pushdown_t)(ClientContext &context, optional_ptr<FunctionData> bind_data,
                                               const unordered_map<idx_t, LogicalType> &new_column_types);

typedef TablePartitionInfo (*table_function_get_partition_info_t)(ClientContext &context,
                                                                  TableFunctionPartitionInput &input);

//! When to call init_global to initialize the table function
enum class TableFunctionInitialization { INITIALIZE_ON_EXECUTE, INITIALIZE_ON_SCHEDULE };

class TableFunction : public SimpleNamedParameterFunction { // NOLINT: work-around bug in clang-tidy
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
	//! This function is called before the regular bind function. It allows returning a TableRef that will be used to
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
	//! (Optional) returns the partition info of the current scan operator
	table_function_get_partition_data_t get_partition_data;
	//! (Optional) returns extra bind info
	table_function_get_bind_info_t get_bind_info;
	//! (Optional) pushes down type information to scanner, returns true if pushdown was successful
	table_function_type_pushdown_t type_pushdown;
	//! (Optional) allows injecting a custom MultiFileReader implementation
	table_function_get_multi_file_reader_t get_multi_file_reader;
	//! (Optional) If this scanner supports filter pushdown, but not to all data types
	table_function_supports_pushdown_type_t supports_pushdown_type;
	//! Get partition info of the table
	table_function_get_partition_info_t get_partition_info;

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
	//! Whether or not the table function supports sampling pushdown. If not supported a sample will be taken after the
	//! table function.
	bool sampling_pushdown;
	//! Additional function info, passed to the bind
	shared_ptr<TableFunctionInfo> function_info;

	//! When to call init_global
	//! By default init_global is called when the pipeline is ready for execution
	//! If this is set to `INITIALIZE_ON_SCHEDULE` the table function is initialized when the query is scheduled
	TableFunctionInitialization global_initialization = TableFunctionInitialization::INITIALIZE_ON_EXECUTE;

	DUCKDB_API bool Equal(const TableFunction &rhs) const;
};

} // namespace duckdb
