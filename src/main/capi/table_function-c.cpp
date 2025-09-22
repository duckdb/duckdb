#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Structures
//===--------------------------------------------------------------------===//
struct CTableFunctionInfo : public TableFunctionInfo {
	~CTableFunctionInfo() override {
		if (extra_info && delete_callback) {
			delete_callback(extra_info);
		}
		extra_info = nullptr;
		delete_callback = nullptr;
	}

	duckdb_table_function_bind_t bind = nullptr;
	duckdb_table_function_init_t init = nullptr;
	duckdb_table_function_init_t local_init = nullptr;
	duckdb_table_function_t function = nullptr;
	void *extra_info = nullptr;
	duckdb_delete_callback_t delete_callback = nullptr;
};

struct CTableBindData : public TableFunctionData {
	explicit CTableBindData(CTableFunctionInfo &info) : info(info) {
	}
	~CTableBindData() override {
		if (bind_data && delete_callback) {
			delete_callback(bind_data);
		}
		bind_data = nullptr;
		delete_callback = nullptr;
	}

	CTableFunctionInfo &info;
	void *bind_data = nullptr;
	duckdb_delete_callback_t delete_callback = nullptr;
	unique_ptr<NodeStatistics> stats;
};

struct CTableInternalBindInfo {
	CTableInternalBindInfo(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
	                       vector<string> &names, CTableBindData &bind_data, CTableFunctionInfo &function_info)
	    : context(context), input(input), return_types(return_types), names(names), bind_data(bind_data),
	      function_info(function_info), success(true) {
	}

	ClientContext &context;
	TableFunctionBindInput &input;
	vector<LogicalType> &return_types;
	vector<string> &names;
	CTableBindData &bind_data;
	CTableFunctionInfo &function_info;
	bool success;
	string error;
};

struct CTableInitData {
	~CTableInitData() {
		if (init_data && delete_callback) {
			delete_callback(init_data);
		}
		init_data = nullptr;
		delete_callback = nullptr;
	}

	void *init_data = nullptr;
	duckdb_delete_callback_t delete_callback = nullptr;
	idx_t max_threads = 1;
};

struct CTableGlobalInitData : public GlobalTableFunctionState {
	CTableInitData init_data;

	idx_t MaxThreads() const override {
		return init_data.max_threads;
	}
};

struct CTableLocalInitData : public LocalTableFunctionState {
	CTableInitData init_data;
};

struct CTableInternalInitInfo {
	CTableInternalInitInfo(const CTableBindData &bind_data, CTableInitData &init_data,
	                       const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters)
	    : bind_data(bind_data), init_data(init_data), column_ids(column_ids), filters(filters), success(true) {
	}

	const CTableBindData &bind_data;
	CTableInitData &init_data;
	const vector<column_t> &column_ids;
	optional_ptr<TableFilterSet> filters;
	bool success;
	string error;
};

struct CTableInternalFunctionInfo {
	CTableInternalFunctionInfo(const CTableBindData &bind_data, CTableInitData &init_data, CTableInitData &local_data)
	    : bind_data(bind_data), init_data(init_data), local_data(local_data), success(true) {
	}

	const CTableBindData &bind_data;
	CTableInitData &init_data;
	CTableInitData &local_data;
	bool success;
	string error;
};

struct CTableFilterNode {
	CTableFilterNode(idx_t column_index, TableFilterType filter_type)
	    : column_index(column_index), filter_type(filter_type), comparison_type(ExpressionType::INVALID),
	      has_comparison(false) {
	}

	idx_t column_index;
	TableFilterType filter_type;
	ExpressionType comparison_type;
	bool has_comparison;
	Value constant;
	vector<unique_ptr<CTableFilterNode>> children;

	unique_ptr<CTableFilterNode> Copy() const {
		auto result = make_uniq<CTableFilterNode>(column_index, filter_type);
		result->comparison_type = comparison_type;
		result->has_comparison = has_comparison;
		result->constant = constant;
		for (auto &child : children) {
			result->children.push_back(child->Copy());
		}
		return result;
	}
};

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//

duckdb::TableFunction &GetCTableFunction(duckdb_table_function function) {
	return *reinterpret_cast<duckdb::TableFunction *>(function);
}

duckdb::CTableInternalBindInfo &GetCTableFunctionBindInfo(duckdb_bind_info info) {
	D_ASSERT(info);
	return *reinterpret_cast<duckdb::CTableInternalBindInfo *>(info);
}

duckdb_bind_info ToCTableFunctionBindInfo(duckdb::CTableInternalBindInfo &info) {
	return reinterpret_cast<duckdb_bind_info>(&info);
}

duckdb::CTableInternalInitInfo &GetCInitInfo(duckdb_init_info info) {
	D_ASSERT(info);
	return *reinterpret_cast<duckdb::CTableInternalInitInfo *>(info);
}

duckdb_init_info ToCInitInfo(duckdb::CTableInternalInitInfo &info) {
	return reinterpret_cast<duckdb_init_info>(&info);
}

duckdb::CTableInternalFunctionInfo &GetCTableFunctionInfo(duckdb_function_info info) {
	D_ASSERT(info);
	return *reinterpret_cast<duckdb::CTableInternalFunctionInfo *>(info);
}

duckdb_function_info ToCTableFunctionInfo(duckdb::CTableInternalFunctionInfo &info) {
	return reinterpret_cast<duckdb_function_info>(&info);
}

static duckdb::CTableFilterNode *GetCTableFunctionFilter(duckdb_table_function_filter filter) {
	return reinterpret_cast<duckdb::CTableFilterNode *>(filter);
}

static duckdb_table_function_filter ToCTableFunctionFilter(duckdb::CTableFilterNode *filter) {
	return reinterpret_cast<duckdb_table_function_filter>(filter);
}

static bool ExpressionTypeToFilterOperator(ExpressionType type, duckdb_table_filter_operator &result) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		result = DUCKDB_TABLE_FILTER_OPERATOR_EQUAL;
		return true;
	case ExpressionType::COMPARE_NOTEQUAL:
		result = DUCKDB_TABLE_FILTER_OPERATOR_NOT_EQUAL;
		return true;
	case ExpressionType::COMPARE_GREATERTHAN:
		result = DUCKDB_TABLE_FILTER_OPERATOR_GREATER_THAN;
		return true;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		result = DUCKDB_TABLE_FILTER_OPERATOR_GREATER_THAN_OR_EQUAL;
		return true;
	case ExpressionType::COMPARE_LESSTHAN:
		result = DUCKDB_TABLE_FILTER_OPERATOR_LESS_THAN;
		return true;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		result = DUCKDB_TABLE_FILTER_OPERATOR_LESS_THAN_OR_EQUAL;
		return true;
	default:
		result = DUCKDB_TABLE_FILTER_OPERATOR_INVALID;
		return false;
	}
}

static duckdb_table_filter_type TableFilterTypeToFilterType(TableFilterType type) {
	switch (type) {
	case TableFilterType::CONSTANT_COMPARISON:
		return DUCKDB_TABLE_FILTER_TYPE_CONSTANT_COMPARISON;
	case TableFilterType::CONJUNCTION_AND:
		return DUCKDB_TABLE_FILTER_TYPE_CONJUNCTION_AND;
	default:
		return DUCKDB_TABLE_FILTER_TYPE_INVALID;
	}
}

static unique_ptr<CTableFilterNode> ExtractFilterNode(idx_t column_index, const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		duckdb_table_filter_operator op;
		if (!ExpressionTypeToFilterOperator(constant_filter.comparison_type, op)) {
			return nullptr;
		}
		auto result = make_uniq<CTableFilterNode>(column_index, TableFilterType::CONSTANT_COMPARISON);
		result->comparison_type = constant_filter.comparison_type;
		result->has_comparison = true;
		result->constant = constant_filter.constant;
		return result;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		auto result = make_uniq<CTableFilterNode>(column_index, TableFilterType::CONJUNCTION_AND);
		for (auto &child : and_filter.child_filters) {
			auto child_node = ExtractFilterNode(column_index, *child);
			if (!child_node) {
				return nullptr;
			}
			result->children.push_back(std::move(child_node));
		}
		return result;
	}
	default:
		return nullptr;
	}
}

static vector<unique_ptr<CTableFilterNode>> ExtractFilters(optional_ptr<TableFilterSet> filters) {
	vector<unique_ptr<CTableFilterNode>> result;
	if (!filters) {
		return result;
	}
	for (auto &entry : filters->filters) {
		if (!entry.second) {
			continue;
		}
		auto filter_node = ExtractFilterNode(entry.first, *entry.second);
		if (!filter_node) {
			continue;
		}
		result.push_back(std::move(filter_node));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Table Function Callbacks
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> CTableFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto &info = input.info->Cast<CTableFunctionInfo>();
	D_ASSERT(info.bind && info.function && info.init);

	auto result = make_uniq<CTableBindData>(info);
	CTableInternalBindInfo bind_info(context, input, return_types, names, *result, info);
	info.bind(ToCTableFunctionBindInfo(bind_info));
	if (!bind_info.success) {
		throw BinderException(bind_info.error);
	}

	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> CTableFunctionInit(ClientContext &context, TableFunctionInitInput &data_p) {
	auto &bind_data = data_p.bind_data->Cast<CTableBindData>();
	auto result = make_uniq<CTableGlobalInitData>();

	CTableInternalInitInfo init_info(bind_data, result->init_data, data_p.column_ids, data_p.filters);
	bind_data.info.init(ToCInitInfo(init_info));
	if (!init_info.success) {
		throw InvalidInputException(init_info.error);
	}
	return std::move(result);
}

unique_ptr<LocalTableFunctionState> CTableFunctionLocalInit(ExecutionContext &context, TableFunctionInitInput &data_p,
                                                            GlobalTableFunctionState *gstate) {
	auto &bind_data = data_p.bind_data->Cast<CTableBindData>();
	auto result = make_uniq<CTableLocalInitData>();
	if (!bind_data.info.local_init) {
		return std::move(result);
	}

	CTableInternalInitInfo init_info(bind_data, result->init_data, data_p.column_ids, data_p.filters);
	bind_data.info.local_init(ToCInitInfo(init_info));
	if (!init_info.success) {
		throw InvalidInputException(init_info.error);
	}
	return std::move(result);
}

unique_ptr<NodeStatistics> CTableFunctionCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<CTableBindData>();
	if (!bind_data.stats) {
		return nullptr;
	}
	return make_uniq<NodeStatistics>(*bind_data.stats);
}

void CTableFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<CTableBindData>();
	auto &global_data = data_p.global_state->Cast<CTableGlobalInitData>();
	auto &local_data = data_p.local_state->Cast<CTableLocalInitData>();
	CTableInternalFunctionInfo function_info(bind_data, global_data.init_data, local_data.init_data);
	bind_data.info.function(ToCTableFunctionInfo(function_info), reinterpret_cast<duckdb_data_chunk>(&output));
	if (!function_info.success) {
		throw InvalidInputException(function_info.error);
	}
}

} // namespace duckdb

//===--------------------------------------------------------------------===//
// Table Function
//===--------------------------------------------------------------------===//
using duckdb::GetCTableFunction;

duckdb_table_function duckdb_create_table_function() {
	auto function = new duckdb::TableFunction("", {}, duckdb::CTableFunction, duckdb::CTableFunctionBind,
	                                          duckdb::CTableFunctionInit, duckdb::CTableFunctionLocalInit);
	function->function_info = duckdb::make_shared_ptr<duckdb::CTableFunctionInfo>();
	function->cardinality = duckdb::CTableFunctionCardinality;
	return reinterpret_cast<duckdb_table_function>(function);
}

void duckdb_destroy_table_function(duckdb_table_function *function) {
	if (function && *function) {
		auto tf = reinterpret_cast<duckdb::TableFunction *>(*function);
		delete tf;
		*function = nullptr;
	}
}

void duckdb_table_function_set_name(duckdb_table_function function, const char *name) {
	if (!function || !name) {
		return;
	}
	auto &tf = GetCTableFunction(function);
	tf.name = name;
}

void duckdb_table_function_add_parameter(duckdb_table_function function, duckdb_logical_type type) {
	if (!function || !type) {
		return;
	}
	auto &tf = GetCTableFunction(function);
	auto logical_type = reinterpret_cast<duckdb::LogicalType *>(type);
	tf.arguments.push_back(*logical_type);
}

void duckdb_table_function_add_named_parameter(duckdb_table_function function, const char *name,
                                               duckdb_logical_type type) {
	if (!function || !type) {
		return;
	}
	auto &tf = GetCTableFunction(function);
	auto logical_type = reinterpret_cast<duckdb::LogicalType *>(type);
	tf.named_parameters.insert({name, *logical_type});
}

void duckdb_table_function_set_extra_info(duckdb_table_function function, void *extra_info,
                                          duckdb_delete_callback_t destroy) {
	if (!function) {
		return;
	}
	auto &tf = GetCTableFunction(function);
	auto &info = tf.function_info->Cast<duckdb::CTableFunctionInfo>();
	info.extra_info = extra_info;
	info.delete_callback = destroy;
}

void duckdb_table_function_set_bind(duckdb_table_function function, duckdb_table_function_bind_t bind) {
	if (!function || !bind) {
		return;
	}
	auto &tf = GetCTableFunction(function);
	auto &info = tf.function_info->Cast<duckdb::CTableFunctionInfo>();
	info.bind = bind;
}

void duckdb_table_function_set_init(duckdb_table_function function, duckdb_table_function_init_t init) {
	if (!function || !init) {
		return;
	}
	auto &tf = GetCTableFunction(function);
	auto &info = tf.function_info->Cast<duckdb::CTableFunctionInfo>();
	info.init = init;
}

void duckdb_table_function_set_local_init(duckdb_table_function function, duckdb_table_function_init_t init) {
	if (!function || !init) {
		return;
	}
	auto &tf = GetCTableFunction(function);
	auto &info = tf.function_info->Cast<duckdb::CTableFunctionInfo>();
	info.local_init = init;
}

void duckdb_table_function_set_function(duckdb_table_function table_function, duckdb_table_function_t function) {
	if (!table_function || !function) {
		return;
	}
	auto &tf = GetCTableFunction(table_function);
	auto &info = tf.function_info->Cast<duckdb::CTableFunctionInfo>();
	info.function = function;
}

void duckdb_table_function_supports_projection_pushdown(duckdb_table_function table_function, bool pushdown) {
	if (!table_function) {
		return;
	}
	auto &tf = GetCTableFunction(table_function);
	tf.projection_pushdown = pushdown;
}

void duckdb_table_function_supports_filter_pushdown(duckdb_table_function table_function, bool pushdown) {
	if (!table_function) {
		return;
	}
	auto &tf = GetCTableFunction(table_function);
	tf.filter_pushdown = pushdown;
}

duckdb_state duckdb_register_table_function(duckdb_connection connection, duckdb_table_function function) {
	if (!connection || !function) {
		return DuckDBError;
	}
	auto con = reinterpret_cast<duckdb::Connection *>(connection);
	auto &tf = GetCTableFunction(function);
	auto &info = tf.function_info->Cast<duckdb::CTableFunctionInfo>();

	if (tf.name.empty() || !info.bind || !info.init || !info.function) {
		return DuckDBError;
	}
	for (auto it = tf.named_parameters.begin(); it != tf.named_parameters.end(); it++) {
		if (duckdb::TypeVisitor::Contains(it->second, duckdb::LogicalTypeId::INVALID)) {
			return DuckDBError;
		}
	}
	for (const auto &argument : tf.arguments) {
		if (duckdb::TypeVisitor::Contains(argument, duckdb::LogicalTypeId::INVALID)) {
			return DuckDBError;
		}
	}

	try {
		con->context->RunFunctionInTransaction([&]() {
			auto &catalog = duckdb::Catalog::GetSystemCatalog(*con->context);
			duckdb::CreateTableFunctionInfo tf_info(tf);
			tf_info.on_conflict = duckdb::OnCreateConflict::ALTER_ON_CONFLICT;
			catalog.CreateTableFunction(*con->context, tf_info);
		});
	} catch (...) { // LCOV_EXCL_START
		return DuckDBError;
	} // LCOV_EXCL_STOP
	return DuckDBSuccess;
}

//===--------------------------------------------------------------------===//
// Bind Interface
//===--------------------------------------------------------------------===//
using duckdb::GetCTableFunctionBindInfo;

void *duckdb_bind_get_extra_info(duckdb_bind_info info) {
	if (!info) {
		return nullptr;
	}
	auto &bind_info = GetCTableFunctionBindInfo(info);
	return bind_info.function_info.extra_info;
}

void duckdb_table_function_get_client_context(duckdb_bind_info info, duckdb_client_context *out_context) {
	if (!info || !out_context) {
		return;
	}
	auto &bind_info = GetCTableFunctionBindInfo(info);
	auto wrapper = new duckdb::CClientContextWrapper(bind_info.context);
	*out_context = reinterpret_cast<duckdb_client_context>(wrapper);
}

void duckdb_bind_add_result_column(duckdb_bind_info info, const char *name, duckdb_logical_type type) {
	if (!info || !name || !type) {
		return;
	}
	auto logical_type = reinterpret_cast<duckdb::LogicalType *>(type);
	if (duckdb::TypeVisitor::Contains(*logical_type, duckdb::LogicalTypeId::INVALID) ||
	    duckdb::TypeVisitor::Contains(*logical_type, duckdb::LogicalTypeId::ANY)) {
		return;
	}

	auto &bind_info = GetCTableFunctionBindInfo(info);
	bind_info.names.push_back(name);
	bind_info.return_types.push_back(*logical_type);
}

idx_t duckdb_bind_get_parameter_count(duckdb_bind_info info) {
	if (!info) {
		return 0;
	}
	auto &bind_info = GetCTableFunctionBindInfo(info);
	return bind_info.input.inputs.size();
}

duckdb_value duckdb_bind_get_parameter(duckdb_bind_info info, idx_t index) {
	if (!info || index >= duckdb_bind_get_parameter_count(info)) {
		return nullptr;
	}
	auto &bind_info = GetCTableFunctionBindInfo(info);
	return reinterpret_cast<duckdb_value>(new duckdb::Value(bind_info.input.inputs[index]));
}

duckdb_value duckdb_bind_get_named_parameter(duckdb_bind_info info, const char *name) {
	if (!info || !name) {
		return nullptr;
	}
	auto &bind_info = GetCTableFunctionBindInfo(info);
	auto t = bind_info.input.named_parameters.find(name);
	if (t == bind_info.input.named_parameters.end()) {
		return nullptr;
	} else {
		return reinterpret_cast<duckdb_value>(new duckdb::Value(t->second));
	}
}

void duckdb_bind_set_bind_data(duckdb_bind_info info, void *bind_data, duckdb_delete_callback_t destroy) {
	if (!info) {
		return;
	}
	auto &bind_info = GetCTableFunctionBindInfo(info);
	bind_info.bind_data.bind_data = bind_data;
	bind_info.bind_data.delete_callback = destroy;
}

void duckdb_bind_set_cardinality(duckdb_bind_info info, idx_t cardinality, bool is_exact) {
	if (!info) {
		return;
	}
	auto &bind_info = GetCTableFunctionBindInfo(info);
	if (is_exact) {
		bind_info.bind_data.stats = duckdb::make_uniq<duckdb::NodeStatistics>(cardinality);
	} else {
		bind_info.bind_data.stats = duckdb::make_uniq<duckdb::NodeStatistics>(cardinality, cardinality);
	}
}

void duckdb_bind_set_error(duckdb_bind_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto &bind_info = GetCTableFunctionBindInfo(info);
	bind_info.error = error;
	bind_info.success = false;
}

//===--------------------------------------------------------------------===//
// Init Interface
//===--------------------------------------------------------------------===//
using duckdb::GetCInitInfo;

void *duckdb_init_get_extra_info(duckdb_init_info info) {
	if (!info) {
		return nullptr;
	}
	auto init_info = reinterpret_cast<duckdb::CTableInternalInitInfo *>(info);
	return init_info->bind_data.info.extra_info;
}

void *duckdb_init_get_bind_data(duckdb_init_info info) {
	if (!info) {
		return nullptr;
	}
	auto &init_info = GetCInitInfo(info);
	return init_info.bind_data.bind_data;
}

void duckdb_init_set_init_data(duckdb_init_info info, void *init_data, duckdb_delete_callback_t destroy) {
	if (!info) {
		return;
	}
	auto &init_info = GetCInitInfo(info);
	init_info.init_data.init_data = init_data;
	init_info.init_data.delete_callback = destroy;
}

void duckdb_init_set_error(duckdb_init_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto &init_info = GetCInitInfo(info);
	init_info.error = error;
	init_info.success = false;
}

idx_t duckdb_init_get_column_count(duckdb_init_info info) {
	if (!info) {
		return 0;
	}
	auto &init_info = GetCInitInfo(info);
	return init_info.column_ids.size();
}

idx_t duckdb_init_get_column_index(duckdb_init_info info, idx_t column_index) {
	if (!info) {
		return 0;
	}
	auto &init_info = GetCInitInfo(info);
	if (column_index >= init_info.column_ids.size()) {
		return 0;
	}
	return init_info.column_ids[column_index];
}

idx_t duckdb_init_get_filter_count(duckdb_init_info info) {
	if (!info) {
		return 0;
	}
	auto &init_info = GetCInitInfo(info);
	auto filters = ExtractFilters(init_info.filters);
	return filters.size();
}

duckdb_state duckdb_init_get_filter(duckdb_init_info info, idx_t filter_index,
                                    duckdb_table_function_filter *out_filter) {
	if (!info || !out_filter) {
		return DuckDBError;
	}
	*out_filter = nullptr;

	auto &init_info = GetCInitInfo(info);
	auto filters = ExtractFilters(init_info.filters);
	if (filter_index >= filters.size()) {
		return DuckDBError;
	}
	auto filter_handle = filters[filter_index].release();
	*out_filter = ToCTableFunctionFilter(filter_handle);
	return DuckDBSuccess;
}

idx_t duckdb_table_function_filter_get_column_index(duckdb_table_function_filter filter) {
	if (!filter) {
		return 0;
	}
	auto *internal = duckdb::GetCTableFunctionFilter(filter);
	if (!internal) {
		return 0;
	}
	return internal->column_index;
}

duckdb_table_filter_type duckdb_table_function_filter_get_type(duckdb_table_function_filter filter) {
	if (!filter) {
		return DUCKDB_TABLE_FILTER_TYPE_INVALID;
	}
	auto *internal = duckdb::GetCTableFunctionFilter(filter);
	if (!internal) {
		return DUCKDB_TABLE_FILTER_TYPE_INVALID;
	}
	return TableFilterTypeToFilterType(internal->filter_type);
}

duckdb_table_filter_operator duckdb_table_function_filter_get_operator(duckdb_table_function_filter filter) {
	if (!filter) {
		return DUCKDB_TABLE_FILTER_OPERATOR_INVALID;
	}
	auto *internal = duckdb::GetCTableFunctionFilter(filter);
	if (!internal) {
		return DUCKDB_TABLE_FILTER_OPERATOR_INVALID;
	}
	if (!internal->has_comparison) {
		return DUCKDB_TABLE_FILTER_OPERATOR_INVALID;
	}
	duckdb_table_filter_operator result;
	if (!ExpressionTypeToFilterOperator(internal->comparison_type, result)) {
		return DUCKDB_TABLE_FILTER_OPERATOR_INVALID;
	}
	return result;
}

duckdb_value duckdb_table_function_filter_get_constant(duckdb_table_function_filter filter) {
	if (!filter) {
		return nullptr;
	}
	auto *internal = duckdb::GetCTableFunctionFilter(filter);
	if (!internal) {
		return nullptr;
	}
	if (!internal->has_comparison) {
		return nullptr;
	}
	return reinterpret_cast<duckdb_value>(new Value(internal->constant));
}

idx_t duckdb_table_function_filter_get_child_count(duckdb_table_function_filter filter) {
	if (!filter) {
		return 0;
	}
	auto *internal = duckdb::GetCTableFunctionFilter(filter);
	if (!internal) {
		return 0;
	}
	return internal->children.size();
}

duckdb_state duckdb_table_function_filter_get_child(duckdb_table_function_filter filter, idx_t index,
                                                    duckdb_table_function_filter *out_child) {
	if (!filter || !out_child) {
		return DuckDBError;
	}
	*out_child = nullptr;
	auto *internal = duckdb::GetCTableFunctionFilter(filter);
	if (!internal) {
		return DuckDBError;
	}
	if (index >= internal->children.size()) {
		return DuckDBError;
	}
	auto child_copy = internal->children[index]->Copy();
	if (!child_copy) {
		return DuckDBError;
	}
	*out_child = ToCTableFunctionFilter(child_copy.release());
	return DuckDBSuccess;
}

void duckdb_destroy_table_function_filter(duckdb_table_function_filter *filter) {
	if (!filter || !*filter) {
		return;
	}
	auto *internal = duckdb::GetCTableFunctionFilter(*filter);
	delete internal;
	*filter = nullptr;
}

void duckdb_init_set_max_threads(duckdb_init_info info, idx_t max_threads) {
	if (!info) {
		return;
	}
	auto &init_info = GetCInitInfo(info);
	init_info.init_data.max_threads = max_threads;
}

//===--------------------------------------------------------------------===//
// Function Interface
//===--------------------------------------------------------------------===//
using duckdb::GetCTableFunctionInfo;

void *duckdb_function_get_extra_info(duckdb_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto &function_info = GetCTableFunctionInfo(info);
	return function_info.bind_data.info.extra_info;
}

void *duckdb_function_get_bind_data(duckdb_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto &function_info = GetCTableFunctionInfo(info);
	return function_info.bind_data.bind_data;
}

void *duckdb_function_get_init_data(duckdb_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto &function_info = GetCTableFunctionInfo(info);
	return function_info.init_data.init_data;
}

void *duckdb_function_get_local_init_data(duckdb_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto &function_info = GetCTableFunctionInfo(info);
	return function_info.local_data.init_data;
}

void duckdb_function_set_error(duckdb_function_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto &function_info = GetCTableFunctionInfo(info);
	function_info.error = error;
	function_info.success = false;
}
