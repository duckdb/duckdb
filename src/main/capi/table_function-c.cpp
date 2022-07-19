#include "duckdb/main/capi_internal.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct CTableFunctionInfo : public TableFunctionInfo {
	~CTableFunctionInfo() {
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
	~CTableBindData() {
		if (bind_data && delete_callback) {
			delete_callback(bind_data);
		}
		bind_data = nullptr;
		delete_callback = nullptr;
	}

	CTableFunctionInfo *info = nullptr;
	void *bind_data = nullptr;
	duckdb_delete_callback_t delete_callback = nullptr;
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
	CTableInternalInitInfo(CTableBindData &bind_data, CTableInitData &init_data, const vector<column_t> &column_ids,
	                       TableFilterSet *filters)
	    : bind_data(bind_data), init_data(init_data), column_ids(column_ids), filters(filters), success(true) {
	}

	CTableBindData &bind_data;
	CTableInitData &init_data;
	const vector<column_t> &column_ids;
	TableFilterSet *filters;
	bool success;
	string error;
};

struct CTableInternalFunctionInfo {
	CTableInternalFunctionInfo(CTableBindData &bind_data, CTableInitData &init_data, CTableInitData &local_data)
	    : bind_data(bind_data), init_data(init_data), local_data(local_data), success(true) {
	}

	CTableBindData &bind_data;
	CTableInitData &init_data;
	CTableInitData &local_data;
	bool success;
	string error;
};

unique_ptr<FunctionData> CTableFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto info = (CTableFunctionInfo *)input.info;
	D_ASSERT(info->bind && info->function && info->init);
	auto result = make_unique<CTableBindData>();
	CTableInternalBindInfo bind_info(context, input, return_types, names, *result, *info);
	info->bind(&bind_info);
	if (!bind_info.success) {
		throw Exception(bind_info.error);
	}

	result->info = info;
	return move(result);
}

unique_ptr<GlobalTableFunctionState> CTableFunctionInit(ClientContext &context, TableFunctionInitInput &data_p) {
	auto &bind_data = (CTableBindData &)*data_p.bind_data;
	auto result = make_unique<CTableGlobalInitData>();

	CTableInternalInitInfo init_info(bind_data, result->init_data, data_p.column_ids, data_p.filters);
	bind_data.info->init(&init_info);
	if (!init_info.success) {
		throw Exception(init_info.error);
	}
	return move(result);
}

unique_ptr<LocalTableFunctionState> CTableFunctionLocalInit(ExecutionContext &context, TableFunctionInitInput &data_p,
                                                            GlobalTableFunctionState *gstate) {
	auto &bind_data = (CTableBindData &)*data_p.bind_data;
	auto result = make_unique<CTableLocalInitData>();
	if (!bind_data.info->local_init) {
		return move(result);
	}

	CTableInternalInitInfo init_info(bind_data, result->init_data, data_p.column_ids, data_p.filters);
	bind_data.info->local_init(&init_info);
	if (!init_info.success) {
		throw Exception(init_info.error);
	}
	return move(result);
}

void CTableFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = (CTableBindData &)*data_p.bind_data;
	auto &global_data = (CTableGlobalInitData &)*data_p.global_state;
	auto &local_data = (CTableLocalInitData &)*data_p.local_state;
	CTableInternalFunctionInfo function_info(bind_data, global_data.init_data, local_data.init_data);
	bind_data.info->function(&function_info, &output);
	if (!function_info.success) {
		throw Exception(function_info.error);
	}
}

} // namespace duckdb

//===--------------------------------------------------------------------===//
// Table Function
//===--------------------------------------------------------------------===//
duckdb_table_function duckdb_create_table_function() {
	auto function = new duckdb::TableFunction("", {}, duckdb::CTableFunction, duckdb::CTableFunctionBind,
	                                          duckdb::CTableFunctionInit, duckdb::CTableFunctionLocalInit);
	function->function_info = duckdb::make_shared<duckdb::CTableFunctionInfo>();
	return function;
}

void duckdb_destroy_table_function(duckdb_table_function *function) {
	if (function && *function) {
		auto tf = (duckdb::TableFunction *)*function;
		delete tf;
		*function = nullptr;
	}
}

void duckdb_table_function_set_name(duckdb_table_function function, const char *name) {
	if (!function || !name) {
		return;
	}
	auto tf = (duckdb::TableFunction *)function;
	tf->name = name;
}

void duckdb_table_function_add_parameter(duckdb_table_function function, duckdb_logical_type type) {
	if (!function || !type) {
		return;
	}
	auto tf = (duckdb::TableFunction *)function;
	auto logical_type = (duckdb::LogicalType *)type;
	tf->arguments.push_back(*logical_type);
}

void duckdb_table_function_set_extra_info(duckdb_table_function function, void *extra_info,
                                          duckdb_delete_callback_t destroy) {
	if (!function) {
		return;
	}
	auto tf = (duckdb::TableFunction *)function;
	auto info = (duckdb::CTableFunctionInfo *)tf->function_info.get();
	info->extra_info = extra_info;
	info->delete_callback = destroy;
}

void duckdb_table_function_set_bind(duckdb_table_function function, duckdb_table_function_bind_t bind) {
	if (!function || !bind) {
		return;
	}
	auto tf = (duckdb::TableFunction *)function;
	auto info = (duckdb::CTableFunctionInfo *)tf->function_info.get();
	info->bind = bind;
}

void duckdb_table_function_set_init(duckdb_table_function function, duckdb_table_function_init_t init) {
	if (!function || !init) {
		return;
	}
	auto tf = (duckdb::TableFunction *)function;
	auto info = (duckdb::CTableFunctionInfo *)tf->function_info.get();
	info->init = init;
}

void duckdb_table_function_set_local_init(duckdb_table_function function, duckdb_table_function_init_t init) {
	if (!function || !init) {
		return;
	}
	auto tf = (duckdb::TableFunction *)function;
	auto info = (duckdb::CTableFunctionInfo *)tf->function_info.get();
	info->local_init = init;
}

void duckdb_table_function_set_function(duckdb_table_function table_function, duckdb_table_function_t function) {
	if (!table_function || !function) {
		return;
	}
	auto tf = (duckdb::TableFunction *)table_function;
	auto info = (duckdb::CTableFunctionInfo *)tf->function_info.get();
	info->function = function;
}

void duckdb_table_function_supports_projection_pushdown(duckdb_table_function table_function, bool pushdown) {
	if (!table_function) {
		return;
	}
	auto tf = (duckdb::TableFunction *)table_function;
	tf->projection_pushdown = pushdown;
}

duckdb_state duckdb_register_table_function(duckdb_connection connection, duckdb_table_function function) {
	if (!connection || !function) {
		return DuckDBError;
	}
	auto con = (duckdb::Connection *)connection;
	auto tf = (duckdb::TableFunction *)function;
	auto info = (duckdb::CTableFunctionInfo *)tf->function_info.get();
	if (tf->name.empty() || !info->bind || !info->init || !info->function) {
		return DuckDBError;
	}
	con->context->RunFunctionInTransaction([&]() {
		auto &catalog = duckdb::Catalog::GetCatalog(*con->context);
		duckdb::CreateTableFunctionInfo tf_info(*tf);

		// create the function in the catalog
		catalog.CreateTableFunction(*con->context, &tf_info);
	});
	return DuckDBSuccess;
}

//===--------------------------------------------------------------------===//
// Bind Interface
//===--------------------------------------------------------------------===//
void *duckdb_bind_get_extra_info(duckdb_bind_info info) {
	if (!info) {
		return nullptr;
	}
	auto bind_info = (duckdb::CTableInternalBindInfo *)info;
	return bind_info->function_info.extra_info;
}

void duckdb_bind_add_result_column(duckdb_bind_info info, const char *name, duckdb_logical_type type) {
	if (!info || !name || !type) {
		return;
	}
	auto bind_info = (duckdb::CTableInternalBindInfo *)info;
	bind_info->names.push_back(name);
	bind_info->return_types.push_back(*((duckdb::LogicalType *)type));
}

idx_t duckdb_bind_get_parameter_count(duckdb_bind_info info) {
	if (!info) {
		return 0;
	}
	auto bind_info = (duckdb::CTableInternalBindInfo *)info;
	return bind_info->input.inputs.size();
}

duckdb_value duckdb_bind_get_parameter(duckdb_bind_info info, idx_t index) {
	if (!info || index >= duckdb_bind_get_parameter_count(info)) {
		return nullptr;
	}
	auto bind_info = (duckdb::CTableInternalBindInfo *)info;
	return new duckdb::Value(bind_info->input.inputs[index]);
}

void duckdb_bind_set_bind_data(duckdb_bind_info info, void *bind_data, duckdb_delete_callback_t destroy) {
	if (!info) {
		return;
	}
	auto bind_info = (duckdb::CTableInternalBindInfo *)info;
	bind_info->bind_data.bind_data = bind_data;
	bind_info->bind_data.delete_callback = destroy;
}

void duckdb_bind_set_error(duckdb_bind_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto function_info = (duckdb::CTableInternalBindInfo *)info;
	function_info->error = error;
	function_info->success = false;
}

//===--------------------------------------------------------------------===//
// Init Interface
//===--------------------------------------------------------------------===//
void *duckdb_init_get_extra_info(duckdb_init_info info) {
	if (!info) {
		return nullptr;
	}
	auto init_info = (duckdb::CTableInternalInitInfo *)info;
	return init_info->bind_data.info->extra_info;
}

void *duckdb_init_get_bind_data(duckdb_init_info info) {
	if (!info) {
		return nullptr;
	}
	auto init_info = (duckdb::CTableInternalInitInfo *)info;
	return init_info->bind_data.bind_data;
}

void duckdb_init_set_init_data(duckdb_init_info info, void *init_data, duckdb_delete_callback_t destroy) {
	if (!info) {
		return;
	}
	auto init_info = (duckdb::CTableInternalInitInfo *)info;
	init_info->init_data.init_data = init_data;
	init_info->init_data.delete_callback = destroy;
}

void duckdb_init_set_error(duckdb_init_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto function_info = (duckdb::CTableInternalInitInfo *)info;
	function_info->error = error;
	function_info->success = false;
}

idx_t duckdb_init_get_column_count(duckdb_init_info info) {
	if (!info) {
		return 0;
	}
	auto function_info = (duckdb::CTableInternalInitInfo *)info;
	return function_info->column_ids.size();
}

idx_t duckdb_init_get_column_index(duckdb_init_info info, idx_t column_index) {
	if (!info) {
		return 0;
	}
	auto function_info = (duckdb::CTableInternalInitInfo *)info;
	if (column_index >= function_info->column_ids.size()) {
		return 0;
	}
	return function_info->column_ids[column_index];
}

void duckdb_init_set_max_threads(duckdb_init_info info, idx_t max_threads) {
	if (!info) {
		return;
	}
	auto function_info = (duckdb::CTableInternalInitInfo *)info;
	function_info->init_data.max_threads = max_threads;
}

//===--------------------------------------------------------------------===//
// Function Interface
//===--------------------------------------------------------------------===//
void *duckdb_function_get_extra_info(duckdb_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto function_info = (duckdb::CTableInternalFunctionInfo *)info;
	return function_info->bind_data.info->extra_info;
}

void *duckdb_function_get_bind_data(duckdb_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto function_info = (duckdb::CTableInternalFunctionInfo *)info;
	return function_info->bind_data.bind_data;
}

void *duckdb_function_get_init_data(duckdb_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto function_info = (duckdb::CTableInternalFunctionInfo *)info;
	return function_info->init_data.init_data;
}

void *duckdb_function_get_local_init_data(duckdb_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto function_info = (duckdb::CTableInternalFunctionInfo *)info;
	return function_info->local_data.init_data;
}

void duckdb_function_set_error(duckdb_function_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto function_info = (duckdb::CTableInternalFunctionInfo *)info;
	function_info->error = error;
	function_info->success = false;
}
