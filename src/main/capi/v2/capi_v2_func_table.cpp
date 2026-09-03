#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

namespace duckdb::capiv2 {

class CV2TableFunctionInfo;

//! The bind data of a bound call site. Besides the user's own bind data it carries a borrowed pointer back to the
//! function info: the progress hook only receives the bind data, so it is its only route to the callbacks. The
//! registered function owns the info and outlives every call site bound against it.
class CV2TableFunctionData final : public FunctionData {
public:
	shared_ptr<CV2UserData> handle;
	optional_ptr<const CV2TableFunctionInfo> info;

	// The static estimate a bind callback may set via table_function_bind_set_cardinality.
	idx_t cardinality = 0;
	bool cardinality_is_exact = false;
	bool cardinality_set = false;

	// How many result columns the bind callback declared.
	idx_t column_count = 0;

	auto Copy() const -> unique_ptr<FunctionData> override {
		auto copy = make_uniq<CV2TableFunctionData>();
		copy->handle = handle;
		copy->info = info;
		copy->cardinality = cardinality;
		copy->cardinality_is_exact = cardinality_is_exact;
		copy->cardinality_set = cardinality_set;
		copy->column_count = column_count;
		return std::move(copy);
	}

	auto Equals(const FunctionData &other) const -> bool override {
		const auto &other_data = other.Cast<CV2TableFunctionData>();
		return handle && other_data.handle && handle->Equals(*other_data.handle);
	}
};

class CV2TableGlobalState final : public GlobalTableFunctionState {
public:
	auto MaxThreads() const -> idx_t override {
		return max_threads;
	}

	CV2UserData handle;
	idx_t max_threads = 1;

	// The declared column each vector of the output chunk stands for, kept here so the exec callback can ask.
	vector<column_t> scan_columns;
};

class CV2TableLocalState final : public LocalTableFunctionState {
public:
	CV2UserData handle;
};

class CV2TableBindInfo {
public:
	void *in_user_data = nullptr;
	const vector<Value> *in_args = nullptr;

	vector<LogicalType> out_column_types;
	vector<Identifier> out_column_names;
	duckdb_v2_opaque out_bind_data = {};
	idx_t out_cardinality = 0;
	bool out_cardinality_is_exact = false;
	bool out_cardinality_set = false;
};

static auto Convert(duckdb_v2_table_function_bind_info_handle info) -> CV2TableBindInfo * {
	return reinterpret_cast<CV2TableBindInfo *>(info);
}
static auto Convert(CV2TableBindInfo *info) -> duckdb_v2_table_function_bind_info_handle {
	return reinterpret_cast<duckdb_v2_table_function_bind_info_handle>(info);
}

class CV2TableInitGlobalInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	const vector<column_t> *in_scan_columns = nullptr;

	duckdb_v2_opaque out_global_state = {};
	idx_t out_max_threads = 1;
};

static auto Convert(duckdb_v2_table_function_init_global_info_handle info) -> CV2TableInitGlobalInfo * {
	return reinterpret_cast<CV2TableInitGlobalInfo *>(info);
}
static auto Convert(CV2TableInitGlobalInfo *info) -> duckdb_v2_table_function_init_global_info_handle {
	return reinterpret_cast<duckdb_v2_table_function_init_global_info_handle>(info);
}

class CV2TableInitLocalInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	void *in_global_state = nullptr;
	const vector<column_t> *in_scan_columns = nullptr;

	duckdb_v2_opaque out_local_state = {};
};

static auto Convert(duckdb_v2_table_function_init_local_info_handle info) -> CV2TableInitLocalInfo * {
	return reinterpret_cast<CV2TableInitLocalInfo *>(info);
}
static auto Convert(CV2TableInitLocalInfo *info) -> duckdb_v2_table_function_init_local_info_handle {
	return reinterpret_cast<duckdb_v2_table_function_init_local_info_handle>(info);
}

class CV2TableExecInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	void *in_global_state = nullptr;
	void *in_local_state = nullptr;
	const vector<column_t> *in_scan_columns = nullptr;

	DataChunk *output = nullptr;
};

static auto Convert(duckdb_v2_table_function_exec_info_handle info) -> CV2TableExecInfo * {
	return reinterpret_cast<CV2TableExecInfo *>(info);
}
static auto Convert(CV2TableExecInfo *info) -> duckdb_v2_table_function_exec_info_handle {
	return reinterpret_cast<duckdb_v2_table_function_exec_info_handle>(info);
}

class CV2TableProgressInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	void *in_global_state = nullptr;

	double out_progress = 0.0;
};

static auto Convert(duckdb_v2_table_function_progress_info_handle info) -> CV2TableProgressInfo * {
	return reinterpret_cast<CV2TableProgressInfo *>(info);
}
static auto Convert(CV2TableProgressInfo *info) -> duckdb_v2_table_function_progress_info_handle {
	return reinterpret_cast<duckdb_v2_table_function_progress_info_handle>(info);
}

class CV2TableFilterPushdownInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	// The predicates the optimizer offers, borrowed from the engine for the duration of the callback.
	const vector<unique_ptr<Expression>> *in_filters = nullptr;
	// What the column references inside the predicates index: the scan's column list at optimization time.
	const vector<ColumnIndex> *in_column_ids = nullptr;

	vector<bool> out_handled;
};

static auto Convert(duckdb_v2_table_function_filter_pushdown_info_handle info) -> CV2TableFilterPushdownInfo * {
	return reinterpret_cast<CV2TableFilterPushdownInfo *>(info);
}
static auto Convert(CV2TableFilterPushdownInfo *info) -> duckdb_v2_table_function_filter_pushdown_info_handle {
	return reinterpret_cast<duckdb_v2_table_function_filter_pushdown_info_handle>(info);
}

class CV2TableFunctionInfo : public TableFunctionInfo {
public:
	duckdb_v2_table_function_bind_callback_fn bind_cb = nullptr;
	duckdb_v2_table_function_init_global_callback_fn init_global_cb = nullptr;
	duckdb_v2_table_function_init_local_callback_fn init_local_cb = nullptr;
	duckdb_v2_table_function_exec_callback_fn exec_cb = nullptr;
	duckdb_v2_table_function_progress_callback_fn progress_cb = nullptr;
	duckdb_v2_table_function_filter_pushdown_callback_fn filter_pushdown_cb = nullptr;
	shared_ptr<CV2UserData> user_data = nullptr;
	bool projection_pushdown = false;

	// The signature's slot plan, captured at registration: every parameter name in signature order, how many of them
	// lead the positional prefix (the ones without a default), and the default of each remaining parameter. The bind
	// wrapper assembles the argument list from it, injecting the default for a parameter the call site omitted, so
	// the bind callback observes a value for every parameter.
	vector<Identifier> parameter_names;
	idx_t positional_count = 0;
	identifier_map_t<Value> named_parameter_defaults;
};

//! Assembles the call's arguments in signature-slot order: first the parameters without a default, taken from the
//! positional arguments, then the parameters with one, taken from the named arguments or the declared default when
//! the call site omitted them, then the variadic tail.
static auto CV2TableCollectArguments(const CV2TableFunctionInfo &info, TableFunctionBindInput &input) -> vector<Value> {
	vector<Value> arguments;
	const auto positional_count = MinValue<idx_t>(info.positional_count, input.inputs.size());

	for (idx_t i = 0; i < positional_count; i++) {
		arguments.push_back(input.inputs[i]);
	}
	for (idx_t i = info.positional_count; i < info.parameter_names.size(); i++) {
		const auto &name = info.parameter_names[i];
		auto provided = input.named_parameters.find(name);
		if (provided != input.named_parameters.end()) {
			arguments.push_back(provided->second);
		} else {
			arguments.push_back(info.named_parameter_defaults.at(name));
		}
	}
	for (idx_t i = positional_count; i < input.inputs.size(); i++) {
		arguments.push_back(input.inputs[i]);
	}
	return arguments;
}

static auto CV2TableBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                         vector<Identifier> &names) -> unique_ptr<FunctionData> {
	const auto &info = input.info->Cast<CV2TableFunctionInfo>();

	auto arguments = CV2TableCollectArguments(info, input);

	CV2TableBindInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_args = &arguments;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.bind_cb(Convert(&args), Convert(&context), &err_ptr);

	// Take ownership of whatever the callback set before reporting an error, so it is destroyed either way.
	auto result = make_uniq<CV2TableFunctionData>();
	result->info = &info;
	if (args.out_bind_data.ptr) {
		result->handle =
		    make_shared_ptr<CV2UserData>(args.out_bind_data.ptr, args.out_bind_data.destroy, args.out_bind_data.equals);
	}
	result->cardinality = args.out_cardinality;
	result->cardinality_is_exact = args.out_cardinality_is_exact;
	result->cardinality_set = args.out_cardinality_set;

	if (err.HasError()) {
		err.ThrowAsException();
	}

	if (args.out_column_types.empty()) {
		throw InvalidInputException("The bind callback of table function \"%s\" did not declare any result columns.",
		                            input.table_function.name);
	}

	result->column_count = args.out_column_types.size();
	return_types = std::move(args.out_column_types);
	names = std::move(args.out_column_names);
	return std::move(result);
}

//! The declared column behind each vector of the output chunk. With projection pushdown the engine sizes the chunk
//! to the columns it asked for; without it the chunk always holds every declared column, whatever the engine asked.
static auto CV2TableScanColumns(const CV2TableFunctionData &bind_data, const TableFunctionInitInput &input)
    -> vector<column_t> {
	if (bind_data.info->projection_pushdown) {
		return input.column_ids;
	}
	vector<column_t> columns;
	for (idx_t i = 0; i < bind_data.column_count; i++) {
		columns.push_back(i);
	}
	return columns;
}

static auto CV2TableInitGlobal(ClientContext &context, TableFunctionInitInput &input)
    -> unique_ptr<GlobalTableFunctionState> {
	const auto &bind_data = input.bind_data->Cast<CV2TableFunctionData>();
	const auto &info = *bind_data.info;

	// Always produced, even without a callback: it carries the thread count and column list the scan runs with.
	auto result = make_uniq<CV2TableGlobalState>();
	result->scan_columns = CV2TableScanColumns(bind_data, input);
	if (!info.init_global_cb) {
		return std::move(result);
	}

	CV2TableInitGlobalInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;
	args.in_scan_columns = &result->scan_columns;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.init_global_cb(Convert(&args), Convert(&context), &err_ptr);

	if (args.out_global_state.ptr) {
		result->handle =
		    CV2UserData(args.out_global_state.ptr, args.out_global_state.destroy, args.out_global_state.equals);
	}
	result->max_threads = args.out_max_threads;

	// Throw after taking ownership of the state, so that it is destroyed even if we error.
	if (err.HasError()) {
		err.ThrowAsException();
	}

	return std::move(result);
}

static auto CV2TableInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                              GlobalTableFunctionState *global_state) -> unique_ptr<LocalTableFunctionState> {
	const auto &bind_data = input.bind_data->Cast<CV2TableFunctionData>();
	const auto &info = *bind_data.info;

	if (!info.init_local_cb) {
		return nullptr;
	}

	auto scan_columns = CV2TableScanColumns(bind_data, input);

	CV2TableInitLocalInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;
	args.in_scan_columns = &scan_columns;
	if (global_state) {
		args.in_global_state = global_state->Cast<CV2TableGlobalState>().handle.GetData();
	}

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.init_local_cb(Convert(&args), Convert(&context.client), &err_ptr);

	unique_ptr<LocalTableFunctionState> result = nullptr;
	if (args.out_local_state.ptr) {
		auto set_result = make_uniq<CV2TableLocalState>();
		set_result->handle =
		    CV2UserData(args.out_local_state.ptr, args.out_local_state.destroy, args.out_local_state.equals);
		result = std::move(set_result);
	}

	// Throw after taking ownership of the state, so that it is destroyed even if we error.
	if (err.HasError()) {
		err.ThrowAsException();
	}

	return result;
}

static auto CV2TableExec(ClientContext &context, TableFunctionInput &input, DataChunk &output) -> void {
	const auto &bind_data = input.bind_data->Cast<CV2TableFunctionData>();
	const auto &info = *bind_data.info;

	CV2TableExecInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;
	if (input.global_state) {
		auto &global_state = input.global_state->Cast<CV2TableGlobalState>();
		args.in_global_state = global_state.handle.GetData();
		args.in_scan_columns = &global_state.scan_columns;
	}
	if (input.local_state) {
		args.in_local_state = input.local_state->Cast<CV2TableLocalState>().handle.GetData();
	}
	args.output = &output;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.exec_cb(Convert(&args), Convert(&context), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}

	// The vector write API sizes per vector, while the engine reads the chunk's cardinality. Callers typically size
	// only the first vector and write the rest through direct buffer writes, so take the first vector's size as the
	// batch's row count and propagate it to the others. A table function always has at least one result column.
	output.SetChildCardinality(output.data[0].size());
}

//! An exact estimate also pins the maximum; otherwise only the estimate is known.
static auto CV2TableMakeStatistics(idx_t cardinality, bool is_exact) -> unique_ptr<NodeStatistics> {
	if (is_exact) {
		return make_uniq<NodeStatistics>(cardinality, cardinality);
	}
	return make_uniq<NodeStatistics>(cardinality);
}

static auto CV2TableCardinality(ClientContext &context, const FunctionData *bind_data) -> unique_ptr<NodeStatistics> {
	const auto &data = bind_data->Cast<CV2TableFunctionData>();
	if (!data.cardinality_set) {
		// No estimate at all: the optimizer falls back on its own defaults.
		return nullptr;
	}
	return CV2TableMakeStatistics(data.cardinality, data.cardinality_is_exact);
}

static auto CV2TableProgress(ClientContext &context, const FunctionData *bind_data,
                             const GlobalTableFunctionState *global_state) -> double {
	const auto &data = bind_data->Cast<CV2TableFunctionData>();
	const auto &info = *data.info;

	CV2TableProgressInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = data.handle ? data.handle->GetData() : nullptr;
	if (global_state) {
		// The callback runs concurrently with the scan, so the state it reads is the shared one, unsynchronized.
		args.in_global_state = global_state->Cast<CV2TableGlobalState>().handle.GetData();
	}

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.progress_cb(Convert(&args), Convert(&context), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}

	// The API contract is a 0.0-1.0 fraction, the engine reads a 0-100 percentage.
	return MinValue<double>(MaxValue<double>(args.out_progress, 0.0), 1.0) * 100.0;
}

//! The optimizer hands over the predicates it would otherwise evaluate above the scan; whatever the callback accepts
//! is removed from the list, and the engine keeps applying the rest itself.
static auto CV2TableFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                   vector<unique_ptr<Expression>> &filters) -> void {
	if (filters.empty()) {
		return;
	}
	const auto &bind_data = bind_data_p->Cast<CV2TableFunctionData>();
	const auto &info = *bind_data.info;

	CV2TableFilterPushdownInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;
	args.in_filters = &filters;
	args.in_column_ids = &get.GetColumnIds();
	args.out_handled.resize(filters.size(), false);

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.filter_pushdown_cb(Convert(&args), Convert(&context), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}

	vector<unique_ptr<Expression>> remaining;
	for (idx_t i = 0; i < filters.size(); i++) {
		if (!args.out_handled[i]) {
			remaining.push_back(std::move(filters[i]));
		}
	}
	filters = std::move(remaining);
}

class CV2TableFunction {
public:
	void Register() {
		if (name.empty()) {
			throw InvalidInputException("Function name cannot be empty.");
		}
		if (!info.bind_cb) {
			throw InvalidInputException("Bind callback must be set for the function.");
		}
		if (!info.exec_cb) {
			throw InvalidInputException("Exec callback must be set for the function.");
		}
		// A table function declares the columns it returns from its bind callback, not through a return type.
		if (signature.GetReturnType().id() != LogicalTypeId::INVALID) {
			throw InvalidInputException("A table function signature cannot set a return type.");
		}

		signature.Verify();

		// Route the signature onto the two ways SQL passes arguments to a table function: a parameter without a
		// default value is a required positional argument, a parameter with one is an optional named argument.
		vector<LogicalType> positional;
		for (idx_t i = 0; i < signature.GetParameterCount(); i++) {
			const auto &param = signature.GetParameter(i);
			if (!param.HasDefaultValue()) {
				positional.push_back(param.GetType());
			}
		}

		TableFunction function(name, positional, CV2TableExec, CV2TableBind, CV2TableInitGlobal, CV2TableInitLocal);
		if (signature.HasVarArgs()) {
			function.SetVarArgs(signature.GetVarArgs());
		}

		// Always wired: it serves the estimate a bind callback may set, which is not known at registration. It
		// reports no estimate when the bind callback sets none.
		function.cardinality = CV2TableCardinality;
		if (info.progress_cb) {
			function.table_scan_progress = CV2TableProgress;
		}
		if (info.filter_pushdown_cb) {
			function.pushdown_complex_filter = CV2TableFilterPushdown;
		}
		function.projection_pushdown = info.projection_pushdown;

		auto function_info = make_shared_ptr<CV2TableFunctionInfo>(std::move(info));
		for (idx_t i = 0; i < signature.GetParameterCount(); i++) {
			const auto &param = signature.GetParameter(i);
			if (param.HasDefaultValue()) {
				function.named_parameters[param.GetName()] = param.GetType();
				function_info->named_parameter_defaults[param.GetName()] = *param.GetDefaultValue();
			}
			function_info->parameter_names.push_back(param.GetName());
		}
		function_info->positional_count = positional.size();
		function.function_info = std::move(function_info);

		// Call the implementation to register
		RegisterToCatalog(std::move(function));
	}

	virtual ~CV2TableFunction() = default;
	virtual void RegisterToCatalog(TableFunction function) = 0;

public:
	FunctionSignature signature;
	CV2TableFunctionInfo info;
	Identifier name;
};

class CV2ConnectionTableFunction : public CV2TableFunction {
public:
	explicit CV2ConnectionTableFunction(Connection &connection) : connection(connection) {
	}

	void RegisterToCatalog(TableFunction function) override {
		auto &context = *connection.context;

		context.RunFunctionInTransaction([&]() {
			auto &catalog = Catalog::GetSystemCatalog(context);
			CreateTableFunctionInfo tf_info(std::move(function));
			tf_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
			catalog.CreateTableFunction(context, tf_info);
		});
	}

private:
	Connection &connection;
};

class CV2ExtensionTableFunction : public CV2TableFunction {
public:
	explicit CV2ExtensionTableFunction(ExtensionLoader &loader) : loader(loader) {
	}

	void RegisterToCatalog(TableFunction function) override {
		loader.RegisterFunction(std::move(function));
	}

private:
	ExtensionLoader &loader;
};

static auto Convert(duckdb_v2_table_function_handle func) -> CV2TableFunction * {
	return reinterpret_cast<CV2TableFunction *>(func);
}
static auto Convert(CV2TableFunction *func) -> duckdb_v2_table_function_handle {
	return reinterpret_cast<duckdb_v2_table_function_handle>(func);
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_table_function_create_with_connection(duckdb_v2_connection_handle connection,
                                                                duckdb_v2_table_function_handle *function,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(connection);
	DUCKDB_CHECK_ARG(function);
	*function = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &conn = *Convert(connection);
		auto result = duckdb::make_uniq<CV2ConnectionTableFunction>(conn);
		*function = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_create_with_extension(duckdb_v2_extension_handle extension,
                                                               duckdb_v2_table_function_handle *function,
                                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(extension);
	DUCKDB_CHECK_ARG(function);
	*function = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &loader = GetExtensionLoader(extension);
		auto result = duckdb::make_uniq<CV2ExtensionTableFunction>(loader);
		*function = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_set_name(duckdb_v2_table_function_handle function, duckdb_v2_str *name,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(*name);
	return WithErrorHandler(err, [&]() { Convert(function)->name = duckdb::Identifier(Convert(*name)); });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_get_signature(duckdb_v2_table_function_handle function,
                                                       duckdb_v2_function_signature_handle *sig,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(sig);
	return WithErrorHandler(err, [&]() { *sig = Convert(&Convert(function)->signature); });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_set_user_data(duckdb_v2_table_function_handle function, duckdb_v2_opaque *data,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		Convert(function)->info.user_data =
		    duckdb::make_shared_ptr<CV2UserData>(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_set_bind_callback(duckdb_v2_table_function_handle function,
                                                           duckdb_v2_table_function_bind_callback_fn callback,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.bind_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_set_init_global_callback(duckdb_v2_table_function_handle function,
                                                  duckdb_v2_table_function_init_global_callback_fn callback,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.init_global_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_set_init_local_callback(duckdb_v2_table_function_handle function,
                                                 duckdb_v2_table_function_init_local_callback_fn callback,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.init_local_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_set_exec_callback(duckdb_v2_table_function_handle function,
                                                           duckdb_v2_table_function_exec_callback_fn callback,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.exec_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_set_progress_callback(duckdb_v2_table_function_handle function,
                                                               duckdb_v2_table_function_progress_callback_fn callback,
                                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.progress_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_set_projection_pushdown(duckdb_v2_table_function_handle function, bool enable,
                                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.projection_pushdown = enable; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_set_filter_pushdown_callback(duckdb_v2_table_function_handle function,
                                                      duckdb_v2_table_function_filter_pushdown_callback_fn callback,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.filter_pushdown_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_get_user_data(duckdb_v2_table_function_bind_info_handle info, void **data,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_set_bind_data(duckdb_v2_table_function_bind_info_handle info,
                                                            duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { Convert(info)->out_bind_data = *data; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_get_arg_count(duckdb_v2_table_function_bind_info_handle info,
                                                            idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->in_args->size(); });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_get_arg_type(duckdb_v2_table_function_bind_info_handle info, idx_t index,
                                                           duckdb_v2_logical_type_handle *type,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(type);
	*type = nullptr;
	return WithErrorHandler(err, [&]() {
		const auto &arguments = *Convert(info)->in_args;
		if (index >= arguments.size()) {
			throw duckdb::InvalidInputException("Index out of bounds in duckdb_v2_table_function_bind_get_arg_type");
		}
		*type = Convert(new duckdb::LogicalType(arguments[index].type()));
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_get_arg_value(duckdb_v2_table_function_bind_info_handle info, idx_t index,
                                                            duckdb_v2_value_handle *value,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(value);
	*value = nullptr;
	return WithErrorHandler(err, [&]() {
		const auto &arguments = *Convert(info)->in_args;
		if (index >= arguments.size()) {
			throw duckdb::InvalidInputException("Index out of bounds in duckdb_v2_table_function_bind_get_arg_value");
		}
		*value = Convert(new duckdb::Value(arguments[index]));
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_add_result_column(duckdb_v2_table_function_bind_info_handle info,
                                                                duckdb_v2_identifier_t name,
                                                                duckdb_v2_logical_type_handle type,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(type);
	return WithErrorHandler(err, [&]() {
		const auto &column_type = *Convert(type);
		// A result column carries data, so a wildcard or otherwise incomplete type has no meaning here.
		if (!column_type.IsComplete()) {
			throw duckdb::InvalidInputException("Result column type must be a fully defined concrete type");
		}
		auto &bind_info = *Convert(info);
		bind_info.out_column_names.emplace_back(Convert(name));
		bind_info.out_column_types.push_back(column_type);
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_set_cardinality(duckdb_v2_table_function_bind_info_handle info,
                                                              idx_t cardinality, bool is_exact,
                                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() {
		auto &bind_info = *Convert(info);
		bind_info.out_cardinality = cardinality;
		bind_info.out_cardinality_is_exact = is_exact;
		bind_info.out_cardinality_set = true;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_init_global_get_user_data(duckdb_v2_table_function_init_global_info_handle info, void **data,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_init_global_get_bind_data(duckdb_v2_table_function_init_global_info_handle info, void **data,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_init_global_set_global_state(duckdb_v2_table_function_init_global_info_handle info,
                                                      duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { Convert(info)->out_global_state = *data; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_init_global_set_max_threads(duckdb_v2_table_function_init_global_info_handle info,
                                                     idx_t max_threads, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() {
		if (max_threads == 0) {
			throw duckdb::InvalidInputException("The maximum number of threads must be at least 1");
		}
		Convert(info)->out_max_threads = max_threads;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_init_global_get_column_count(duckdb_v2_table_function_init_global_info_handle info,
                                                      idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->in_scan_columns->size(); });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_init_global_get_column_index(duckdb_v2_table_function_init_global_info_handle info,
                                                      idx_t index, idx_t *column_index,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(column_index);
	return WithErrorHandler(err, [&]() {
		const auto &columns = *Convert(info)->in_scan_columns;
		if (index >= columns.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_table_function_init_global_get_column_index");
		}
		*column_index = columns[index];
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_init_local_get_user_data(duckdb_v2_table_function_init_local_info_handle info,
                                                                  void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_init_local_get_bind_data(duckdb_v2_table_function_init_local_info_handle info,
                                                                  void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_init_local_get_global_state(duckdb_v2_table_function_init_local_info_handle info, void **data,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_global_state; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_init_local_set_local_state(duckdb_v2_table_function_init_local_info_handle info,
                                                    duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { Convert(info)->out_local_state = *data; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_init_local_get_column_count(duckdb_v2_table_function_init_local_info_handle info, idx_t *count,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->in_scan_columns->size(); });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_init_local_get_column_index(duckdb_v2_table_function_init_local_info_handle info, idx_t index,
                                                     idx_t *column_index, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(column_index);
	return WithErrorHandler(err, [&]() {
		const auto &columns = *Convert(info)->in_scan_columns;
		if (index >= columns.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_table_function_init_local_get_column_index");
		}
		*column_index = columns[index];
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_exec_get_user_data(duckdb_v2_table_function_exec_info_handle info, void **data,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_exec_get_bind_data(duckdb_v2_table_function_exec_info_handle info, void **data,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_exec_get_global_state(duckdb_v2_table_function_exec_info_handle info,
                                                               void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_global_state; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_exec_get_local_state(duckdb_v2_table_function_exec_info_handle info,
                                                              void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_local_state; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_exec_get_output_chunk(duckdb_v2_table_function_exec_info_handle info,
                                                               duckdb_v2_data_chunk_handle *chunk,
                                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(chunk);
	return WithErrorHandler(err, [&]() { *chunk = Convert(Convert(info)->output); });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_exec_get_column_count(duckdb_v2_table_function_exec_info_handle info,
                                                               idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->in_scan_columns->size(); });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_exec_get_column_index(duckdb_v2_table_function_exec_info_handle info,
                                                               idx_t index, idx_t *column_index,
                                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(column_index);
	return WithErrorHandler(err, [&]() {
		const auto &columns = *Convert(info)->in_scan_columns;
		if (index >= columns.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_table_function_exec_get_column_index");
		}
		*column_index = columns[index];
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_progress_get_user_data(duckdb_v2_table_function_progress_info_handle info,
                                                                void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_progress_get_bind_data(duckdb_v2_table_function_progress_info_handle info,
                                                                void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_progress_get_global_state(duckdb_v2_table_function_progress_info_handle info,
                                                                   void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_global_state; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_progress_set_progress(duckdb_v2_table_function_progress_info_handle info,
                                                               double progress, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() { Convert(info)->out_progress = progress; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_filter_pushdown_get_user_data(duckdb_v2_table_function_filter_pushdown_info_handle info,
                                                       void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_filter_pushdown_get_bind_data(duckdb_v2_table_function_filter_pushdown_info_handle info,
                                                       void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_filter_pushdown_get_filter_count(duckdb_v2_table_function_filter_pushdown_info_handle info,
                                                          idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->in_filters->size(); });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_filter_pushdown_get_filter(duckdb_v2_table_function_filter_pushdown_info_handle info,
                                                    idx_t index, duckdb_v2_expression_handle *filter,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(filter);
	*filter = nullptr;
	return WithErrorHandler(err, [&]() {
		const auto &filters = *Convert(info)->in_filters;
		if (index >= filters.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_table_function_filter_pushdown_get_filter");
		}
		*filter = Convert(filters[index].get());
	});
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_filter_pushdown_accept(duckdb_v2_table_function_filter_pushdown_info_handle info, idx_t index,
                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() {
		auto &handled = Convert(info)->out_handled;
		if (index >= handled.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_table_function_filter_pushdown_accept");
		}
		handled[index] = true;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_filter_pushdown_get_column_count(duckdb_v2_table_function_filter_pushdown_info_handle info,
                                                          idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->in_column_ids->size(); });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_filter_pushdown_get_column_index(duckdb_v2_table_function_filter_pushdown_info_handle info,
                                                          idx_t index, idx_t *column_index,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(column_index);
	return WithErrorHandler(err, [&]() {
		const auto &columns = *Convert(info)->in_column_ids;
		if (index >= columns.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_table_function_filter_pushdown_get_column_index");
		}
		// A function registered here declares no virtual columns, so every entry is a declared column.
		*column_index = columns[index].GetPrimaryIndex();
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_register(duckdb_v2_table_function_handle function,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->Register(); });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_destroy(duckdb_v2_table_function_handle *function) {
	return WithErrorHandler(nullptr, [&]() {
		if (!function) {
			return;
		}
		if (*function) {
			delete Convert(*function);
			*function = nullptr;
		}
	});
}
