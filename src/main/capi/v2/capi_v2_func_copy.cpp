#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"

namespace duckdb::capiv2 {

class CV2CopyFunctionInfo final : public CopyFunctionInfo {
public:
	duckdb_v2_copy_function_bind_callback_fn bind_cb = nullptr;
	duckdb_v2_copy_function_init_callback_fn init_cb = nullptr;
	duckdb_v2_copy_function_batch_callback_fn batch_cb = nullptr;
	duckdb_v2_copy_function_flush_callback_fn flush_cb = nullptr;
	duckdb_v2_copy_function_finalize_callback_fn finalize_cb = nullptr;
	duckdb_v2_copy_function_batch_size_callback_fn batch_size_cb = nullptr;
	shared_ptr<CV2UserData> user_data = nullptr;
};

//! The bind data of a bound COPY statement. Besides the user's own bind data it shares ownership of the function info:
//! the init, batch, flush and finalize hooks only receive the bind data, so it is their only route to the callbacks.
class CV2CopyFunctionData final : public FunctionData {
public:
	shared_ptr<CV2UserData> handle;
	shared_ptr<CopyFunctionInfo> info;

	auto Copy() const -> unique_ptr<FunctionData> override {
		auto copy = make_uniq<CV2CopyFunctionData>();
		copy->handle = handle;
		copy->info = info;
		return std::move(copy);
	}

	auto Equals(const FunctionData &other) const -> bool override {
		const auto &other_data = other.Cast<CV2CopyFunctionData>();
		return handle && other_data.handle && handle->Equals(*other_data.handle);
	}
};

class CV2CopyGlobalState final : public GlobalFunctionData {
public:
	CV2UserData handle;
};

class CV2CopyBatchData final : public PreparedBatchData {
public:
	CV2UserData handle;
};

class CV2CopyBindInfo {
public:
	void *in_user_data = nullptr;
	const vector<Identifier> *in_names = nullptr;
	const vector<LogicalType> *in_types = nullptr;

	duckdb_v2_opaque out_bind_data = {};
};

static auto Convert(duckdb_v2_copy_function_bind_info_handle info) -> CV2CopyBindInfo * {
	return reinterpret_cast<CV2CopyBindInfo *>(info);
}
static auto Convert(CV2CopyBindInfo *info) -> duckdb_v2_copy_function_bind_info_handle {
	return reinterpret_cast<duckdb_v2_copy_function_bind_info_handle>(info);
}

class CV2CopyInitInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	const string *in_file_path = nullptr;

	duckdb_v2_opaque out_init_data = {};
};

static auto Convert(duckdb_v2_copy_function_init_info_handle info) -> CV2CopyInitInfo * {
	return reinterpret_cast<CV2CopyInitInfo *>(info);
}
static auto Convert(CV2CopyInitInfo *info) -> duckdb_v2_copy_function_init_info_handle {
	return reinterpret_cast<duckdb_v2_copy_function_init_info_handle>(info);
}

class CV2CopyBatchInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	void *in_init_data = nullptr;
	// The batch, until the callback takes it; whatever is left is destroyed with the args.
	unique_ptr<ColumnDataCollection> in_collection;

	duckdb_v2_opaque out_batch_data = {};
};

static auto Convert(duckdb_v2_copy_function_batch_info_handle info) -> CV2CopyBatchInfo * {
	return reinterpret_cast<CV2CopyBatchInfo *>(info);
}
static auto Convert(CV2CopyBatchInfo *info) -> duckdb_v2_copy_function_batch_info_handle {
	return reinterpret_cast<duckdb_v2_copy_function_batch_info_handle>(info);
}

class CV2CopyFlushInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	void *in_init_data = nullptr;
	void *in_batch_data = nullptr;
};

static auto Convert(duckdb_v2_copy_function_flush_info_handle info) -> CV2CopyFlushInfo * {
	return reinterpret_cast<CV2CopyFlushInfo *>(info);
}
static auto Convert(CV2CopyFlushInfo *info) -> duckdb_v2_copy_function_flush_info_handle {
	return reinterpret_cast<duckdb_v2_copy_function_flush_info_handle>(info);
}

class CV2CopyBatchSizeInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;

	idx_t out_target = 0;
};

static auto Convert(duckdb_v2_copy_function_batch_size_info_handle info) -> CV2CopyBatchSizeInfo * {
	return reinterpret_cast<CV2CopyBatchSizeInfo *>(info);
}
static auto Convert(CV2CopyBatchSizeInfo *info) -> duckdb_v2_copy_function_batch_size_info_handle {
	return reinterpret_cast<duckdb_v2_copy_function_batch_size_info_handle>(info);
}

class CV2CopyFinalizeInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	void *in_init_data = nullptr;
};

static auto Convert(duckdb_v2_copy_function_finalize_info_handle info) -> CV2CopyFinalizeInfo * {
	return reinterpret_cast<CV2CopyFinalizeInfo *>(info);
}
static auto Convert(CV2CopyFinalizeInfo *info) -> duckdb_v2_copy_function_finalize_info_handle {
	return reinterpret_cast<duckdb_v2_copy_function_finalize_info_handle>(info);
}

static auto GetFunctionInfo(const FunctionData &bind_data) -> const CV2CopyFunctionInfo & {
	return bind_data.Cast<CV2CopyFunctionData>().info->Cast<CV2CopyFunctionInfo>();
}

static auto GetUserData(const CV2CopyFunctionInfo &info) -> void * {
	return info.user_data ? info.user_data->GetData() : nullptr;
}

static auto GetUserBindData(const FunctionData &bind_data) -> void * {
	const auto &handle = bind_data.Cast<CV2CopyFunctionData>().handle;
	return handle ? handle->GetData() : nullptr;
}

static auto GetUserInitData(const GlobalFunctionData &gstate) -> void * {
	return gstate.Cast<CV2CopyGlobalState>().handle.GetData();
}

static auto CV2CopyBind(ClientContext &context, CopyFunctionBindInput &input, const vector<Identifier> &names,
                        const vector<LogicalType> &sql_types) -> unique_ptr<FunctionData> {
	const auto &info = input.function_info->Cast<CV2CopyFunctionInfo>();

	CV2CopyBindInfo args = {};
	args.in_user_data = GetUserData(info);
	args.in_names = &names;
	args.in_types = &sql_types;

	// The bind callback is optional: without one, the statement carries no bind data.
	CV2ErrorInfo err = {};
	if (info.bind_cb) {
		auto err_ptr = Convert(&err);
		info.bind_cb(Convert(&args), Convert(&context), &err_ptr);
	}

	// Take ownership of whatever the callback set before reporting an error, so it is destroyed either way.
	auto result = make_uniq<CV2CopyFunctionData>();
	result->info = input.function_info;
	if (args.out_bind_data.ptr) {
		result->handle =
		    make_shared_ptr<CV2UserData>(args.out_bind_data.ptr, args.out_bind_data.destroy, args.out_bind_data.equals);
	}

	if (err.HasError()) {
		err.ThrowAsException();
	}

	return std::move(result);
}

static auto CV2CopyInitGlobal(ClientContext &context, FunctionData &bind_data, const string &file_path)
    -> unique_ptr<GlobalFunctionData> {
	const auto &info = GetFunctionInfo(bind_data);

	CV2CopyInitInfo args = {};
	args.in_user_data = GetUserData(info);
	args.in_bind_data = GetUserBindData(bind_data);
	args.in_file_path = &file_path;

	// The init callback is optional: without one, the file carries no init data.
	CV2ErrorInfo err = {};
	if (info.init_cb) {
		auto err_ptr = Convert(&err);
		info.init_cb(Convert(&args), Convert(&context), &err_ptr);
	}

	auto result = make_uniq<CV2CopyGlobalState>();
	if (args.out_init_data.ptr) {
		result->handle = CV2UserData(args.out_init_data.ptr, args.out_init_data.destroy, args.out_init_data.equals);
	}

	if (err.HasError()) {
		err.ThrowAsException();
	}

	return std::move(result);
}

static auto CV2CopyPrepareBatch(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                                unique_ptr<ColumnDataCollection> collection) -> unique_ptr<PreparedBatchData> {
	const auto &info = GetFunctionInfo(bind_data);

	CV2CopyBatchInfo args = {};
	args.in_user_data = GetUserData(info);
	args.in_bind_data = GetUserBindData(bind_data);
	args.in_init_data = GetUserInitData(gstate);
	args.in_collection = std::move(collection);

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.batch_cb(Convert(&args), Convert(&context), &err_ptr);

	auto result = make_uniq<CV2CopyBatchData>();
	if (args.out_batch_data.ptr) {
		result->handle = CV2UserData(args.out_batch_data.ptr, args.out_batch_data.destroy, args.out_batch_data.equals);
	}

	if (err.HasError()) {
		err.ThrowAsException();
	}

	return std::move(result);
}

static auto CV2CopyFlushBatch(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                              PreparedBatchData &batch) -> void {
	const auto &info = GetFunctionInfo(bind_data);

	CV2CopyFlushInfo args = {};
	args.in_user_data = GetUserData(info);
	args.in_bind_data = GetUserBindData(bind_data);
	args.in_init_data = GetUserInitData(gstate);
	args.in_batch_data = batch.Cast<CV2CopyBatchData>().handle.GetData();

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.flush_cb(Convert(&args), Convert(&context), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}
}

static auto CV2CopyFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) -> void {
	const auto &info = GetFunctionInfo(bind_data);

	// Always wired, as the engine requires a finalize hook; the user's callback is optional.
	if (!info.finalize_cb) {
		return;
	}

	CV2CopyFinalizeInfo args = {};
	args.in_user_data = GetUserData(info);
	args.in_bind_data = GetUserBindData(bind_data);
	args.in_init_data = GetUserInitData(gstate);

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.finalize_cb(Convert(&args), Convert(&context), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}
}

static auto CV2CopyBatchSize(ClientContext &context, FunctionData &bind_data) -> idx_t {
	const auto &info = GetFunctionInfo(bind_data);

	CV2CopyBatchSizeInfo args = {};
	args.in_user_data = GetUserData(info);
	args.in_bind_data = GetUserBindData(bind_data);

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.batch_size_cb(Convert(&args), Convert(&context), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}
	if (args.out_target == 0) {
		throw InvalidInputException("The batch size callback must set a target greater than 0.");
	}
	return args.out_target;
}

class CV2CopyFunction {
public:
	void Register() {
		if (name.empty()) {
			throw InvalidInputException("Function name cannot be empty.");
		}
		if (!info.batch_cb) {
			throw InvalidInputException("Batch callback must be set for the function.");
		}
		if (!info.flush_cb) {
			throw InvalidInputException("Flush callback must be set for the function.");
		}

		CopyFunction function(name);
		function.copy_to_bind = CV2CopyBind;
		function.copy_to_initialize_global = CV2CopyInitGlobal;
		function.prepare_batch = CV2CopyPrepareBatch;
		function.flush_batch = CV2CopyFlushBatch;
		function.copy_to_finalize = CV2CopyFinalize;
		if (info.batch_size_cb) {
			function.desired_batch_size = CV2CopyBatchSize;
		}
		function.function_info = make_shared_ptr<CV2CopyFunctionInfo>(std::move(info));

		// Call the implementation to register
		RegisterToCatalog(std::move(function));
	}

	virtual ~CV2CopyFunction() = default;
	virtual void RegisterToCatalog(CopyFunction function) = 0;

public:
	CV2CopyFunctionInfo info;
	Identifier name;
};

class CV2ConnectionCopyFunction : public CV2CopyFunction {
public:
	explicit CV2ConnectionCopyFunction(Connection &connection) : connection(connection) {
	}

	void RegisterToCatalog(CopyFunction function) override {
		auto &context = *connection.context;

		context.RunFunctionInTransaction([&]() {
			auto &catalog = Catalog::GetSystemCatalog(context);
			CreateCopyFunctionInfo cf_info(std::move(function));
			cf_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
			catalog.CreateCopyFunction(context, cf_info);
		});
	}

private:
	Connection &connection;
};

class CV2ExtensionCopyFunction : public CV2CopyFunction {
public:
	explicit CV2ExtensionCopyFunction(ExtensionLoader &loader) : loader(loader) {
	}

	void RegisterToCatalog(CopyFunction function) override {
		loader.RegisterFunction(std::move(function));
	}

private:
	ExtensionLoader &loader;
};

static auto Convert(duckdb_v2_copy_function_handle func) -> CV2CopyFunction * {
	return reinterpret_cast<CV2CopyFunction *>(func);
}
static auto Convert(CV2CopyFunction *func) -> duckdb_v2_copy_function_handle {
	return reinterpret_cast<duckdb_v2_copy_function_handle>(func);
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_copy_function_create_with_connection(duckdb_v2_connection_handle connection,
                                                               duckdb_v2_copy_function_handle *function,
                                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(connection);
	DUCKDB_CHECK_ARG(function);
	*function = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &conn = *Convert(connection);
		auto result = duckdb::make_uniq<CV2ConnectionCopyFunction>(conn);
		*function = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_create_with_extension(duckdb_v2_extension_handle extension,
                                                              duckdb_v2_copy_function_handle *function,
                                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(extension);
	DUCKDB_CHECK_ARG(function);
	*function = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &loader = GetExtensionLoader(extension);
		auto result = duckdb::make_uniq<CV2ExtensionCopyFunction>(loader);
		*function = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_set_name(duckdb_v2_copy_function_handle function, duckdb_v2_str *name,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(*name);
	return WithErrorHandler(err, [&]() { Convert(function)->name = duckdb::Identifier(Convert(*name)); });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_set_user_data(duckdb_v2_copy_function_handle function, duckdb_v2_opaque *data,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		Convert(function)->info.user_data =
		    duckdb::make_shared_ptr<CV2UserData>(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_set_bind_callback(duckdb_v2_copy_function_handle function,
                                                          duckdb_v2_copy_function_bind_callback_fn callback,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.bind_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_set_init_callback(duckdb_v2_copy_function_handle function,
                                                          duckdb_v2_copy_function_init_callback_fn callback,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.init_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_set_batch_callback(duckdb_v2_copy_function_handle function,
                                                           duckdb_v2_copy_function_batch_callback_fn callback,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.batch_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_set_flush_callback(duckdb_v2_copy_function_handle function,
                                                           duckdb_v2_copy_function_flush_callback_fn callback,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.flush_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_set_batch_size_callback(duckdb_v2_copy_function_handle function,
                                                                duckdb_v2_copy_function_batch_size_callback_fn callback,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.batch_size_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_set_finalize_callback(duckdb_v2_copy_function_handle function,
                                                              duckdb_v2_copy_function_finalize_callback_fn callback,
                                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.finalize_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_bind_get_user_data(duckdb_v2_copy_function_bind_info_handle info, void **data,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_bind_set_bind_data(duckdb_v2_copy_function_bind_info_handle info,
                                                           duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { Convert(info)->out_bind_data = *data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_bind_get_column_count(duckdb_v2_copy_function_bind_info_handle info,
                                                              idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->in_types->size(); });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_bind_get_column_type(duckdb_v2_copy_function_bind_info_handle info, idx_t index,
                                                             duckdb_v2_logical_type_handle *type,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(type);
	*type = nullptr;
	return WithErrorHandler(err, [&]() {
		const auto &types = *Convert(info)->in_types;
		if (index >= types.size()) {
			throw duckdb::InvalidInputException("Index out of bounds in duckdb_v2_copy_function_bind_get_column_type");
		}
		*type = Convert(new duckdb::LogicalType(types[index]));
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_bind_get_column_name(duckdb_v2_copy_function_bind_info_handle info, idx_t index,
                                                             duckdb_v2_identifier_t *name,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(name);
	return WithErrorHandler(err, [&]() {
		const auto &names = *Convert(info)->in_names;
		if (index >= names.size()) {
			throw duckdb::InvalidInputException("Index out of bounds in duckdb_v2_copy_function_bind_get_column_name");
		}
		*name = Convert(names[index]);
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_batch_size_get_user_data(duckdb_v2_copy_function_batch_size_info_handle info,
                                                                 void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_batch_size_get_bind_data(duckdb_v2_copy_function_batch_size_info_handle info,
                                                                 void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_batch_size_set_target(duckdb_v2_copy_function_batch_size_info_handle info,
                                                              idx_t rows, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() { Convert(info)->out_target = rows; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_init_get_user_data(duckdb_v2_copy_function_init_info_handle info, void **data,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_init_get_bind_data(duckdb_v2_copy_function_init_info_handle info, void **data,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_init_get_file_path(duckdb_v2_copy_function_init_info_handle info,
                                                           duckdb_v2_str *path, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(path);
	return WithErrorHandler(err, [&]() { *path = Convert(*Convert(info)->in_file_path); });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_init_set_init_data(duckdb_v2_copy_function_init_info_handle info,
                                                           duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { Convert(info)->out_init_data = *data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_batch_get_user_data(duckdb_v2_copy_function_batch_info_handle info, void **data,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_batch_get_bind_data(duckdb_v2_copy_function_batch_info_handle info, void **data,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_batch_get_init_data(duckdb_v2_copy_function_batch_info_handle info, void **data,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_init_data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_batch_take_input(duckdb_v2_copy_function_batch_info_handle info,
                                                         duckdb_v2_column_data_collection_handle *collection,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(collection);
	*collection = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &batch_info = *Convert(info);
		if (!batch_info.in_collection) {
			throw duckdb::InvalidInputException("The batch input was already taken.");
		}
		*collection = Convert(batch_info.in_collection.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_batch_set_batch_data(duckdb_v2_copy_function_batch_info_handle info,
                                                             duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { Convert(info)->out_batch_data = *data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_flush_get_user_data(duckdb_v2_copy_function_flush_info_handle info, void **data,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_flush_get_bind_data(duckdb_v2_copy_function_flush_info_handle info, void **data,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_flush_get_init_data(duckdb_v2_copy_function_flush_info_handle info, void **data,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_init_data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_flush_get_batch_data(duckdb_v2_copy_function_flush_info_handle info,
                                                             void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_batch_data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_finalize_get_user_data(duckdb_v2_copy_function_finalize_info_handle info,
                                                               void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_finalize_get_bind_data(duckdb_v2_copy_function_finalize_info_handle info,
                                                               void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_finalize_get_init_data(duckdb_v2_copy_function_finalize_info_handle info,
                                                               void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_init_data; });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_register(duckdb_v2_copy_function_handle function,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->Register(); });
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_destroy(duckdb_v2_copy_function_handle *function) {
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
