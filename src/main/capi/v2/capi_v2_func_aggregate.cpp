#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

namespace duckdb::capiv2 {

class CV2AggregateFunctionData final : public FunctionData {
public:
	// The bind data set by the user
	shared_ptr<CV2UserData> handle;

	auto Copy() const -> unique_ptr<FunctionData> override {
		auto copy = make_uniq<CV2AggregateFunctionData>();
		copy->handle = handle;
		return std::move(copy);
	}

	auto Equals(const FunctionData &other) const -> bool override {
		const auto &other_data = other.Cast<CV2AggregateFunctionData>();
		return handle && other_data.handle && handle->Equals(*other_data.handle);
	}
};

class CV2AggregateBindInfo {
public:
	void *in_user_data = nullptr;
	BindAggregateFunctionInput *in_input = nullptr;

	duckdb_v2_opaque out_bind_data = {};
};

static auto Convert(duckdb_v2_aggregate_function_bind_info_handle info) -> CV2AggregateBindInfo * {
	return reinterpret_cast<CV2AggregateBindInfo *>(info);
}
static auto Convert(CV2AggregateBindInfo *info) -> duckdb_v2_aggregate_function_bind_info_handle {
	return reinterpret_cast<duckdb_v2_aggregate_function_bind_info_handle>(info);
}

class CV2AggregateSizeInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;

	idx_t out_size = 0;
};

static auto Convert(duckdb_v2_aggregate_function_size_info_handle info) -> CV2AggregateSizeInfo * {
	return reinterpret_cast<CV2AggregateSizeInfo *>(info);
}
static auto Convert(CV2AggregateSizeInfo *info) -> duckdb_v2_aggregate_function_size_info_handle {
	return reinterpret_cast<duckdb_v2_aggregate_function_size_info_handle>(info);
}

class CV2AggregateInitInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;

	void **states = nullptr;
	idx_t state_count = 0;
};

static auto Convert(duckdb_v2_aggregate_function_init_info_handle info) -> CV2AggregateInitInfo * {
	return reinterpret_cast<CV2AggregateInitInfo *>(info);
}
static auto Convert(CV2AggregateInitInfo *info) -> duckdb_v2_aggregate_function_init_info_handle {
	return reinterpret_cast<duckdb_v2_aggregate_function_init_info_handle>(info);
}

class CV2AggregateUpdateInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;

	Vector *inputs = nullptr;
	idx_t input_count = 0;
	idx_t row_count = 0;
	void **states = nullptr;
};

static auto Convert(duckdb_v2_aggregate_function_update_info_handle info) -> CV2AggregateUpdateInfo * {
	return reinterpret_cast<CV2AggregateUpdateInfo *>(info);
}
static auto Convert(CV2AggregateUpdateInfo *info) -> duckdb_v2_aggregate_function_update_info_handle {
	return reinterpret_cast<duckdb_v2_aggregate_function_update_info_handle>(info);
}

class CV2AggregateCombineInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;

	void **sources = nullptr;
	void **targets = nullptr;
	idx_t state_count = 0;
};

static auto Convert(duckdb_v2_aggregate_function_combine_info_handle info) -> CV2AggregateCombineInfo * {
	return reinterpret_cast<CV2AggregateCombineInfo *>(info);
}
static auto Convert(CV2AggregateCombineInfo *info) -> duckdb_v2_aggregate_function_combine_info_handle {
	return reinterpret_cast<duckdb_v2_aggregate_function_combine_info_handle>(info);
}

class CV2AggregateFinalizeInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;

	void **states = nullptr;
	idx_t state_count = 0;
	Vector *result = nullptr;
	idx_t result_offset = 0;
};

static auto Convert(duckdb_v2_aggregate_function_finalize_info_handle info) -> CV2AggregateFinalizeInfo * {
	return reinterpret_cast<CV2AggregateFinalizeInfo *>(info);
}
static auto Convert(CV2AggregateFinalizeInfo *info) -> duckdb_v2_aggregate_function_finalize_info_handle {
	return reinterpret_cast<duckdb_v2_aggregate_function_finalize_info_handle>(info);
}

class CV2AggregateDestroyInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;

	void **states = nullptr;
	idx_t state_count = 0;
};

static auto Convert(duckdb_v2_aggregate_function_destroy_info_handle info) -> CV2AggregateDestroyInfo * {
	return reinterpret_cast<CV2AggregateDestroyInfo *>(info);
}
static auto Convert(CV2AggregateDestroyInfo *info) -> duckdb_v2_aggregate_function_destroy_info_handle {
	return reinterpret_cast<duckdb_v2_aggregate_function_destroy_info_handle>(info);
}

class CV2AggregateFunctionInfo : public AggregateFunctionInfo {
public:
	duckdb_v2_aggregate_function_bind_callback_fn bind_cb = nullptr;
	duckdb_v2_aggregate_function_size_callback_fn size_cb = nullptr;
	duckdb_v2_aggregate_function_init_callback_fn init_cb = nullptr;
	duckdb_v2_aggregate_function_update_callback_fn update_cb = nullptr;
	duckdb_v2_aggregate_function_combine_callback_fn combine_cb = nullptr;
	duckdb_v2_aggregate_function_finalize_callback_fn finalize_cb = nullptr;
	duckdb_v2_aggregate_function_destroy_callback_fn destroy_cb = nullptr;
	shared_ptr<CV2UserData> user_data = nullptr;
};

static auto GetUserBindData(const FunctionData *bind_data) -> void * {
	if (!bind_data) {
		return nullptr;
	}
	const auto &handle = bind_data->Cast<CV2AggregateFunctionData>().handle;
	return handle ? handle->GetData() : nullptr;
}

static auto CV2AggregateBind(BindAggregateFunctionInput &input) -> unique_ptr<FunctionData> {
	const auto &info = input.GetBoundFunction().GetExtraFunctionInfo().Cast<CV2AggregateFunctionInfo>();

	CV2AggregateBindInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_input = &input;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.bind_cb(Convert(&args), Convert(&input.GetClientContext()), &err_ptr);

	unique_ptr<FunctionData> result = nullptr;
	if (args.out_bind_data.ptr) {
		auto set_result = make_uniq<CV2AggregateFunctionData>();
		set_result->handle =
		    make_shared_ptr<CV2UserData>(args.out_bind_data.ptr, args.out_bind_data.destroy, args.out_bind_data.equals);
		result = std::move(set_result);
	}

	if (err.HasError()) {
		err.ThrowAsException();
	}

	return result;
}

static auto CV2AggregateSize(AggregateStateInput &input) -> idx_t {
	const auto &info = input.function.GetExtraFunctionInfo().Cast<CV2AggregateFunctionInfo>();

	CV2AggregateSizeInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = GetUserBindData(input.bind_data.get());

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.size_cb(Convert(&args), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}

	return args.out_size;
}

static auto CV2AggregateInit(AggregateStateInput &input, data_ptr_t *states, idx_t count) -> void {
	const auto &info = input.function.GetExtraFunctionInfo().Cast<CV2AggregateFunctionInfo>();

	CV2AggregateInitInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = GetUserBindData(input.bind_data.get());
	args.states = reinterpret_cast<void **>(states);
	args.state_count = count;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.init_cb(Convert(&args), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}
}

static auto CV2AggregateUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &state,
                               idx_t count) -> void {
	const auto &info = aggr_input_data.function.GetExtraFunctionInfo().Cast<CV2AggregateFunctionInfo>();

	CV2AggregateUpdateInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = GetUserBindData(aggr_input_data.bind_data.get());
	args.inputs = inputs;
	args.input_count = input_count;
	args.row_count = count;
	args.states = FlatVector::GetDataMutableUnsafe<void *>(state);

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.update_cb(Convert(&args), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}
}

static auto CV2AggregateCombine(Vector &state, Vector &combined, AggregateInputData &aggr_input_data, idx_t count)
    -> void {
	const auto &info = aggr_input_data.function.GetExtraFunctionInfo().Cast<CV2AggregateFunctionInfo>();

	state.Flatten();

	CV2AggregateCombineInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = GetUserBindData(aggr_input_data.bind_data.get());
	args.sources = FlatVector::GetDataMutableUnsafe<void *>(state);
	args.targets = FlatVector::GetDataMutableUnsafe<void *>(combined);
	args.state_count = count;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.combine_cb(Convert(&args), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}
}

static auto CV2AggregateFinalize(Vector &state, AggregateFinalizeInputData &finalize_input_data, Vector &result,
                                 idx_t count, idx_t offset) -> void {
	const auto &info = finalize_input_data.function.GetExtraFunctionInfo().Cast<CV2AggregateFunctionInfo>();

	state.Flatten();

	CV2AggregateFinalizeInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = GetUserBindData(finalize_input_data.bind_data.get());
	args.states = FlatVector::GetDataMutableUnsafe<void *>(state);
	args.state_count = count;
	args.result = &result;
	args.result_offset = offset;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.finalize_cb(Convert(&args), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}
}

static auto CV2AggregateDestroy(Vector &state, AggregateInputData &aggr_input_data, idx_t count) -> void {
	const auto &info = aggr_input_data.function.GetExtraFunctionInfo().Cast<CV2AggregateFunctionInfo>();

	state.Flatten();

	CV2AggregateDestroyInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = GetUserBindData(aggr_input_data.bind_data.get());
	args.states = FlatVector::GetDataMutableUnsafe<void *>(state);
	args.state_count = count;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.destroy_cb(Convert(&args), &err_ptr);

	// The destructor path may not throw: an error reported through the slot is dropped.
}

class CV2AggregateFunction {
public:
	CV2AggregateFunction() {
		// The callbacks' error slots make failure part of the API contract, so the function defaults to fallible:
		// a non-fallible function turns any execution error reported through the slot into an internal error.
		properties.SetFallible();
	}

	void Register() {
		if (name.empty()) {
			throw InvalidInputException("Function name cannot be empty.");
		}
		if (!info.size_cb) {
			throw InvalidInputException("Size callback must be set for the function.");
		}
		if (!info.init_cb) {
			throw InvalidInputException("Init callback must be set for the function.");
		}
		if (!info.update_cb) {
			throw InvalidInputException("Update callback must be set for the function.");
		}
		if (!info.combine_cb) {
			throw InvalidInputException("Combine callback must be set for the function.");
		}
		if (!info.finalize_cb) {
			throw InvalidInputException("Finalize callback must be set for the function.");
		}

		const auto &return_type = signature.GetReturnType();

		// ANY is allowed as a placeholder return type only when a bind callback is present to resolve the actual type.
		if (return_type.id() == LogicalTypeId::ANY) {
			if (info.bind_cb == nullptr) {
				throw InvalidInputException(
				    "An ANY return type requires a bind callback to set the concrete return type.");
			}
		} else {
			if (return_type.id() == LogicalTypeId::INVALID) {
				throw InvalidInputException("Return type must be set for the function.");
			}
			if (!return_type.IsComplete()) {
				throw InvalidInputException("Return type must be a fully defined concrete type");
			}
		}

		signature.Verify();

		AggregateFunction function(name, {}, return_type, CV2AggregateSize, CV2AggregateInit, CV2AggregateUpdate,
		                           CV2AggregateCombine, CV2AggregateFinalize,
		                           FunctionNullHandling::DEFAULT_NULL_HANDLING);
		function.SetProperties(properties);
		function.GetSignature() = signature;

		if (info.bind_cb) {
			function.SetBindCallback(CV2AggregateBind);
		}
		if (info.destroy_cb) {
			function.SetStateDestructorCallback(CV2AggregateDestroy);
		}
		function.SetExtraFunctionInfo<CV2AggregateFunctionInfo>(std::move(info));

		// Call the implementation to register
		RegisterToCatalog(std::move(function));
	}

	virtual ~CV2AggregateFunction() = default;
	virtual void RegisterToCatalog(AggregateFunction function) = 0;

public:
	FunctionSignature signature;
	CV2AggregateFunctionInfo info;
	Identifier name;
	AggregateFunctionProperties properties;
};

class CV2ConnectionAggregateFunction : public CV2AggregateFunction {
public:
	explicit CV2ConnectionAggregateFunction(Connection &connection) : connection(connection) {
	}

	void RegisterToCatalog(AggregateFunction function) override {
		auto &context = *connection.context;

		context.RunFunctionInTransaction([&]() {
			auto &catalog = Catalog::GetSystemCatalog(context);
			CreateAggregateFunctionInfo af_info(std::move(function));
			af_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
			catalog.CreateFunction(context, af_info);
		});
	}

private:
	Connection &connection;
};

class CV2ExtensionAggregateFunction : public CV2AggregateFunction {
public:
	explicit CV2ExtensionAggregateFunction(ExtensionLoader &loader) : loader(loader) {
	}

	void RegisterToCatalog(AggregateFunction function) override {
		loader.RegisterFunction(std::move(function));
	}

private:
	ExtensionLoader &loader;
};

static auto Convert(duckdb_v2_aggregate_function_handle func) -> CV2AggregateFunction * {
	return reinterpret_cast<CV2AggregateFunction *>(func);
}
static auto Convert(CV2AggregateFunction *func) -> duckdb_v2_aggregate_function_handle {
	return reinterpret_cast<duckdb_v2_aggregate_function_handle>(func);
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_create_with_connection(duckdb_v2_connection_handle connection,
                                                                    duckdb_v2_aggregate_function_handle *out_function,
                                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(connection);
	DUCKDB_CHECK_ARG(out_function);
	*out_function = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &conn = *Convert(connection);
		auto function = duckdb::make_uniq<CV2ConnectionAggregateFunction>(conn);
		*out_function = Convert(function.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_create_with_extension(duckdb_v2_extension_handle extension,
                                                                   duckdb_v2_aggregate_function_handle *out_function,
                                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(extension);
	DUCKDB_CHECK_ARG(out_function);
	*out_function = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &loader = GetExtensionLoader(extension);
		auto function = duckdb::make_uniq<CV2ExtensionAggregateFunction>(loader);
		*out_function = Convert(function.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_set_name(duckdb_v2_aggregate_function_handle function, duckdb_v2_str *name,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(*name);
	return WithErrorHandler(err, [&]() { Convert(function)->name = duckdb::Identifier(Convert(*name)); });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_get_signature(duckdb_v2_aggregate_function_handle function,
                                                           duckdb_v2_function_signature_handle *sig,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(sig);
	return WithErrorHandler(err, [&]() { *sig = Convert(&Convert(function)->signature); });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_set_user_data(duckdb_v2_aggregate_function_handle function,
                                                           duckdb_v2_opaque *user_data,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(user_data);
	return WithErrorHandler(err, [&]() {
		Convert(function)->info.user_data =
		    duckdb::make_shared_ptr<CV2UserData>(user_data->ptr, user_data->destroy, user_data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_set_property(duckdb_v2_aggregate_function_handle function,
                                                          DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
                                                          DUCKDB_V2_FUNCTION_PROPERTY_VALUE value,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { SetAggregateFunctionProperty(Convert(function)->properties, key, value); });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_set_bind_callback(duckdb_v2_aggregate_function_handle function,
                                                               duckdb_v2_aggregate_function_bind_callback_fn callback,
                                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.bind_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_set_size_callback(duckdb_v2_aggregate_function_handle function,
                                                               duckdb_v2_aggregate_function_size_callback_fn callback,
                                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.size_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_set_init_callback(duckdb_v2_aggregate_function_handle function,
                                                               duckdb_v2_aggregate_function_init_callback_fn callback,
                                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.init_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_set_update_callback(duckdb_v2_aggregate_function_handle function,
                                                 duckdb_v2_aggregate_function_update_callback_fn callback,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.update_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_set_combine_callback(duckdb_v2_aggregate_function_handle function,
                                                  duckdb_v2_aggregate_function_combine_callback_fn callback,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.combine_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_set_finalize_callback(duckdb_v2_aggregate_function_handle function,
                                                   duckdb_v2_aggregate_function_finalize_callback_fn callback,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.finalize_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_set_destroy_callback(duckdb_v2_aggregate_function_handle function,
                                                  duckdb_v2_aggregate_function_destroy_callback_fn callback,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.destroy_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_bind_get_user_data(duckdb_v2_aggregate_function_bind_info_handle info,
                                                                void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_bind_set_bind_data(duckdb_v2_aggregate_function_bind_info_handle info,
                                                                duckdb_v2_opaque *data,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() { Convert(info)->out_bind_data = *data; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_bind_get_arg_count(duckdb_v2_aggregate_function_bind_info_handle info,
                                                                idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->in_input->GetArguments().size(); });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_bind_get_arg_type(duckdb_v2_aggregate_function_bind_info_handle info,
                                                               idx_t index, duckdb_v2_logical_type_handle *type,
                                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(type);
	*type = nullptr;
	return WithErrorHandler(err, [&]() {
		const auto &input = *Convert(info)->in_input;
		const auto &arguments = input.GetArguments();
		if (index >= arguments.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_aggregate_function_bind_get_arg_type");
		}
		*type = Convert(new duckdb::LogicalType(arguments[index]->GetReturnType()));
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_bind_get_arg_value(duckdb_v2_aggregate_function_bind_info_handle info,
                                                                idx_t index, duckdb_v2_value_handle *value,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(value);
	*value = nullptr;
	return WithErrorHandler(err, [&]() {
		const auto &input = *Convert(info)->in_input;
		if (index >= input.GetArguments().size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_aggregate_function_bind_get_arg_value");
		}
		*value = Convert(new duckdb::Value(input.GetConstant(index)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_bind_set_return_type(duckdb_v2_aggregate_function_bind_info_handle info,
                                                                  duckdb_v2_logical_type_handle return_type,
                                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(return_type);
	return WithErrorHandler(
	    err, [&]() { Convert(info)->in_input->GetBoundFunction().SetReturnType(*Convert(return_type)); });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_size_get_user_data(duckdb_v2_aggregate_function_size_info_handle info,
                                                                void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_size_get_bind_data(duckdb_v2_aggregate_function_size_info_handle info,
                                                                void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_size_set_state_size(duckdb_v2_aggregate_function_size_info_handle info,
                                                                 idx_t size, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() { Convert(info)->out_size = size; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_init_get_user_data(duckdb_v2_aggregate_function_init_info_handle info,
                                                                void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_init_get_bind_data(duckdb_v2_aggregate_function_init_info_handle info,
                                                                void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_init_get_state_count(duckdb_v2_aggregate_function_init_info_handle info,
                                                                  idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->state_count; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_init_get_states(duckdb_v2_aggregate_function_init_info_handle info,
                                                             void ***states, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(states);
	return WithErrorHandler(err, [&]() { *states = Convert(info)->states; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_update_get_user_data(duckdb_v2_aggregate_function_update_info_handle info,
                                                                  void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_update_get_bind_data(duckdb_v2_aggregate_function_update_info_handle info,
                                                                  void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_update_get_row_count(duckdb_v2_aggregate_function_update_info_handle info,
                                                                  idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->row_count; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_update_get_arg_count(duckdb_v2_aggregate_function_update_info_handle info,
                                                                  uint32_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = duckdb::NumericCast<uint32_t>(Convert(info)->input_count); });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_update_get_arg(duckdb_v2_aggregate_function_update_info_handle info,
                                                            uint32_t index, duckdb_v2_vector_handle *vector,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(vector);
	return WithErrorHandler(err, [&]() {
		auto &update_info = *Convert(info);
		if (index >= update_info.input_count) {
			throw duckdb::InvalidInputException("Index out of bounds in duckdb_v2_aggregate_function_update_get_arg");
		}
		*vector = Convert(&update_info.inputs[index]);
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_update_get_states(duckdb_v2_aggregate_function_update_info_handle info,
                                                               void ***states, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(states);
	return WithErrorHandler(err, [&]() { *states = Convert(info)->states; });
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_combine_get_user_data(duckdb_v2_aggregate_function_combine_info_handle info, void **data,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_combine_get_bind_data(duckdb_v2_aggregate_function_combine_info_handle info, void **data,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_combine_get_state_count(duckdb_v2_aggregate_function_combine_info_handle info,
                                                     idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->state_count; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_combine_get_sources(duckdb_v2_aggregate_function_combine_info_handle info,
                                                                 void ***states, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(states);
	return WithErrorHandler(err, [&]() { *states = Convert(info)->sources; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_combine_get_targets(duckdb_v2_aggregate_function_combine_info_handle info,
                                                                 void ***states, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(states);
	return WithErrorHandler(err, [&]() { *states = Convert(info)->targets; });
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_finalize_get_user_data(duckdb_v2_aggregate_function_finalize_info_handle info, void **data,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_finalize_get_bind_data(duckdb_v2_aggregate_function_finalize_info_handle info, void **data,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_finalize_get_state_count(duckdb_v2_aggregate_function_finalize_info_handle info,
                                                      idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->state_count; });
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_finalize_get_states(duckdb_v2_aggregate_function_finalize_info_handle info, void ***states,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(states);
	return WithErrorHandler(err, [&]() { *states = Convert(info)->states; });
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_finalize_get_result(duckdb_v2_aggregate_function_finalize_info_handle info,
                                                 duckdb_v2_vector_handle *vector, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(vector);
	return WithErrorHandler(err, [&]() { *vector = Convert(Convert(info)->result); });
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_finalize_get_result_offset(duckdb_v2_aggregate_function_finalize_info_handle info,
                                                        idx_t *offset, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(offset);
	return WithErrorHandler(err, [&]() { *offset = Convert(info)->result_offset; });
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_destroy_get_user_data(duckdb_v2_aggregate_function_destroy_info_handle info, void **data,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_destroy_get_bind_data(duckdb_v2_aggregate_function_destroy_info_handle info, void **data,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_destroy_get_state_count(duckdb_v2_aggregate_function_destroy_info_handle info,
                                                     idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->state_count; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_destroy_get_states(duckdb_v2_aggregate_function_destroy_info_handle info,
                                                                void ***states, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(states);
	return WithErrorHandler(err, [&]() { *states = Convert(info)->states; });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_register(duckdb_v2_aggregate_function_handle function,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->Register(); });
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_destroy(duckdb_v2_aggregate_function_handle *function) {
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
