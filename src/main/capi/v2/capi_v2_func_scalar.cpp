#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb::capiv2 {

class CV2FunctionData final : public FunctionData {
public:
	// The bind data set by the user
	shared_ptr<CV2UserData> handle;

	auto Copy() const -> unique_ptr<FunctionData> override {
		auto copy = make_uniq<CV2FunctionData>();
		copy->handle = handle;
		return std::move(copy);
	}

	auto Equals(const FunctionData &other) const -> bool override {
		const auto &other_data = other.Cast<CV2FunctionData>();
		return handle && other_data.handle && handle->Equals(*other_data.handle);
	}
};

class CV2FunctionLocalState final : public FunctionLocalState {
public:
	CV2UserData handle;
};

class CV2BindInfo {
public:
	void *in_user_data = nullptr;
	BindScalarFunctionInput *in_input = nullptr;

	duckdb_v2_opaque out_bind_data = {};
};

static auto Convert(duckdb_v2_scalar_function_bind_info_handle info) -> CV2BindInfo * {
	return reinterpret_cast<CV2BindInfo *>(info);
}
static auto Convert(CV2BindInfo *info) -> duckdb_v2_scalar_function_bind_info_handle {
	return reinterpret_cast<duckdb_v2_scalar_function_bind_info_handle>(info);
}

class CV2InitInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;

	duckdb_v2_opaque out_init_data = {};
};

static auto Convert(duckdb_v2_scalar_function_init_info_handle info) -> CV2InitInfo * {
	return reinterpret_cast<CV2InitInfo *>(info);
}
static auto Convert(CV2InitInfo *info) -> duckdb_v2_scalar_function_init_info_handle {
	return reinterpret_cast<duckdb_v2_scalar_function_init_info_handle>(info);
}

class CV2ExecInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	void *in_init_data = nullptr;

	DataChunk *input;
	Vector *result;
};

static auto Convert(duckdb_v2_scalar_function_exec_info_handle info) -> CV2ExecInfo * {
	return reinterpret_cast<CV2ExecInfo *>(info);
}
static auto Convert(CV2ExecInfo *info) -> duckdb_v2_scalar_function_exec_info_handle {
	return reinterpret_cast<duckdb_v2_scalar_function_exec_info_handle>(info);
}

class CV2ScalarFunctionInfo : public ScalarFunctionInfo {
public:
	duckdb_v2_scalar_function_bind_callback_fn bind_cb = nullptr;
	duckdb_v2_scalar_function_init_callback_fn init_cb = nullptr;
	duckdb_v2_scalar_function_exec_callback_fn exec_cb = nullptr;
	shared_ptr<CV2UserData> user_data = nullptr;
};

static auto CV2ScalarBind(BindScalarFunctionInput &input) -> unique_ptr<FunctionData> {
	const auto &info = input.GetBoundFunction().GetExtraFunctionInfo().Cast<CV2ScalarFunctionInfo>();

	CV2BindInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_input = &input;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.bind_cb(Convert(&args), Convert(&input.GetClientContext()), &err_ptr);

	unique_ptr<FunctionData> result = nullptr;
	if (args.out_bind_data.ptr) {
		auto set_result = make_uniq<CV2FunctionData>();
		set_result->handle =
		    make_shared_ptr<CV2UserData>(args.out_bind_data.ptr, args.out_bind_data.destroy, args.out_bind_data.equals);
		result = std::move(set_result);
	}

	if (err.HasError()) {
		err.ThrowAsException();
	}

	return result;
}

static auto CV2ScalarInit(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data)
    -> unique_ptr<FunctionLocalState> {
	const auto &info = expr.Function().GetExtraFunctionInfo().Cast<CV2ScalarFunctionInfo>();

	auto user_bind_data = bind_data ? bind_data->Cast<CV2FunctionData>().handle : nullptr;

	CV2InitInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = user_bind_data ? user_bind_data->GetData() : nullptr;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.init_cb(Convert(&args), Convert(&state.GetContext()), &err_ptr);

	unique_ptr<FunctionLocalState> result = nullptr;
	if (args.out_init_data.ptr) {
		auto set_result = make_uniq<CV2FunctionLocalState>();
		set_result->handle = CV2UserData(args.out_init_data.ptr, args.out_init_data.destroy, args.out_init_data.equals);
		result = std::move(set_result);
	}

	// Throw after setting the state, so that it is destroyed even if we error.

	if (err.HasError()) {
		err.ThrowAsException();
	}

	return result;
}

static auto CV2ScalarExec(DataChunk &input, ExpressionState &state, Vector &result) -> void {
	auto &expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = expr.Function().GetExtraFunctionInfo().Cast<CV2ScalarFunctionInfo>();

	CV2ExecInfo args;
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.input = &input;
	args.result = &result;

	// Setup bind data (if provided)
	if (auto bind_ptr = expr.BindInfo().get()) {
		const auto &bind_data = bind_ptr->Cast<CV2FunctionData>();
		args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;
	}

	// Setup local state (if provided)
	if (auto state_ptr = ExecuteFunctionState::GetFunctionState(state)) {
		const auto &state_data = state_ptr->Cast<CV2FunctionLocalState>();
		args.in_init_data = state_data.handle.GetData();
	}

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);

	info.exec_cb(Convert(&args), Convert(&state.GetContext()), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}
}

class CV2ScalarFunction {
public:
	CV2ScalarFunction() {
		// The exec callback's error slot makes failure part of the API contract, so the function defaults to fallible:
		// a non-fallible function turns any execution error reported through the slot into an internal error.
		properties.SetFallible();
	}

	void Register() {
		if (name.empty()) {
			throw InvalidInputException("Function name cannot be empty.");
		}
		if (!info.exec_cb) {
			throw InvalidInputException("Exec callback must be set for the function.");
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

		ScalarFunction function(name, {}, return_type, CV2ScalarExec);
		function.SetProperties(properties);
		function.GetSignature() = signature;

		if (info.bind_cb) {
			function.SetBindCallback(CV2ScalarBind);
		}
		if (info.init_cb) {
			function.SetInitStateCallback(CV2ScalarInit);
		}
		function.SetExtraFunctionInfo<CV2ScalarFunctionInfo>(std::move(info));

		// Call the implementation to register
		RegisterToCatalog(std::move(function));
	}

	virtual ~CV2ScalarFunction() = default;
	virtual void RegisterToCatalog(ScalarFunction function) = 0;

public:
	FunctionSignature signature;
	CV2ScalarFunctionInfo info;
	Identifier name;
	FunctionProperties properties;
};

class CV2ConnectionScalarFunction : public CV2ScalarFunction {
public:
	explicit CV2ConnectionScalarFunction(Connection &connection) : connection(connection) {
	}

	void RegisterToCatalog(ScalarFunction function) override {
		auto &context = *connection.context;

		context.RunFunctionInTransaction([&]() {
			auto &catalog = Catalog::GetSystemCatalog(context);
			CreateScalarFunctionInfo sf_info(std::move(function));
			sf_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
			catalog.CreateFunction(context, sf_info);
		});
	}

private:
	Connection &connection;
};

class CV2ExtensionScalarFunction : public CV2ScalarFunction {
public:
	explicit CV2ExtensionScalarFunction(ExtensionLoader &loader) : loader(loader) {
	}

	void RegisterToCatalog(ScalarFunction function) override {
		loader.RegisterFunction(std::move(function));
	}

private:
	ExtensionLoader &loader;
};

static auto Convert(duckdb_v2_scalar_function_handle func) -> CV2ScalarFunction * {
	return reinterpret_cast<CV2ScalarFunction *>(func);
}
static auto Convert(CV2ScalarFunction *func) -> duckdb_v2_scalar_function_handle {
	return reinterpret_cast<duckdb_v2_scalar_function_handle>(func);
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_scalar_function_create_with_connection(duckdb_v2_connection_handle connection,
                                                                 duckdb_v2_scalar_function_handle *out_function,
                                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(connection);
	DUCKDB_CHECK_ARG(out_function);
	*out_function = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &conn = *Convert(connection);
		auto function = duckdb::make_uniq<CV2ConnectionScalarFunction>(conn);
		*out_function = Convert(function.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_create_with_extension(duckdb_v2_extension_handle extension,
                                                                duckdb_v2_scalar_function_handle *out_function,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(extension);
	DUCKDB_CHECK_ARG(out_function);
	*out_function = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &loader = GetExtensionLoader(extension);
		auto function = duckdb::make_uniq<CV2ExtensionScalarFunction>(loader);
		*out_function = Convert(function.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_set_name(duckdb_v2_scalar_function_handle function, duckdb_v2_str *name,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(*name);
	return WithErrorHandler(err, [&]() { Convert(function)->name = duckdb::Identifier(Convert(*name)); });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_get_signature(duckdb_v2_scalar_function_handle function,
                                                        duckdb_v2_function_signature_handle *sig,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(sig);
	return WithErrorHandler(err, [&]() { *sig = Convert(&Convert(function)->signature); });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_set_user_data(duckdb_v2_scalar_function_handle function,
                                                        duckdb_v2_opaque *user_data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(user_data);
	return WithErrorHandler(err, [&]() {
		Convert(function)->info.user_data =
		    duckdb::make_shared_ptr<CV2UserData>(user_data->ptr, user_data->destroy, user_data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_set_property(duckdb_v2_scalar_function_handle function,
                                                       DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
                                                       DUCKDB_V2_FUNCTION_PROPERTY_VALUE value,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { SetScalarFunctionProperty(Convert(function)->properties, key, value); });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_set_bind_callback(duckdb_v2_scalar_function_handle function,
                                                            duckdb_v2_scalar_function_bind_callback_fn callback,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.bind_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_set_init_callback(duckdb_v2_scalar_function_handle function,
                                                            duckdb_v2_scalar_function_init_callback_fn callback,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.init_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_set_exec_callback(duckdb_v2_scalar_function_handle function,
                                                            duckdb_v2_scalar_function_exec_callback_fn callback,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.exec_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_bind_get_user_data(duckdb_v2_scalar_function_bind_info_handle info,
                                                             void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_bind_set_bind_data(duckdb_v2_scalar_function_bind_info_handle info,
                                                             duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() { Convert(info)->out_bind_data = *data; });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_bind_get_arg_count(duckdb_v2_scalar_function_bind_info_handle info,
                                                             idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->in_input->GetArguments().size(); });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_bind_get_arg_type(duckdb_v2_scalar_function_bind_info_handle info,
                                                            idx_t index, duckdb_v2_logical_type_handle *type,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(type);
	*type = nullptr;
	return WithErrorHandler(err, [&]() {
		const auto &input = *Convert(info)->in_input;
		const auto &arguments = input.GetArguments();
		if (index >= arguments.size()) {
			throw duckdb::InvalidInputException("Index out of bounds in duckdb_v2_scalar_function_bind_get_arg_type");
		}
		*type = Convert(new duckdb::LogicalType(arguments[index]->GetReturnType()));
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_bind_get_arg_value(duckdb_v2_scalar_function_bind_info_handle info,
                                                             idx_t index, duckdb_v2_value_handle *value,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(value);
	*value = nullptr;
	return WithErrorHandler(err, [&]() {
		const auto &input = *Convert(info)->in_input;
		if (index >= input.GetArguments().size()) {
			throw duckdb::InvalidInputException("Index out of bounds in duckdb_v2_scalar_function_bind_get_arg_value");
		}
		*value = Convert(new duckdb::Value(input.GetConstant(index)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_bind_set_return_type(duckdb_v2_scalar_function_bind_info_handle info,
                                                               duckdb_v2_logical_type_handle return_type,
                                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(return_type);
	return WithErrorHandler(
	    err, [&]() { Convert(info)->in_input->GetBoundFunction().SetReturnType(*Convert(return_type)); });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_init_get_user_data(duckdb_v2_scalar_function_init_info_handle info,
                                                             void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_init_get_bind_data(duckdb_v2_scalar_function_init_info_handle info,
                                                             void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_init_set_init_data(duckdb_v2_scalar_function_init_info_handle info,
                                                             duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);

	return WithErrorHandler(err, [&]() { Convert(info)->out_init_data = *data; });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_exec_get_user_data(duckdb_v2_scalar_function_exec_info_handle info,
                                                             void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);

	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_exec_get_bind_data(duckdb_v2_scalar_function_exec_info_handle info,
                                                             void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);

	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_exec_get_init_data(duckdb_v2_scalar_function_exec_info_handle info,
                                                             void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);

	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_init_data; });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_exec_get_row_count(duckdb_v2_scalar_function_exec_info_handle info,
                                                             idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);

	return WithErrorHandler(err, [&]() { *count = Convert(info)->input->size(); });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_exec_get_arg_count(duckdb_v2_scalar_function_exec_info_handle info,
                                                             uint32_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);

	return WithErrorHandler(err,
	                        [&]() { *count = duckdb::NumericCast<uint32_t>(Convert(info)->input->ColumnCount()); });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_exec_get_arg(duckdb_v2_scalar_function_exec_info_handle info, uint32_t index,
                                                       duckdb_v2_vector_handle *vector,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(vector);

	return WithErrorHandler(err, [&]() {
		auto &exec_info = *Convert(info);
		if (index >= exec_info.input->ColumnCount()) {
			throw duckdb::InvalidInputException("Index out of bounds in duckdb_v2_scalar_function_exec_get_arg");
		}
		*vector = Convert(&exec_info.input->data[index]);
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_exec_get_result(duckdb_v2_scalar_function_exec_info_handle info,
                                                          duckdb_v2_vector_handle *vector,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(vector);

	return WithErrorHandler(err, [&]() {
		auto &exec_info = *Convert(info);
		*vector = Convert(exec_info.result);
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_register(duckdb_v2_scalar_function_handle function,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->Register(); });
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_destroy(duckdb_v2_scalar_function_handle *function) {
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
