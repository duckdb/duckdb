#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

namespace duckdb::capiv2 {

class CV2CastBoundData final : public BoundCastData {
public:
	CV2CastBoundData(duckdb_v2_cast_function_exec_callback_fn exec_cb, shared_ptr<CV2UserData> user_data)
	    : exec_cb(exec_cb), user_data(std::move(user_data)) {
	}

	auto Copy() const -> unique_ptr<BoundCastData> override {
		return make_uniq<CV2CastBoundData>(exec_cb, user_data);
	}

public:
	duckdb_v2_cast_function_exec_callback_fn exec_cb;
	shared_ptr<CV2UserData> user_data;
};

// CastParameters carries no context, so the only place to pick one up is the local state init, which does receive
// one. The state exists purely to forward it to the exec callback.
class CV2CastLocalState final : public FunctionLocalState {
public:
	optional_ptr<ClientContext> context;
};

class CV2CastExecInfo {
public:
	void *in_user_data = nullptr;

	Vector *input = nullptr;
	Vector *output = nullptr;
	idx_t count = 0;
	DUCKDB_V2_CAST_MODE mode = DUCKDB_V2_CAST_MODE_NORMAL;
};

static auto Convert(duckdb_v2_cast_function_exec_info_handle info) -> CV2CastExecInfo * {
	return reinterpret_cast<CV2CastExecInfo *>(info);
}
static auto Convert(CV2CastExecInfo *info) -> duckdb_v2_cast_function_exec_info_handle {
	return reinterpret_cast<duckdb_v2_cast_function_exec_info_handle>(info);
}

static auto CV2CastInitLocalState(CastLocalStateParameters &parameters) -> unique_ptr<FunctionLocalState> {
	auto state = make_uniq<CV2CastLocalState>();

	if (!parameters.context) {
		throw InvalidInputException("Cannot initialize local state for extension cast function without a context");
	}

	state->context = parameters.context;
	return std::move(state);
}

static auto CV2CastExec(Vector &input, Vector &output, idx_t count, CastParameters &parameters) -> bool {
	const auto &bound_data = parameters.cast_data->Cast<CV2CastBoundData>();

	// A try cast is exactly the case where the engine handed us a slot to write a message into instead of aborting.
	const auto is_try_cast = parameters.error_message != nullptr;

	CV2CastExecInfo args = {};
	args.in_user_data = bound_data.user_data ? bound_data.user_data->GetData() : nullptr;
	args.input = &input;
	args.output = &output;
	args.count = count;
	args.mode = is_try_cast ? DUCKDB_V2_CAST_MODE_TRY : DUCKDB_V2_CAST_MODE_NORMAL;

	optional_ptr<ClientContext> context = nullptr;
	if (parameters.local_state) {
		context = parameters.local_state->Cast<CV2CastLocalState>().context;
	}

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	bound_data.exec_cb(Convert(&args), Convert(context.get()), &err_ptr);

	if (!err.HasError()) {
		return true;
	}
	if (is_try_cast) {
		// The engine discards the message and keeps whatever the callback left in the output vector.
		HandleCastError::AssignError(err.message, parameters);
		return false;
	}
	err.ThrowAsException();
}

class CV2CastFunction {
public:
	void Register() {
		if (!source_type.IsComplete()) {
			throw InvalidInputException("Source type must be set to a fully defined concrete type");
		}
		if (!target_type.IsComplete()) {
			throw InvalidInputException("Target type must be set to a fully defined concrete type");
		}
		if (!exec_cb) {
			throw InvalidInputException("Exec callback must be set for the function.");
		}

		BoundCastInfo cast_info(CV2CastExec, make_uniq<CV2CastBoundData>(exec_cb, user_data), CV2CastInitLocalState);
		RegisterToCatalog(std::move(cast_info));
	}

	virtual ~CV2CastFunction() = default;
	virtual void RegisterToCatalog(BoundCastInfo cast_info) = 0;

public:
	LogicalType source_type;
	LogicalType target_type;
	int64_t implicit_cast_cost = -1;
	duckdb_v2_cast_function_exec_callback_fn exec_cb = nullptr;
	shared_ptr<CV2UserData> user_data = nullptr;
};

class CV2ConnectionCastFunction : public CV2CastFunction {
public:
	explicit CV2ConnectionCastFunction(Connection &connection) : connection(connection) {
	}

	void RegisterToCatalog(BoundCastInfo cast_info) override {
		auto &context = *connection.context;

		context.RunFunctionInTransaction([&]() {
			auto &casts = CastFunctionSet::Get(context);
			casts.RegisterCastFunction(source_type, target_type, std::move(cast_info), implicit_cast_cost);
		});
	}

private:
	Connection &connection;
};

class CV2ExtensionCastFunction : public CV2CastFunction {
public:
	explicit CV2ExtensionCastFunction(ExtensionLoader &loader) : loader(loader) {
	}

	void RegisterToCatalog(BoundCastInfo cast_info) override {
		loader.RegisterCastFunction(source_type, target_type, std::move(cast_info), implicit_cast_cost);
	}

private:
	ExtensionLoader &loader;
};

static auto Convert(duckdb_v2_cast_function_handle func) -> CV2CastFunction * {
	return reinterpret_cast<CV2CastFunction *>(func);
}
static auto Convert(CV2CastFunction *func) -> duckdb_v2_cast_function_handle {
	return reinterpret_cast<duckdb_v2_cast_function_handle>(func);
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_cast_function_create_with_connection(duckdb_v2_connection_handle connection,
                                                               duckdb_v2_cast_function_handle *out_function,
                                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(connection);
	DUCKDB_CHECK_ARG(out_function);
	*out_function = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &conn = *Convert(connection);
		auto function = duckdb::make_uniq<CV2ConnectionCastFunction>(conn);
		*out_function = Convert(function.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_create_with_extension(duckdb_v2_extension_handle extension,
                                                              duckdb_v2_cast_function_handle *out_function,
                                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(extension);
	DUCKDB_CHECK_ARG(out_function);
	*out_function = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &loader = GetExtensionLoader(extension);
		auto function = duckdb::make_uniq<CV2ExtensionCastFunction>(loader);
		*out_function = Convert(function.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_set_source_type(duckdb_v2_cast_function_handle function,
                                                        duckdb_v2_logical_type_handle source_type,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(source_type);
	return WithErrorHandler(err, [&]() { Convert(function)->source_type = *Convert(source_type); });
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_set_target_type(duckdb_v2_cast_function_handle function,
                                                        duckdb_v2_logical_type_handle target_type,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(target_type);
	return WithErrorHandler(err, [&]() { Convert(function)->target_type = *Convert(target_type); });
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_set_implicit_cast_cost(duckdb_v2_cast_function_handle function, int64_t cost,
                                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->implicit_cast_cost = cost; });
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_set_user_data(duckdb_v2_cast_function_handle function,
                                                      duckdb_v2_opaque *user_data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(user_data);
	return WithErrorHandler(err, [&]() {
		Convert(function)->user_data =
		    duckdb::make_shared_ptr<CV2UserData>(user_data->ptr, user_data->destroy, user_data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_set_exec_callback(duckdb_v2_cast_function_handle function,
                                                          duckdb_v2_cast_function_exec_callback_fn callback,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->exec_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_exec_get_user_data(duckdb_v2_cast_function_exec_info_handle info, void **data,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_exec_get_row_count(duckdb_v2_cast_function_exec_info_handle info, idx_t *count,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->count; });
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_exec_get_input(duckdb_v2_cast_function_exec_info_handle info,
                                                       duckdb_v2_vector_handle *vector,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(vector);
	return WithErrorHandler(err, [&]() { *vector = Convert(Convert(info)->input); });
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_exec_get_output(duckdb_v2_cast_function_exec_info_handle info,
                                                        duckdb_v2_vector_handle *vector,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(vector);
	return WithErrorHandler(err, [&]() { *vector = Convert(Convert(info)->output); });
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_exec_get_mode(duckdb_v2_cast_function_exec_info_handle info,
                                                      DUCKDB_V2_CAST_MODE *mode, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(mode);
	return WithErrorHandler(err, [&]() { *mode = Convert(info)->mode; });
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_register(duckdb_v2_cast_function_handle function,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->Register(); });
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_destroy(duckdb_v2_cast_function_handle *function) {
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
