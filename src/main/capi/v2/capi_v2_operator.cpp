#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "duckdb/main/capi_v2/capi_v2_function_internal.hpp"

namespace duckdb::capiv2 {

static_assert(static_cast<int>(OperatorResultType::NEED_MORE_INPUT) == DUCKDB_V2_OPERATOR_RESULT_NEED_MORE_INPUT,
              "OperatorResultType::NEED_MORE_INPUT mismatch");
static_assert(static_cast<int>(OperatorResultType::HAVE_MORE_OUTPUT) == DUCKDB_V2_OPERATOR_RESULT_HAVE_MORE_OUTPUT,
              "OperatorResultType::HAVE_MORE_OUTPUT mismatch");
static_assert(static_cast<int>(OperatorResultType::FINISHED) == DUCKDB_V2_OPERATOR_RESULT_FINISHED,
              "OperatorResultType::FINISHED mismatch");

//! BLOCKED is not part of the C API: it needs the engine's interrupt machinery
static auto CV2ConvertOperatorResult(DUCKDB_V2_OPERATOR_RESULT value) -> OperatorResultType {
	if (static_cast<uint32_t>(value) > DUCKDB_V2_OPERATOR_RESULT_FINISHED) {
		throw InvalidInputException("Invalid value in duckdb_v2_operator_execute_set_result");
	}
	return static_cast<OperatorResultType>(value);
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_operator_create(duckdb_v2_factory_handle factory, duckdb_v2_operator_handle *op,
                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(factory);
	DUCKDB_CHECK_ARG(op);
	*op = nullptr;
	return WithErrorHandler(err, [&]() {
		auto result = duckdb::make_uniq<CV2Operator>(Convert(factory)->GetDatabase());
		*op = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_operator_set_user_data(duckdb_v2_operator_handle op, duckdb_v2_opaque *data,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(op);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		Convert(op)->info.user_data = duckdb::make_shared_ptr<CV2UserData>(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_operator_set_init_global_callback(duckdb_v2_operator_handle op,
                                                            duckdb_v2_operator_init_global_callback_fn callback,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(op);
	return WithErrorHandler(err, [&]() { Convert(op)->info.init_global_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_set_init_local_callback(duckdb_v2_operator_handle op,
                                                           duckdb_v2_operator_init_local_callback_fn callback,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(op);
	return WithErrorHandler(err, [&]() { Convert(op)->info.init_local_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_set_execute_callback(duckdb_v2_operator_handle op,
                                                        duckdb_v2_operator_execute_callback_fn callback,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(op);
	return WithErrorHandler(err, [&]() { Convert(op)->info.execute_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_set_finalize_callback(duckdb_v2_operator_handle op,
                                                         duckdb_v2_operator_finalize_callback_fn callback,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(op);
	return WithErrorHandler(err, [&]() { Convert(op)->info.finalize_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_init_global_get_user_data(duckdb_v2_operator_init_global_info_handle info,
                                                             void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_init_global_get_bind_data(duckdb_v2_operator_init_global_info_handle info,
                                                             void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_init_global_set_global_state(duckdb_v2_operator_init_global_info_handle info,
                                                                duckdb_v2_opaque *data,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { Convert(info)->out_global_state = *data; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_init_global_set_max_threads(duckdb_v2_operator_init_global_info_handle info,
                                                               idx_t max_threads, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() {
		if (max_threads == 0) {
			throw duckdb::InvalidInputException("The maximum number of threads must be at least 1");
		}
		Convert(info)->out_max_threads = max_threads;
	});
}

DUCKDB_V2_ERROR duckdb_v2_operator_init_local_get_user_data(duckdb_v2_operator_init_local_info_handle info, void **data,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_init_local_get_bind_data(duckdb_v2_operator_init_local_info_handle info, void **data,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_init_local_get_global_state(duckdb_v2_operator_init_local_info_handle info,
                                                               void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_global_state; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_init_local_set_local_state(duckdb_v2_operator_init_local_info_handle info,
                                                              duckdb_v2_opaque *data,
                                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { Convert(info)->out_local_state = *data; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_execute_get_user_data(duckdb_v2_operator_execute_info_handle info, void **data,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_execute_get_bind_data(duckdb_v2_operator_execute_info_handle info, void **data,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_execute_get_global_state(duckdb_v2_operator_execute_info_handle info, void **data,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_global_state; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_execute_get_local_state(duckdb_v2_operator_execute_info_handle info, void **data,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_local_state; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_execute_get_input_chunk(duckdb_v2_operator_execute_info_handle info,
                                                           duckdb_v2_data_chunk_handle *chunk,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(chunk);
	return WithErrorHandler(err, [&]() { *chunk = Convert(Convert(info)->input.get()); });
}

DUCKDB_V2_ERROR duckdb_v2_operator_execute_get_output_chunk(duckdb_v2_operator_execute_info_handle info,
                                                            duckdb_v2_data_chunk_handle *chunk,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(chunk);
	return WithErrorHandler(err, [&]() { *chunk = Convert(Convert(info)->output.get()); });
}

DUCKDB_V2_ERROR duckdb_v2_operator_execute_set_result(duckdb_v2_operator_execute_info_handle info,
                                                      DUCKDB_V2_OPERATOR_RESULT result,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() { Convert(info)->out_result = CV2ConvertOperatorResult(result); });
}

DUCKDB_V2_ERROR duckdb_v2_operator_finalize_get_user_data(duckdb_v2_operator_finalize_info_handle info, void **data,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_finalize_get_bind_data(duckdb_v2_operator_finalize_info_handle info, void **data,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_finalize_get_global_state(duckdb_v2_operator_finalize_info_handle info, void **data,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_global_state; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_finalize_get_local_state(duckdb_v2_operator_finalize_info_handle info, void **data,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_local_state; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_finalize_get_output_chunk(duckdb_v2_operator_finalize_info_handle info,
                                                             duckdb_v2_data_chunk_handle *chunk,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(chunk);
	return WithErrorHandler(err, [&]() { *chunk = Convert(Convert(info)->output.get()); });
}

DUCKDB_V2_ERROR duckdb_v2_operator_finalize_set_have_more_output(duckdb_v2_operator_finalize_info_handle info,
                                                                 bool have_more_output,
                                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() { Convert(info)->out_have_more_output = have_more_output; });
}

DUCKDB_V2_ERROR duckdb_v2_operator_destroy(duckdb_v2_operator_handle *op) {
	return WithErrorHandler(nullptr, [&]() {
		if (!op) {
			return;
		}
		if (*op) {
			delete Convert(*op);
			*op = nullptr;
		}
	});
}
