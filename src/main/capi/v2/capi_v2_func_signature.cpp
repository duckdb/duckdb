#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_function_signature_add_parameter(duckdb_v2_function_signature_handle sig,
                                                           duckdb_v2_identifier_t name,
                                                           duckdb_v2_logical_type_handle type,
                                                           duckdb_v2_value_handle value,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(sig);
	DUCKDB_CHECK_ARG(type);

	return WithErrorHandler(err, [&]() {
		auto &signature = *Convert(sig);
		if (value) {
			signature.AddParameter(duckdb::Identifier(Convert(name)), *Convert(type), *Convert(value));
		} else {
			signature.AddParameter(duckdb::Identifier(Convert(name)), *Convert(type));
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_signature_set_varargs(duckdb_v2_function_signature_handle sig,
                                                         duckdb_v2_logical_type_handle type,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(sig);
	DUCKDB_CHECK_ARG(type);

	return WithErrorHandler(err, [&]() {
		auto &signature = *Convert(sig);
		signature.SetVarArgs(*Convert(type));
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_signature_set_return_type(duckdb_v2_function_signature_handle sig,
                                                             duckdb_v2_logical_type_handle type,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(sig);
	DUCKDB_CHECK_ARG(type);

	return WithErrorHandler(err, [&]() {
		auto &signature = *Convert(sig);
		signature.SetReturnType(*Convert(type));
	});
}
