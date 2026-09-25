#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "duckdb/main/capi/capi_function_signature.hpp"

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
		auto param_name = duckdb::Identifier(ConvertIdentifierName(name));
		duckdb::optional<duckdb::Value> default_value;
		if (value) {
			default_value = *Convert(value);
		}
		// the varargs can be set before the parameters, but they come after them in the signature
		duckdb::CAPIFunctionSignature::AddParameter(
		    signature, duckdb::FunctionParameter(std::move(param_name), *Convert(type), std::move(default_value)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_signature_set_varargs(duckdb_v2_function_signature_handle sig,
                                                         duckdb_v2_logical_type_handle type,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(sig);
	DUCKDB_CHECK_ARG(type);

	return WithErrorHandler(err, [&]() {
		auto &signature = *Convert(sig);
		duckdb::CAPIFunctionSignature::SetVarArgs(signature, *Convert(type));
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
