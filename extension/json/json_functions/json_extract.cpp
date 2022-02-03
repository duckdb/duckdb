#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

template <class T>
static inline bool TypedExtractFromVal(yyjson_val *val, T &result) {
	throw NotImplementedException("Cannot extract JSON of this type");
}

template <>
inline bool TypedExtractFromVal(yyjson_val *val, bool &result) {
	auto valid = yyjson_is_bool(val);
	if (valid) {
		result = unsafe_yyjson_get_bool(val);
	}
	return valid;
}

template <>
inline bool TypedExtractFromVal(yyjson_val *val, int32_t &result) {
	auto valid = yyjson_is_int(val);
	if (valid) {
		result = unsafe_yyjson_get_int(val);
	}
	return valid;
}

template <>
inline bool TypedExtractFromVal(yyjson_val *val, int64_t &result) {
	// Needs to check whether it is int first, otherwise we get NULL for small values
	auto valid = yyjson_is_int(val) || yyjson_is_sint(val);
	if (valid) {
		result = unsafe_yyjson_get_sint(val);
	}
	return valid;
}

template <>
inline bool TypedExtractFromVal(yyjson_val *val, uint64_t &result) {
	auto valid = yyjson_is_uint(val);
	if (valid) {
		result = unsafe_yyjson_get_uint(val);
	}
	return valid;
}

template <>
inline bool TypedExtractFromVal(yyjson_val *val, double &result) {
	auto valid = yyjson_is_real(val);
	if (valid) {
		result = unsafe_yyjson_get_real(val);
	}
	return valid;
}

template <>
inline bool TypedExtractFromVal(yyjson_val *val, string_t &result) {
	auto valid = yyjson_is_str(val);
	if (valid) {
		result = string_t(unsafe_yyjson_get_str(val), unsafe_yyjson_get_len(val));
	}
	return valid;
}

template <class T>
static void TemplatedTypedExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::BinaryJSONReadFunction<T>(args, state, result, TypedExtractFromVal<T>);
}

static inline bool ExtractFromVal(yyjson_val *val, string_t &result) {
	if (val) {
		result = JSONCommon::WriteDocument(val);
	}
	return val;
}

static void ExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::BinaryJSONReadFunction<string_t>(args, state, result, ExtractFromVal);
}

static void AddFunctionAliases(vector<CreateScalarFunctionInfo> &functions, vector<string> names, ScalarFunction fun) {
	for (const auto &name : names) {
		fun.name = name;
		functions.push_back(CreateScalarFunctionInfo(fun));
	}
}

vector<CreateScalarFunctionInfo> JSONFunctions::GetExtractFunctions() {
	// Typed extract functions
	vector<CreateScalarFunctionInfo> functions;
	auto bool_fun =
	    ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                   TemplatedTypedExtractFunction<bool>, false, JSONReadFunctionData::Bind, nullptr, nullptr);
	auto int_fun =
	    ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, LogicalType::INTEGER,
	                   TemplatedTypedExtractFunction<int32_t>, false, JSONReadFunctionData::Bind, nullptr, nullptr);
	auto bigint_fun =
	    ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, LogicalType::BIGINT,
	                   TemplatedTypedExtractFunction<int64_t>, false, JSONReadFunctionData::Bind, nullptr, nullptr);
	auto ubigint_fun =
	    ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, LogicalType::UBIGINT,
	                   TemplatedTypedExtractFunction<uint64_t>, false, JSONReadFunctionData::Bind, nullptr, nullptr);
	auto double_fun =
	    ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, LogicalType::DOUBLE,
	                   TemplatedTypedExtractFunction<double>, false, JSONReadFunctionData::Bind, nullptr, nullptr);
	auto string_fun =
	    ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                   TemplatedTypedExtractFunction<string_t>, false, JSONReadFunctionData::Bind, nullptr, nullptr);

	AddFunctionAliases(functions, {"json_extract_bool", "json_extract_boolean"}, bool_fun);
	AddFunctionAliases(functions, {"json_extract_int", "json_extract_integer"}, int_fun);
	AddFunctionAliases(functions, {"json_extract_bigint"}, bigint_fun);
	AddFunctionAliases(functions, {"json_extract_ubigint"}, ubigint_fun);
	AddFunctionAliases(functions, {"json_extract_double"}, double_fun);
	AddFunctionAliases(functions, {"json_extract_string", "json_extract_varchar"}, string_fun);

	// Non-typed extract function
	// Set because we intend to add the {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)} variant
	ScalarFunctionSet set("json_extract");
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, LogicalType::JSON, ExtractFunction, false,
	                               JSONReadFunctionData::Bind, nullptr, nullptr));
	functions.push_back(CreateScalarFunctionInfo(set));

	return functions;
}

} // namespace duckdb
