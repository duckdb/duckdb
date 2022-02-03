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

static void ExtractManyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::ManyJSONReadFunction<string_t>(args, state, result, ExtractFromVal);
}

template <class T>
static void AddFunction(vector<CreateScalarFunctionInfo> &functions, vector<string> names, LogicalType return_type) {
	ScalarFunctionSet set("");
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, return_type,
	                               TemplatedTypedExtractFunction<T>, false, JSONReadFunctionData::Bind, nullptr,
	                               nullptr));
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(return_type), TemplatedTypedExtractFunction<T>, false,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr));
	for (const auto &name : names) {
		set.name = name;
		functions.push_back(CreateScalarFunctionInfo(set));
	}
}

vector<CreateScalarFunctionInfo> JSONFunctions::GetExtractFunctions() {
	vector<CreateScalarFunctionInfo> functions;

	// Typed extract functions
	AddFunction<bool>(functions, {"json_extract_bool", "json_extract_boolean"}, LogicalType::BOOLEAN);
	AddFunction<int32_t>(functions, {"json_extract_int", "json_extract_integer"}, LogicalType::INTEGER);
	AddFunction<int64_t>(functions, {"json_extract_bigint"}, LogicalType::BIGINT);
	AddFunction<uint64_t>(functions, {"json_extract_ubigint"}, LogicalType::UBIGINT);
	AddFunction<double>(functions, {"json_extract_double"}, LogicalType::DOUBLE);
	AddFunction<string_t>(functions, {"json_extract_string", "json_extract_varchar"}, LogicalType::VARCHAR);

	// Generic extract function
	ScalarFunctionSet set("json_extract");
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, LogicalType::JSON, ExtractFunction, false,
	                               JSONReadFunctionData::Bind, nullptr, nullptr));
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::JSON), ExtractManyFunction, false,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr));
	functions.push_back(CreateScalarFunctionInfo(set));

	return functions;
}

} // namespace duckdb
