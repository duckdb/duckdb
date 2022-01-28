#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

template <class T>
static inline bool ExtractFromVal(yyjson_val *val, T &result) {
	throw NotImplementedException("Cannot extract JSON of this type");
}

template <>
inline bool ExtractFromVal(yyjson_val *val, bool &result) {
	auto valid = yyjson_is_bool(val);
	if (valid) {
		result = unsafe_yyjson_get_bool(val);
	}
	return valid;
}

template <>
inline bool ExtractFromVal(yyjson_val *val, int32_t &result) {
	auto valid = yyjson_is_int(val);
	if (valid) {
		result = unsafe_yyjson_get_int(val);
	}
	return valid;
}

template <>
inline bool ExtractFromVal(yyjson_val *val, int64_t &result) {
	// Needs to check whether it is int first, otherwise we get NULL for small values
	auto valid = yyjson_is_int(val) || yyjson_is_sint(val);
	if (valid) {
		result = unsafe_yyjson_get_sint(val);
	}
	return valid;
}

template <>
inline bool ExtractFromVal(yyjson_val *val, uint64_t &result) {
	auto valid = yyjson_is_uint(val);
	if (valid) {
		result = unsafe_yyjson_get_uint(val);
	}
	return valid;
}

template <>
inline bool ExtractFromVal(yyjson_val *val, double &result) {
	auto valid = yyjson_is_real(val);
	if (valid) {
		result = unsafe_yyjson_get_real(val);
	}
	return valid;
}

template <>
inline bool ExtractFromVal(yyjson_val *val, string_t &result) {
	auto valid = yyjson_is_str(val);
	if (valid) {
		result = string_t(unsafe_yyjson_get_str(val), unsafe_yyjson_get_len(val));
	}
	return valid;
}

template <class T>
static inline bool TemplatedExtract(const string_t &input, const string_t &query, T &result) {
	return ExtractFromVal<T>(JSONCommon::GetPointer(input, query), result);
}

template <class T>
static inline bool TemplatedExtract(const string_t &input, const char *ptr, const idx_t &len, T &result) {
	return ExtractFromVal<T>(JSONCommon::GetPointerUnsafe(input, ptr, len), result);
}

template <class T>
static void TemplatedExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	const auto &info = (JSONFunctionData &)*func_expr.bind_info;

	auto &inputs = args.data[0];
	if (info.constant) {
		// Constant query
		const char *ptr = info.path.c_str();
		const idx_t &len = info.len;
		UnaryExecutor::ExecuteWithNulls<string_t, T>(inputs, result, args.size(),
		                                             [&](string_t input, ValidityMask &mask, idx_t idx) {
			                                             T result_val {};
			                                             if (!TemplatedExtract<T>(input, ptr, len, result_val)) {
				                                             mask.SetInvalid(idx);
			                                             }
			                                             return result_val;
		                                             });
	} else {
		// Columnref query
		auto &queries = args.data[1];
		BinaryExecutor::ExecuteWithNulls<string_t, string_t, T>(
		    inputs, queries, result, args.size(), [&](string_t input, string_t query, ValidityMask &mask, idx_t idx) {
			    T result_val {};
			    if (!TemplatedExtract<T>(input, query, result_val)) {
				    mask.SetInvalid(idx);
			    }
			    return result_val;
		    });
	}
}

static void AddFunctionAliases(vector<CreateScalarFunctionInfo> &functions, vector<string> names, ScalarFunction fun) {
	for (const auto &name : names) {
		fun.name = name;
		functions.push_back(CreateScalarFunctionInfo(fun));
	}
}

vector<CreateScalarFunctionInfo> JSONFunctions::GetExtractFunctions() {
	vector<CreateScalarFunctionInfo> functions;
	auto bool_fun = ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                               TemplatedExtractFunction<bool>, false, JSONFunctionData::Bind, nullptr, nullptr);
	auto int_fun = ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::INTEGER,
	                              TemplatedExtractFunction<int32_t>, false, JSONFunctionData::Bind, nullptr, nullptr);
	auto bigint_fun =
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BIGINT,
	                   TemplatedExtractFunction<int64_t>, false, JSONFunctionData::Bind, nullptr, nullptr);
	auto ubigint_fun =
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::UBIGINT,
	                   TemplatedExtractFunction<uint64_t>, false, JSONFunctionData::Bind, nullptr, nullptr);
	auto double_fun = ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE,
	                                 TemplatedExtractFunction<double>, false, JSONFunctionData::Bind, nullptr, nullptr);
	auto string_fun =
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                   TemplatedExtractFunction<string_t>, false, JSONFunctionData::Bind, nullptr, nullptr);

	AddFunctionAliases(functions, {"json_extract_bool", "json_extract_boolean"}, bool_fun);
	AddFunctionAliases(functions, {"json_extract_int", "json_extract_integer"}, int_fun);
	AddFunctionAliases(functions, {"json_extract_bigint"}, bigint_fun);
	AddFunctionAliases(functions, {"json_extract_ubigint"}, ubigint_fun);
	AddFunctionAliases(functions, {"json_extract_double"}, double_fun);
	AddFunctionAliases(functions, {"json_extract_string", "json_extract_varchar"}, string_fun);

	return functions;
}

} // namespace duckdb
