#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

static inline bool GetArrayLength(yyjson_val *val, uint64_t &result) {
	result = yyjson_arr_size(val);
	return val;
}

static void UnaryArrayLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	UnaryExecutor::ExecuteWithNulls<string_t, uint64_t>(
	    inputs, result, args.size(), [&](string_t input, ValidityMask &mask, idx_t idx) {
		    uint64_t result_val;
		    if (!GetArrayLength(JSONCommon::GetRootUnsafe(input), result_val)) {
			    mask.SetInvalid(idx);
		    }
		    return result_val;
	    });
}

static void BinaryArrayLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	const auto &info = (JSONFunctionData &)*func_expr.bind_info;

	auto &inputs = args.data[0];
	if (info.constant) {
		// Constant query
		const char *ptr = info.path.c_str();
		const idx_t &len = info.len;
		UnaryExecutor::ExecuteWithNulls<string_t, uint64_t>(
		    inputs, result, args.size(), [&](string_t input, ValidityMask &mask, idx_t idx) {
			    uint64_t result_val;
			    if (!GetArrayLength(JSONCommon::GetPointerUnsafe(input, ptr, len), result_val)) {
				    mask.SetInvalid(idx);
			    }
			    return result_val;
		    });
	} else {
		// Columnref query
		auto &queries = args.data[1];
		BinaryExecutor::ExecuteWithNulls<string_t, string_t, uint64_t>(
		    inputs, queries, result, args.size(), [&](string_t input, string_t query, ValidityMask &mask, idx_t idx) {
			    uint64_t result_val;
			    if (!GetArrayLength(JSONCommon::GetPointer(input, query), result_val)) {
				    mask.SetInvalid(idx);
			    }
			    return result_val;
		    });
	}
}

CreateScalarFunctionInfo JSONFunctions::GetArrayLengthFunction() {
	ScalarFunctionSet set("json_array_length");
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::UBIGINT, UnaryArrayLengthFunction, false,
	                               nullptr, nullptr, nullptr));
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::UBIGINT,
	                               BinaryArrayLengthFunction, false, JSONFunctionData::Bind, nullptr, nullptr));

	return CreateScalarFunctionInfo(move(set));
}

} // namespace duckdb
