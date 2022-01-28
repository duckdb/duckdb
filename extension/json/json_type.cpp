#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

static inline bool GetType(yyjson_val *val, string_t &result) {
	switch (yyjson_get_type(val)) {
	case YYJSON_TYPE_NULL:
		result = string_t("null");
		break;
	case YYJSON_TYPE_BOOL:
		result = string_t("boolean");
		break;
	case YYJSON_TYPE_NUM:
		switch (unsafe_yyjson_get_subtype(val)) {
		case YYJSON_SUBTYPE_UINT:
		case YYJSON_SUBTYPE_SINT:
			result = string_t("integer");
			break;
		case YYJSON_SUBTYPE_REAL:
			result = string_t("real");
			break;
		default:
			return false;
		}
		break;
	case YYJSON_TYPE_STR:
		result = string_t("string");
		break;
	case YYJSON_TYPE_ARR:
		result = string_t("array");
		break;
	case YYJSON_TYPE_OBJ:
		result = string_t("object");
		break;
	default:
		return false;
	}
	return true;
}

static void UnaryTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
	    inputs, result, args.size(), [&](string_t input, ValidityMask &mask, idx_t idx) {
		    string_t result_val {};
		    if (!GetType(JSONCommon::GetRootUnsafe(input), result_val)) {
			    mask.SetInvalid(idx);
		    }
		    return result_val;
	    });
}

static void BinaryTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	const auto &info = (JSONFunctionData &)*func_expr.bind_info;

	auto &inputs = args.data[0];
	if (info.constant) {
		// Constant query
		const char *ptr = info.path.c_str();
		const idx_t &len = info.len;
		UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
		    inputs, result, args.size(), [&](string_t input, ValidityMask &mask, idx_t idx) {
			    string_t result_val {};
			    if (!GetType(JSONCommon::GetPointerUnsafe(input, ptr, len), result_val)) {
				    mask.SetInvalid(idx);
			    }
			    return result_val;
		    });
	} else {
		// Columnref query
		auto &queries = args.data[1];
		BinaryExecutor::ExecuteWithNulls<string_t, string_t, string_t>(
		    inputs, queries, result, args.size(), [&](string_t input, string_t query, ValidityMask &mask, idx_t idx) {
			    string_t result_val {};
			    if (!GetType(JSONCommon::GetPointer(input, query), result_val)) {
				    mask.SetInvalid(idx);
			    }
			    return result_val;
		    });
	}
}

CreateScalarFunctionInfo JSONFunctions::GetTypeFunction() {
	ScalarFunctionSet set("json_type");
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, UnaryTypeFunction, false, nullptr,
	                               nullptr, nullptr));
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               BinaryTypeFunction, false, JSONFunctionData::Bind, nullptr, nullptr));

	return CreateScalarFunctionInfo(move(set));
}

} // namespace duckdb
