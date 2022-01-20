#include "duckdb/execution/expression_executor.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

static inline bool GetType(const string_t &input, string_t &result) {
	yyjson_val *val = JSONCommon::GetRoot(input);
	switch (yyjson_get_type(val)) {
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

static inline bool GetType(const string_t &input, const char *ptr, const idx_t &len, string_t &result) {
	yyjson_val *val = JSONCommon::GetPointer(input, ptr, len);
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
	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(inputs, result, args.size(),
	                                                    [&](string_t input, ValidityMask &mask, idx_t idx) {
		                                                    string_t result_val {};
		                                                    if (!GetType(input, result_val)) {
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
		UnaryExecutor::ExecuteWithNulls<string_t, string_t>(inputs, result, args.size(),
		                                                    [&](string_t input, ValidityMask &mask, idx_t idx) {
			                                                    string_t result_val {};
			                                                    if (!GetType(input, ptr, len, result_val)) {
				                                                    mask.SetInvalid(idx);
			                                                    }
			                                                    return result_val;
		                                                    });
	} else {
		// Columnref query
		auto &queries = args.data[1];
		BinaryExecutor::ExecuteWithNulls<string_t, string_t, string_t>(
		    inputs, queries, result, args.size(), [&](string_t input, string_t query, ValidityMask &mask, idx_t idx) {
			    string path;
			    idx_t len;
			    string_t result_val {};
			    if (!JSONCommon::ConvertToPath(query, path, len) || !GetType(input, path.c_str(), len, result_val)) {
				    mask.SetInvalid(idx);
			    }
			    return result_val;
		    });
	}
}

void JSONFunctions::AddTypeFunction(ClientContext &context) {
	auto &catalog = Catalog::GetCatalog(context);
	ScalarFunctionSet set("json_type");
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, UnaryTypeFunction, false, nullptr,
	                               nullptr, nullptr));
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               BinaryTypeFunction, false, JSONFunctionData::Bind, nullptr, nullptr));
	CreateScalarFunctionInfo type_fun(move(set));
	catalog.CreateFunction(context, &type_fun);
}

} // namespace duckdb
