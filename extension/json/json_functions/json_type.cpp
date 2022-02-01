#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

static inline bool GetType(yyjson_val *val, string_t &result) {
	if (val) {
		switch (unsafe_yyjson_get_type(val)) {
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
	}
	return val;
}

static void UnaryTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::TemplatedUnaryJSONFunction<string_t>(args, state, result, GetType);
}

static void BinaryTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::TemplatedBinaryJSONFunction<string_t>(args, state, result, GetType);
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
