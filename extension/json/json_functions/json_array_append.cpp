#include "json_executors.hpp"

namespace duckdb {

//! Append String or JSON value to an array
yyjson_mut_val *ArrayAppendJSON(yyjson_mut_val *arr, string_t element, yyjson_alc *alc, Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not an JSON Array");
	}

	auto edoc = JSONCommon::ReadDocument(element, JSONCommon::READ_FLAG, alc);
	auto mut_edoc = yyjson_doc_mut_copy(edoc, alc);

	yyjson_mut_arr_append(arr, mut_edoc->root);
	return arr;
}

//! Append function wrapper
static void ArrayAppendFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto left_type = args.data[0].GetType();
	D_ASSERT(left_type == LogicalType::VARCHAR || left_type == LogicalType::JSON());
	auto right_type = args.data[1].GetType();
	D_ASSERT(right_type == LogicalType::VARCHAR || right_type == LogicalType::JSON());

	JSONExecutors::BinaryMutExecute<string_t>(args, state, result, ArrayAppendJSON);
}

static void GetArrayAppendFunctionInternal(ScalarFunctionSet &set, const LogicalType &lhs, const LogicalType &rhs) {
	set.AddFunction(ScalarFunction("json_array_append", {lhs, rhs}, LogicalType::JSON(), ArrayAppendFunction, nullptr,
	                               nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetArrayAppendFunction() {
	ScalarFunctionSet set("json_array_append");
	GetArrayAppendFunctionInternal(set, LogicalType::JSON(), LogicalType::JSON());

	return set;
}

} // namespace duckdb
