#include "json_executors.hpp"

namespace duckdb {

//! Prepend String or JSON value to an array
yyjson_mut_val *ArrayPrependJSON(yyjson_mut_val *element, string_t arr, yyjson_alc *alc, Vector &result) {
	auto arrdoc = JSONCommon::ReadDocument(arr, JSONCommon::READ_FLAG, alc);
	auto mut_arr = yyjson_doc_mut_copy(arrdoc, alc)->root;

	if (!yyjson_mut_is_arr(mut_arr)) {
		throw InvalidInputException("JSON input not an JSON Array");
	}

	yyjson_mut_arr_prepend(mut_arr, element);
	return mut_arr;
}

//! Prepend function wrapper
static void ArrayPrependFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto left_type = args.data[0].GetType();
	D_ASSERT(left_type == LogicalType::VARCHAR || left_type == LogicalType::JSON());
	auto right_type = args.data[1].GetType();
	D_ASSERT(right_type == LogicalType::VARCHAR || right_type == LogicalType::JSON());

	JSONExecutors::BinaryMutExecute<string_t>(args, state, result, ArrayPrependJSON);
}

static void GetArrayPrependFunctionInternal(ScalarFunctionSet &set, const LogicalType &lhs, const LogicalType &rhs) {
	set.AddFunction(ScalarFunction("json_array_prepend", {lhs, rhs}, LogicalType::JSON(), ArrayPrependFunction, nullptr,
	                               nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetArrayPrependFunction() {
	ScalarFunctionSet set("json_array_prepend");
	GetArrayPrependFunctionInternal(set, LogicalType::JSON(), LogicalType::JSON());

	return set;
}

} // namespace duckdb
