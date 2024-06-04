#include "json_executors.hpp"

namespace duckdb {

// yyjson_mut_val *JSONArrayTail(yyjson_mut_val *arr, yyjson_mut_doc *doc, yyjson_alc *alc, Vector &result) {
yyjson_mut_val *JSONArrayTail(yyjson_mut_val *arr, yyjson_alc *alc, Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not an JSON Array");
	}

	if (yyjson_mut_arr_size(arr) == 0) {
		auto doc = JSONCommon::CreateDocument(alc);
		return yyjson_mut_arr(doc);
	}

	yyjson_mut_arr_remove_first(arr);
	return arr;
}

static void ArrayTailFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::UnaryMutExecute(args, state, result, JSONArrayTail);
}

static void GetArrayTailFunctionInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction("json_array_tail", {input_type}, LogicalType::JSON(), ArrayTailFunction, nullptr,
	                               nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetArrayTailFunction() {
	ScalarFunctionSet set("json_array_tail");
	GetArrayTailFunctionInternal(set, LogicalType::VARCHAR);
	GetArrayTailFunctionInternal(set, LogicalType::JSON());
	return set;
}

} // namespace duckdb
