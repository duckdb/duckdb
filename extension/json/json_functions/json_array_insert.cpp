#include "json_executors.hpp"

namespace duckdb {

//! Insert String or JSON value to an array
yyjson_mut_val *ArrayInsertJSON(yyjson_mut_val *arr, string_t element, int64_t idx, yyjson_alc *alc, Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not an JSON Array");
	}

	size_t index = DetermineArrayIndex(arr, idx);

	// Fill remaining indeces with null until element index
	auto doc = JSONCommon::CreateDocument(alc);
	for (size_t entries = yyjson_mut_arr_size(arr); entries < index; ++entries) {
		yyjson_mut_arr_add_null(doc, arr);
	}

	auto edoc = JSONCommon::ReadDocument(element, JSONCommon::READ_FLAG, alc);
	auto mut_edoc = yyjson_doc_mut_copy(edoc, alc);
	yyjson_mut_arr_insert(arr, mut_edoc->root, index);
	return arr;
}

//! Insert function wrapper
static void ArrayInsertFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto json_type = args.data[0].GetType();
	D_ASSERT(json_type == LogicalType::VARCHAR || json_type == LogicalType::JSON());
	auto element_type = args.data[1].GetType();
	D_ASSERT(element_type == LogicalType::VARCHAR || element_type == LogicalType::JSON());
	auto idx_type = args.data[2].GetType();
	D_ASSERT(idx_type == LogicalType::BIGINT);

	JSONExecutors::TernaryMutExecute<string_t, int64_t>(args, state, result, ArrayInsertJSON);
}

static void GetArrayInsertFunctionInternal(ScalarFunctionSet &set, const LogicalType &fst, const LogicalType &snd,
                                           const LogicalType &thrd) {
	set.AddFunction(ScalarFunction("json_array_insert", {fst, snd, thrd}, LogicalType::JSON(), ArrayInsertFunction,
	                               nullptr, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetArrayInsertFunction() {
	ScalarFunctionSet set("json_array_insert");
	GetArrayInsertFunctionInternal(set, LogicalType::JSON(), LogicalType::JSON(), LogicalType::BIGINT);

	return set;
}

} // namespace duckdb
