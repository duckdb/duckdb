#include "json_executors.hpp"

namespace duckdb {

//! Remove element by its index (allows for negative indeces from the back)
yyjson_mut_val *ArrayRemoveElement(yyjson_mut_val *arr, yyjson_mut_doc *doc, int64_t idx, yyjson_alc *alc,
                                   Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not an JSON Array");
	}

	size_t index = DetermineArrayIndex(arr, idx);

	if (index == 0) {
		yyjson_mut_arr_remove_first(arr);
		return arr;
	}

	if (index >= yyjson_mut_arr_size(arr)) {
		return arr;
	}

	yyjson_mut_arr_remove(arr, index);
	return arr;
}

//! Delete entry range from json array
yyjson_mut_val *ArrayRemoveRange(yyjson_mut_val *arr, yyjson_mut_doc *doc, int64_t start, int64_t end, yyjson_alc *alc,
                                 Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not an JSON Array");
	}

	if (start < 0 || end < 0) {
		// TODO: Maybe introduce negative indices similar to list slicing
		throw InvalidInputException("Only positives indices in range deletion");
	}

	if (start > end) {
		throw InvalidInputException("Start index not smaller or equal to last index");
	}

	size_t array_length = yyjson_mut_arr_size(arr);
	size_t index = static_cast<size_t>(start);
	size_t length = static_cast<size_t>(end - start + 1);

	if (index > array_length) {
		return arr;
	}

	if ((index + length) > array_length) {
		length = array_length - index;
	}

	yyjson_mut_arr_remove_range(arr, index, length);
	return arr;
}

static void ArrayRemoveElementFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto json_type = args.data[0].GetType();
	D_ASSERT(json_type == LogicalType::VARCHAR || json_type == JSONCommon::JSONType());
	auto idx_type = args.data[1].GetType();
	D_ASSERT(idx_type == LogicalType::BIGINT);

	JSONExecutors::BinaryMutExecute<int64_t>(args, state, result, ArrayRemoveElement);
}

static void ArrayRemoveRangeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto json_type = args.data[0].GetType();
	D_ASSERT(json_type == LogicalType::VARCHAR || json_type == JSONCommon::JSONType());
	auto idx_type = args.data[1].GetType();
	D_ASSERT(idx_type == LogicalType::BIGINT);
	auto length_type = args.data[2].GetType();
	D_ASSERT(length_type == LogicalType::BIGINT);

	JSONExecutors::TernaryMutExecute<int64_t, int64_t>(args, state, result, ArrayRemoveRange);
}

static void GetArrayRemoveElementFunctionInternal(ScalarFunctionSet &set, const LogicalType &fst,
                                                  const LogicalType &snd) {
	set.AddFunction(ScalarFunction("json_array_remove", {fst, snd}, JSONCommon::JSONType(), ArrayRemoveElementFunction,
	                               nullptr, nullptr, nullptr, JSONFunctionLocalState::Init));
}

static void GetArrayRemoveRangeFunctionInternal(ScalarFunctionSet &set, const LogicalType &fst, const LogicalType &snd,
                                                const LogicalType &thrd) {
	set.AddFunction(ScalarFunction("json_array_remove", {fst, snd, thrd}, JSONCommon::JSONType(),
	                               ArrayRemoveRangeFunction, nullptr, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetArrayRemoveFunction() {
	ScalarFunctionSet set("json_array_remove");

	GetArrayRemoveElementFunctionInternal(set, JSONCommon::JSONType(), LogicalType::BIGINT);
	// GetArrayRemoveRangeFunctionInternal(set, JSONCommon::JSONType(), LogicalType::RANGE); // TODO: Check Range option
	GetArrayRemoveRangeFunctionInternal(set, JSONCommon::JSONType(), LogicalType::BIGINT, LogicalType::BIGINT);

	return set;
}

} // namespace duckdb
