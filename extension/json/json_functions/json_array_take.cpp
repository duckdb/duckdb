#include "json_executors.hpp"

namespace duckdb {

//! Take n elements.
yyjson_mut_val *ArrayTakeElement(yyjson_mut_val *arr, int64_t n, yyjson_alc *alc, Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not an JSON Array");
	}

	if (n <= 0) {
		// TODO: Maybe introduce negative space to take from the end?
		auto doc = JSONCommon::CreateDocument(alc);
		return yyjson_mut_arr(doc);
	}

	size_t last_index = static_cast<size_t>(n);
	size_t array_length = yyjson_mut_arr_size(arr);

	if (last_index >= array_length) {
		return arr;
	}

	yyjson_mut_arr_remove_range(arr, last_index, array_length - last_index);
	return arr;
}

//! Take entry range from array
yyjson_mut_val *ArrayTakeRange(yyjson_mut_val *arr, int64_t start, int64_t end, yyjson_alc *alc, Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not an JSON Array");
	}

	if (start < 0 || end < 0 || start > end) {
		throw InvalidInputException("Invalid range indices");
	}

	size_t array_length = yyjson_mut_arr_size(arr);
	size_t first_index = static_cast<size_t>(start);
	size_t last_index = static_cast<size_t>(end + 1);

	if (first_index > array_length) {
		auto doc = JSONCommon::CreateDocument(alc);
		return yyjson_mut_arr(doc);
	}

	if (last_index <= array_length) {
		yyjson_mut_arr_remove_range(arr, last_index, array_length - last_index);
	}

	yyjson_mut_arr_remove_range(arr, 0, first_index);

	return arr;
}

static void ArrayTakeElementFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto json_type = args.data[0].GetType();
	D_ASSERT(json_type == LogicalType::VARCHAR || json_type == LogicalType::JSON());
	auto idx_type = args.data[1].GetType();
	D_ASSERT(idx_type == LogicalType::BIGINT);

	JSONExecutors::BinaryMutExecute<int64_t>(args, state, result, ArrayTakeElement);
}

static void ArrayTakeRangeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto json_type = args.data[0].GetType();
	D_ASSERT(json_type == LogicalType::VARCHAR || json_type == LogicalType::JSON());
	auto idx_type = args.data[1].GetType();
	D_ASSERT(idx_type == LogicalType::BIGINT);
	auto length_type = args.data[2].GetType();
	D_ASSERT(length_type == LogicalType::BIGINT);

	JSONExecutors::TernaryMutExecute<int64_t, int64_t>(args, state, result, ArrayTakeRange);
}

static void GetArrayTakeElementFunctionInternal(ScalarFunctionSet &set, const LogicalType &fst,
                                                const LogicalType &snd) {
	set.AddFunction(ScalarFunction("json_array_take", {fst, snd}, LogicalType::JSON(), ArrayTakeElementFunction,
	                               nullptr, nullptr, nullptr, JSONFunctionLocalState::Init));
}

static void GetArrayTakeRangeFunctionInternal(ScalarFunctionSet &set, const LogicalType &fst, const LogicalType &snd,
                                              const LogicalType &thrd) {
	set.AddFunction(ScalarFunction("json_array_take", {fst, snd, thrd}, LogicalType::JSON(), ArrayTakeRangeFunction,
	                               nullptr, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetArrayTakeFunction() {
	ScalarFunctionSet set("json_array_take");

	GetArrayTakeElementFunctionInternal(set, LogicalType::JSON(), LogicalType::BIGINT);
	// GetArrayTakeRangeFunctionInternal(set, LogicalType::JSON(), LogicalType::RANGE); // TODO: Check Range option
	GetArrayTakeRangeFunctionInternal(set, LogicalType::JSON(), LogicalType::BIGINT, LogicalType::BIGINT);

	return set;
}

} // namespace duckdb
