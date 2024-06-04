#include "json_executors.hpp"

namespace duckdb {

//! Rotate String or JSON value to an array
template <class AMOUNT_TYPE>
yyjson_mut_val *ArrayRotate(yyjson_mut_val *arr, AMOUNT_TYPE amount, yyjson_alc *alc, Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not an JSON Array");
	}

	size_t array_size = yyjson_mut_arr_size(arr);
	if (amount == 0 || array_size == 0) {
		return arr;
	}

	size_t shift_amount = 0;

	if (amount < 0) {
		shift_amount = array_size - static_cast<size_t>(-(amount % static_cast<int64_t>(array_size)));
	} else {
		shift_amount = static_cast<size_t>(amount) % array_size;
	}

	yyjson_mut_arr_rotate(arr, shift_amount);

	return arr;
}

//! Rotate function wrapper
static void ArrayRotateFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto json_type = args.data[0].GetType();
	D_ASSERT(json_type == LogicalType::VARCHAR || json_type == LogicalType::JSON());
	auto argument_type = args.data[1].GetType();
	D_ASSERT(argument_type == LogicalType::BIGINT || argument_type == LogicalType::UBIGINT);

	switch (argument_type.id()) {
	case LogicalType::BIGINT:
		JSONExecutors::BinaryMutExecute<int64_t>(args, state, result, ArrayRotate<int64_t>);
		break;
	case LogicalType::UBIGINT:
		JSONExecutors::BinaryMutExecute<int64_t>(args, state, result, ArrayRotate<uint64_t>);
		break;
	default:
		// Shouldn't be thrown except implicit casting changes
		throw InvalidInputException("Not a valid input type");
	}
}

static void GetArrayRotateFunctionInternal(ScalarFunctionSet &set, const LogicalType &fst, const LogicalType &snd) {
	set.AddFunction(ScalarFunction("json_array_rotate", {fst, snd}, LogicalType::JSON(), ArrayRotateFunction, nullptr,
	                               nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetArrayRotateFunction() {
	ScalarFunctionSet set("json_array_rotate");

	GetArrayRotateFunctionInternal(set, LogicalType::JSON(), LogicalType::BIGINT);
	GetArrayRotateFunctionInternal(set, LogicalType::JSON(), LogicalType::UBIGINT);

	return set;
}

} // namespace duckdb
