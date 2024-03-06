#include "json_executors.hpp"

namespace duckdb {

//! Prepend String or JSON value to an array
yyjson_mut_val *ArrayPrependStringOrJSON(yyjson_mut_val *arr, yyjson_mut_doc *doc, string_t element, yyjson_alc *alc,
                                         Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not an JSON Array");
	}

	auto edoc = JSONCommon::ReadDocument(element, JSONCommon::READ_FLAG, alc);
	auto mut_edoc = yyjson_doc_mut_copy(edoc, alc);

	yyjson_mut_arr_prepend(arr, mut_edoc->root);
	return arr;
}

//! Prepend boolean value to an array
yyjson_mut_val *ArrayPrependBoolean(yyjson_mut_val *arr, yyjson_mut_doc *doc, bool element, yyjson_alc *alc,
                                    Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not a JSON Array");
	}

	auto mut_value = yyjson_mut_bool(doc, element);
	yyjson_mut_arr_prepend(arr, mut_value);
	return arr;
}

//! Prepend unsigned Integers to an array
yyjson_mut_val *ArrayPrependUnsignedIntegers(yyjson_mut_val *arr, yyjson_mut_doc *doc, uint64_t element,
                                             yyjson_alc *alc, Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not a JSON Array");
	}

	auto mut_value = yyjson_mut_uint(doc, element);
	yyjson_mut_arr_prepend(arr, mut_value);
	return arr;
}

//! Prepend signed Integers to an array
yyjson_mut_val *ArrayPrependSignedIntegers(yyjson_mut_val *arr, yyjson_mut_doc *doc, int64_t element, yyjson_alc *alc,
                                           Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not a JSON Array");
	}

	auto mut_value = yyjson_mut_sint(doc, element);
	yyjson_mut_arr_prepend(arr, mut_value);
	return arr;
}

//! Prepend floating values to an array
yyjson_mut_val *ArrayPrependFloating(yyjson_mut_val *arr, yyjson_mut_doc *doc, double element, yyjson_alc *alc,
                                     Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not a JSON Array");
	}

	auto mut_value = yyjson_mut_real(doc, element);
	yyjson_mut_arr_prepend(arr, mut_value);
	return arr;
}

//! Prepend function wrapper
static void ArrayPrependFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto right_type = args.data[1].GetType();
	D_ASSERT(right_type == LogicalType::VARCHAR || right_type == JSONCommon::JSONType());

	auto left_type = args.data[0].GetType();

	switch (left_type.id()) {
	case LogicalType::VARCHAR:
		JSONExecutors::BinaryMutExecuteFlip<string_t>(args, state, result, ArrayPrependStringOrJSON);
		break;
	case LogicalType::BOOLEAN:
		JSONExecutors::BinaryMutExecuteFlip<bool>(args, state, result, ArrayPrependBoolean);
		break;
	case LogicalType::UBIGINT:
		JSONExecutors::BinaryMutExecuteFlip<uint64_t>(args, state, result, ArrayPrependUnsignedIntegers);
		break;
	case LogicalType::BIGINT:
		JSONExecutors::BinaryMutExecuteFlip<int64_t>(args, state, result, ArrayPrependSignedIntegers);
		break;
	case LogicalType::DOUBLE:
		JSONExecutors::BinaryMutExecuteFlip<double>(args, state, result, ArrayPrependFloating);
		break;
	default:
		// Shouldn't be thrown except implicit casting changes
		throw InvalidInputException("Not a valid input type");
	}
}

static void GetArrayPrependFunctionInternal(ScalarFunctionSet &set, const LogicalType &lhs, const LogicalType &rhs) {
	set.AddFunction(ScalarFunction("json_array_prepend", {lhs, rhs}, JSONCommon::JSONType(), ArrayPrependFunction,
	                               nullptr, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetArrayPrependFunction() {
	ScalarFunctionSet set("json_array_prepend");

	// Use different executor for these
	// Allows booleans directly
	GetArrayPrependFunctionInternal(set, LogicalType::BOOLEAN, JSONCommon::JSONType());

	// Allows for Integer types
	// TINYINT, SMALLINT, INTEGER, UTINYINT, USMALLINT, UINTEGER are captured by UBIGINT and BIGINT	respecively
	// relies on consistant casting strategy upfront

	// unsigned
	GetArrayPrependFunctionInternal(set, LogicalType::UBIGINT, JSONCommon::JSONType());

	// signed
	GetArrayPrependFunctionInternal(set, LogicalType::BIGINT, JSONCommon::JSONType());

	// Allows for floating types
	// FLOAT is covered by automatic upfront casting to double
	GetArrayPrependFunctionInternal(set, LogicalType::DOUBLE, JSONCommon::JSONType());

	// Allows for json and string values
	GetArrayPrependFunctionInternal(set, JSONCommon::JSONType(), JSONCommon::JSONType());
	GetArrayPrependFunctionInternal(set, LogicalType::VARCHAR, JSONCommon::JSONType());

	return set;
}

} // namespace duckdb
