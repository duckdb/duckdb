#include "json_executors.hpp"

namespace duckdb {

//! Append String or JSON value to an array
yyjson_mut_val *ArrayAppendStringOrJSON(yyjson_mut_val *arr, yyjson_mut_doc *doc, string_t element, yyjson_alc *alc,
                                        Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not an JSON Array");
	}

	auto edoc = JSONCommon::ReadDocument(element, JSONCommon::READ_FLAG, alc);
	auto mut_edoc = yyjson_doc_mut_copy(edoc, alc);

	yyjson_mut_arr_append(arr, mut_edoc->root);
	return arr;
}

//! Append boolean value to an array
yyjson_mut_val *ArrayAppendBoolean(yyjson_mut_val *arr, yyjson_mut_doc *doc, bool element, yyjson_alc *alc,
                                   Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not a JSON Array");
	}

	yyjson_mut_arr_add_bool(doc, arr, element);
	return arr;
}

//! Append unsigned Integers to an array
yyjson_mut_val *ArrayAppendUnsignedIntegers(yyjson_mut_val *arr, yyjson_mut_doc *doc, uint64_t element, yyjson_alc *alc,
                                            Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not a JSON Array");
	}

	yyjson_mut_arr_add_uint(doc, arr, element);
	return arr;
}

//! Append signed Integers to an array
yyjson_mut_val *ArrayAppendSignedIntegers(yyjson_mut_val *arr, yyjson_mut_doc *doc, int64_t element, yyjson_alc *alc,
                                          Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not a JSON Array");
	}

	yyjson_mut_arr_add_int(doc, arr, element);
	return arr;
}

//! Append floating values to an array
yyjson_mut_val *ArrayAppendFloating(yyjson_mut_val *arr, yyjson_mut_doc *doc, double element, yyjson_alc *alc,
                                    Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not a JSON Array");
	}

	yyjson_mut_arr_add_real(doc, arr, element);
	return arr;
}

//! Append function wrapper
static void ArrayAppendFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto left_type = args.data[0].GetType();
	D_ASSERT(left_type == LogicalType::VARCHAR || left_type == LogicalType::JSON());

	auto right_type = args.data[1].GetType();

	switch (right_type.id()) {
	case LogicalType::VARCHAR:
		JSONExecutors::BinaryMutExecute<string_t>(args, state, result, ArrayAppendStringOrJSON);
		break;
	case LogicalType::BOOLEAN:
		JSONExecutors::BinaryMutExecute<bool>(args, state, result, ArrayAppendBoolean);
		break;
	case LogicalType::UBIGINT:
		JSONExecutors::BinaryMutExecute<uint64_t>(args, state, result, ArrayAppendUnsignedIntegers);
		break;
	case LogicalType::BIGINT:
		JSONExecutors::BinaryMutExecute<int64_t>(args, state, result, ArrayAppendSignedIntegers);
		break;
	case LogicalType::DOUBLE:
		JSONExecutors::BinaryMutExecute<double>(args, state, result, ArrayAppendFloating);
		break;
	default:
		// Shouldn't be thrown except implicit casting changes
		throw InvalidInputException("Not a valid input type");
	}
}

static void GetArrayAppendFunctionInternal(ScalarFunctionSet &set, const LogicalType &lhs, const LogicalType &rhs) {
	set.AddFunction(ScalarFunction("json_array_append", {lhs, rhs}, LogicalType::JSON(), ArrayAppendFunction,
	                               nullptr, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetArrayAppendFunction() {
	ScalarFunctionSet set("json_array_append");

	// Use different executor for these
	// Allows booleans directly
	// GetArrayAppendFunctionInternal(set, LogicalType::JSON(), LogicalType::BOOLEAN);

	// Allows for Integer types
	// TINYINT, SMALLINT, INTEGER, UTINYINT, USMALLINT, UINTEGER are captured by UBIGINT and BIGINT	respecively
	// relies on consistant casting strategy upfront

	// unsigned
	// GetArrayAppendFunctionInternal(set, LogicalType::JSON(), LogicalType::UBIGINT);

	// signed
	// GetArrayAppendFunctionInternal(set, LogicalType::JSON(), LogicalType::BIGINT);

	// Allows for floating types
	// FLOAT is covered by automatic upfront casting to double
	// GetArrayAppendFunctionInternal(set, LogicalType::JSON(), LogicalType::DOUBLE);

	// Allows for json and string values
	GetArrayAppendFunctionInternal(set, LogicalType::JSON(), LogicalType::JSON());
	GetArrayAppendFunctionInternal(set, LogicalType::JSON(), LogicalType::VARCHAR);

	return set;
}

} // namespace duckdb
