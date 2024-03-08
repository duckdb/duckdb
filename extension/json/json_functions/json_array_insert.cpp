#include "json_executors.hpp"

namespace duckdb {

//! Insert String or JSON value to an array
yyjson_mut_val *ArrayInsertStringOrJSON(yyjson_mut_val *arr, yyjson_mut_doc *doc, string_t element, int64_t idx,
                                        yyjson_alc *alc, Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not an JSON Array");
	}

	// Determine index
	auto array_size = yyjson_mut_arr_size(arr);
	size_t index = static_cast<size_t>(idx);

	// negative indexes address from the rear
	if (idx < 0) {
		index = static_cast<size_t>(static_cast<int64_t>(array_size) + idx + 1);
	}
	// In case of |idx| > array_size clamp to 0
	if (static_cast<int64_t>(index) < 0) {
		index = 0;
	}

	// Fill remaining indeces with null until element index
	for (size_t entries = yyjson_mut_arr_size(arr); entries < index; ++entries) {
		yyjson_mut_arr_add_null(doc, arr);
	}
	auto edoc = JSONCommon::ReadDocument(element, JSONCommon::READ_FLAG, alc);
	auto mut_edoc = yyjson_doc_mut_copy(edoc, alc);
	yyjson_mut_arr_insert(arr, mut_edoc->root, index);
	return arr;
}

//! Insert boolean value to an array
yyjson_mut_val *ArrayInsertBoolean(yyjson_mut_val *arr, yyjson_mut_doc *doc, bool element, int64_t idx, yyjson_alc *alc,
                                   Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not a JSON Array");
	}

	// Determine index
	auto array_size = yyjson_mut_arr_size(arr);
	size_t index = static_cast<size_t>(idx);

	// negative indexes address from the rear
	if (idx < 0) {
		index = static_cast<size_t>(static_cast<int64_t>(array_size) + idx + 1);
	}
	// In case of |idx| > array_size clamp to 0
	if (static_cast<int64_t>(index) < 0) {
		index = 0;
	}

	// Fill remaining indeces with null until element index
	for (size_t entries = array_size; entries < index; ++entries) {
		yyjson_mut_arr_add_null(doc, arr);
	}

	auto mut_value = yyjson_mut_bool(doc, element);
	yyjson_mut_arr_insert(arr, mut_value, index);
	return arr;
}

//! Insert unsigned Integers to an array
yyjson_mut_val *ArrayInsertUnsignedIntegers(yyjson_mut_val *arr, yyjson_mut_doc *doc, uint64_t element, int64_t idx,
                                            yyjson_alc *alc, Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not a JSON Array");
	}

	// Determine index
	auto array_size = yyjson_mut_arr_size(arr);
	size_t index = static_cast<size_t>(idx);

	// negative indexes address from the rear
	if (idx < 0) {
		index = static_cast<size_t>(static_cast<int64_t>(array_size) + idx + 1);
	}
	// In case of |idx| > array_size clamp to 0
	if (static_cast<int64_t>(index) < 0) {
		index = 0;
	}

	// Fill remaining indeces with null until element index
	for (size_t entries = array_size; entries < index; ++entries) {
		yyjson_mut_arr_add_null(doc, arr);
	}

	auto mut_value = yyjson_mut_uint(doc, element);
	yyjson_mut_arr_insert(arr, mut_value, index);
	return arr;
}

//! Insert signed Integers to an array
yyjson_mut_val *ArrayInsertSignedIntegers(yyjson_mut_val *arr, yyjson_mut_doc *doc, int64_t element, int64_t idx,
                                          yyjson_alc *alc, Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not a JSON Array");
	}

	// Determine index
	auto array_size = yyjson_mut_arr_size(arr);
	size_t index = static_cast<size_t>(idx);

	// negative indexes address from the rear
	if (idx < 0) {
		index = static_cast<size_t>(static_cast<int64_t>(array_size) + idx + 1);
	}
	// In case of |idx| > array_size clamp to 0
	if (static_cast<int64_t>(index) < 0) {
		index = 0;
	}

	// Fill remaining indeces with null until element index
	for (size_t entries = array_size; entries < index; ++entries) {
		yyjson_mut_arr_add_null(doc, arr);
	}

	// Insert Element add index
	auto mut_value = yyjson_mut_sint(doc, element);
	yyjson_mut_arr_insert(arr, mut_value, index);
	return arr;
}

//! Insert floating values to an array
yyjson_mut_val *ArrayInsertFloating(yyjson_mut_val *arr, yyjson_mut_doc *doc, double element, int64_t idx,
                                    yyjson_alc *alc, Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not a JSON Array");
	}

	// Determine index
	auto array_size = yyjson_mut_arr_size(arr);
	size_t index = static_cast<size_t>(idx);

	// negative indexes address from the rear
	if (idx < 0) {
		index = static_cast<size_t>(static_cast<int64_t>(array_size) + idx + 1);
	}
	// In case of |idx| > array_size clamp to 0
	if (static_cast<int64_t>(index) < 0) {
		index = 0;
	}

	// Fill remaining indeces with null until element index
	for (size_t entries = yyjson_mut_arr_size(arr); entries < index; ++entries) {
		yyjson_mut_arr_add_null(doc, arr);
	}
	auto mut_value = yyjson_mut_real(doc, element);
	yyjson_mut_arr_insert(arr, mut_value, index);
	return arr;
}

//! Insert function wrapper
static void ArrayInsertFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto json_type = args.data[0].GetType();
	D_ASSERT(json_type == LogicalType::VARCHAR || json_type == JSONCommon::JSONType());
	auto idx_type = args.data[2].GetType();
	D_ASSERT(idx_type == LogicalType::BIGINT);

	auto element_type = args.data[1].GetType();

	switch (element_type.id()) {
	case LogicalType::VARCHAR:
		JSONExecutors::TernaryMutExecute<string_t, int64_t>(args, state, result, ArrayInsertStringOrJSON);
		break;
	case LogicalType::BOOLEAN:
		JSONExecutors::TernaryMutExecute<bool, int64_t>(args, state, result, ArrayInsertBoolean);
		break;
	case LogicalType::UBIGINT:
		JSONExecutors::TernaryMutExecute<uint64_t, int64_t>(args, state, result, ArrayInsertUnsignedIntegers);
		break;
	case LogicalType::BIGINT:
		JSONExecutors::TernaryMutExecute<int64_t, int64_t>(args, state, result, ArrayInsertSignedIntegers);
		break;
	case LogicalType::DOUBLE:
		JSONExecutors::TernaryMutExecute<double, int64_t>(args, state, result, ArrayInsertFloating);
		break;
	default:
		// Shouldn't be thrown except implicit casting changes
		throw InvalidInputException("Not a valid input type");
	}
}

static void GetArrayInsertFunctionInternal(ScalarFunctionSet &set, const LogicalType &fst, const LogicalType &snd,
                                           const LogicalType &thrd) {
	set.AddFunction(ScalarFunction("json_array_insert", {fst, snd, thrd}, JSONCommon::JSONType(), ArrayInsertFunction,
	                               nullptr, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetArrayInsertFunction() {
	ScalarFunctionSet set("json_array_insert");

	// Use different executor for these
	// Allows booleans directly
	GetArrayInsertFunctionInternal(set, JSONCommon::JSONType(), LogicalType::BOOLEAN, LogicalType::BIGINT);

	// Allows for Integer types
	// TINYINT, SMALLINT, INTEGER, UTINYINT, USMALLINT, UINTEGER are captured by UBIGINT and BIGINT	respecively
	// relies on consistant casting strategy upfront

	// unsigned
	GetArrayInsertFunctionInternal(set, JSONCommon::JSONType(), LogicalType::UBIGINT, LogicalType::BIGINT);

	// signed
	GetArrayInsertFunctionInternal(set, JSONCommon::JSONType(), LogicalType::BIGINT, LogicalType::BIGINT);

	// Allows for floating types
	// FLOAT is covered by automatic upfront casting to double
	GetArrayInsertFunctionInternal(set, JSONCommon::JSONType(), LogicalType::DOUBLE, LogicalType::BIGINT);

	// Allows for json and string values
	GetArrayInsertFunctionInternal(set, JSONCommon::JSONType(), JSONCommon::JSONType(), LogicalType::BIGINT);
	GetArrayInsertFunctionInternal(set, JSONCommon::JSONType(), LogicalType::VARCHAR, LogicalType::BIGINT);

	return set;
}

} // namespace duckdb
