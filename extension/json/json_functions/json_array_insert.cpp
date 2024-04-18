#include "json_executors.hpp"

namespace duckdb {

//! Insert String or JSON value to an array
yyjson_mut_val *ArrayInsertStringOrJSON(yyjson_mut_val *arr, yyjson_mut_doc *doc, string_t element, int64_t idx,
                                        yyjson_alc *alc, Vector &result) {
	if (!yyjson_mut_is_arr(arr)) {
		throw InvalidInputException("JSON input not an JSON Array");
	}

	size_t index = DetermineArrayIndex(arr, idx);

	// Fill remaining indeces with null until element index
	for (size_t entries = yyjson_mut_arr_size(arr); entries < index; ++entries) {
		yyjson_mut_arr_add_null(doc, arr);
	}

	auto edoc = JSONCommon::ReadDocument(element, JSONCommon::READ_FLAG, alc);
	auto mut_edoc = yyjson_doc_mut_copy(edoc, alc);
	yyjson_mut_arr_insert(arr, mut_edoc->root, index);
	return arr;
}

//! Insert any yyjson_mut_ELEMENT_TYPE type and function
template <class ELEMENT_TYPE>
std::function<yyjson_mut_val *(yyjson_mut_val *, yyjson_mut_doc *, ELEMENT_TYPE, int64_t, yyjson_alc *, Vector &)>
ArrayInsert(std::function<yyjson_mut_val *(yyjson_mut_doc *, ELEMENT_TYPE)> fconvert) {
	return [&](yyjson_mut_val *arr, yyjson_mut_doc *doc, ELEMENT_TYPE element, int64_t idx, yyjson_alc *alc,
	           Vector &result) {
		if (!yyjson_mut_is_arr(arr)) {
			throw InvalidInputException("JSON input not a JSON Array");
		}

		size_t index = DetermineArrayIndex(arr, idx);

		// Fill remaining indeces with null until element index
		for (size_t entries = yyjson_mut_arr_size(arr); entries < index; ++entries) {
			yyjson_mut_arr_add_null(doc, arr);
		}
		auto mut_value = fconvert(doc, element);
		yyjson_mut_arr_insert(arr, mut_value, index);
		return arr;
	};
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
		JSONExecutors::TernaryMutExecute<bool, int64_t>(args, state, result, ArrayInsert<bool>(yyjson_mut_bool));
		break;
	case LogicalType::UBIGINT:
		JSONExecutors::TernaryMutExecute<uint64_t, int64_t>(args, state, result,
		                                                    ArrayInsert<uint64_t>(yyjson_mut_uint));
		break;
	case LogicalType::BIGINT:
		JSONExecutors::TernaryMutExecute<int64_t, int64_t>(args, state, result, ArrayInsert<int64_t>(yyjson_mut_sint));
		break;
	case LogicalType::DOUBLE:
		JSONExecutors::TernaryMutExecute<double, int64_t>(args, state, result, ArrayInsert<double>(yyjson_mut_real));
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
