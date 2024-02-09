#include "json_executors.hpp"

namespace duckdb {


static void ArrayAppendFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto left_type = args.data[0].GetType();
	D_ASSERT(left_type == LogicalType::VARCHAR || left_type == JSONCommon::JSONType());

    auto right_type = args.data[1].GetType();

	// String or JSON value
	if (right_type == LogicalType::VARCHAR || right_type == JSONCommon::JSONType()) {
		JSONExecutors::BinaryMutExecute<string_t>(
		    args, state, result,
		    [](yyjson_mut_val *arr, yyjson_mut_doc *doc, string_t element, yyjson_alc *alc, Vector &result) {
			D_ASSERT(yyjson_mut_is_arr(arr));

            auto edoc = JSONCommon::ReadDocument(element, JSONCommon::READ_FLAG, alc);
            auto mut_edoc = yyjson_doc_mut_copy(edoc, alc);

			yyjson_mut_arr_append(arr, mut_edoc->root);

			return arr;
		});
    // Boolean
    } else if (right_type == LogicalType::BOOLEAN) {
		JSONExecutors::BinaryMutExecute<bool>(
		    args, state, result,
		    [&](yyjson_mut_val *arr, yyjson_mut_doc *doc, bool element, yyjson_alc *alc, Vector &result) {
			    D_ASSERT(yyjson_mut_is_arr(arr));

			    yyjson_mut_arr_add_bool(doc, arr, element);
			    return arr;
		    });
    // Integer value
   } else if (right_type == LogicalType::BIGINT || right_type == LogicalType::UTINYINT ||
	          right_type == LogicalType::USMALLINT || right_type == LogicalType::UINTEGER ||
	          right_type == LogicalType::UBIGINT) {
        JSONExecutors::BinaryMutExecute<int64_t>(
            args, state, result,
            [&](yyjson_mut_val *arr, yyjson_mut_doc *doc, int64_t el, yyjson_alc *alc, Vector &result) {
                D_ASSERT(yyjson_mut_is_arr(arr));

                int64_t element = static_cast<int64_t>(el);

                yyjson_mut_arr_add_int(doc, arr, element);
                return arr;
            });
    // Floating value
    } else if (right_type == LogicalType::FLOAT || right_type == LogicalType::DOUBLE) {
        JSONExecutors::BinaryMutExecute<double>(
		    args, state, result,
		    [&](yyjson_mut_val *arr, yyjson_mut_doc *doc, double el, yyjson_alc *alc, Vector &result) {
			    D_ASSERT(yyjson_mut_is_arr(arr));

			    double element = static_cast<double>(el);

			    yyjson_mut_arr_add_real(doc, arr, element);
			    return arr;
		    });
    }
}

static void GetArrayAppendFunctionInternal(ScalarFunctionSet &set, const LogicalType &lhs, const LogicalType &rhs) {
	set.AddFunction(ScalarFunction("json_array_append", {lhs, rhs}, JSONCommon::JSONType(), ArrayAppendFunction,
	                               nullptr, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetArrayAppendFunction() {
	ScalarFunctionSet  set("json_array_append");

	// Use different executor for these
	// Allows booleans directly
    GetArrayAppendFunctionInternal(set, LogicalType::VARCHAR, LogicalType::BOOLEAN);
    GetArrayAppendFunctionInternal(set, JSONCommon::JSONType(), LogicalType::BOOLEAN);

	// Allows for Integer types
	// TINYINT, SMALLINT, INTEGER, UTINYINT, USMALLINT are captured by BIGINT
	// relies on consistant casting strategy upfront
    GetArrayAppendFunctionInternal(set, LogicalType::VARCHAR, LogicalType::BIGINT);
    GetArrayAppendFunctionInternal(set, JSONCommon::JSONType(), LogicalType::BIGINT);
    GetArrayAppendFunctionInternal(set, LogicalType::VARCHAR, LogicalType::UINTEGER);
    GetArrayAppendFunctionInternal(set, JSONCommon::JSONType(), LogicalType::UINTEGER);
    GetArrayAppendFunctionInternal(set, LogicalType::VARCHAR, LogicalType::UBIGINT);
    GetArrayAppendFunctionInternal(set, JSONCommon::JSONType(), LogicalType::UBIGINT);

    // Allows for floating types
    // FLOAT is covered by automatic upfront casting to double
    GetArrayAppendFunctionInternal(set, LogicalType::VARCHAR, LogicalType::DOUBLE);
    GetArrayAppendFunctionInternal(set, JSONCommon::JSONType(), LogicalType::DOUBLE);

	// Allows for json and string values
	GetArrayAppendFunctionInternal(set, LogicalType::VARCHAR, LogicalType::VARCHAR);
	GetArrayAppendFunctionInternal(set, LogicalType::VARCHAR, JSONCommon::JSONType());
	GetArrayAppendFunctionInternal(set, JSONCommon::JSONType(), JSONCommon::JSONType());
	GetArrayAppendFunctionInternal(set, JSONCommon::JSONType(), LogicalType::VARCHAR);

	return set;
}

}
