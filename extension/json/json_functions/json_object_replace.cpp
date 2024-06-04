#include "json_executors.hpp"

namespace duckdb {

//! Replace String or JSON value of a key in JSON object
yyjson_mut_val *ObjectReplaceJSON(yyjson_mut_val *obj, string_t key, string_t element, yyjson_alc *alc,
                                  Vector &result) {
	if (!yyjson_mut_is_obj(obj)) {
		throw InvalidInputException("JSON input not an JSON Object");
	}

	const char *_key = key.GetDataWriteable();
	auto doc = JSONCommon::CreateDocument(alc);
	auto mut_key = yyjson_mut_strcpy(doc, _key);

	auto edoc = JSONCommon::ReadDocument(element, JSONCommon::READ_FLAG, alc);
	auto mut_edoc = yyjson_doc_mut_copy(edoc, alc);

	yyjson_mut_obj_replace(obj, mut_key, mut_edoc->root);
	return obj;
}

//! Replace key-value pairs to a json object
static void ObjectReplaceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto obj_type = args.data[0].GetType();
	D_ASSERT(obj_type == LogicalType::VARCHAR || obj_type == LogicalType::JSON());
	auto first_type = args.data[1].GetType();
	D_ASSERT(first_type == LogicalType::VARCHAR);
	auto second_type = args.data[2].GetType();
	D_ASSERT(second_type == LogicalType::VARCHAR || second_type == LogicalType::JSON());

	JSONExecutors::TernaryMutExecute<string_t, string_t>(args, state, result, ObjectReplaceJSON);
}

static void GetObjectReplaceFunctionInternal(ScalarFunctionSet &set, const LogicalType &obj, const LogicalType &first,
                                             const LogicalType &second) {
	set.AddFunction(ScalarFunction("json_obj_replace", {obj, first, second}, LogicalType::JSON(), ObjectReplaceFunction,
	                               nullptr, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetObjectReplaceFunction() {
	ScalarFunctionSet set("json_obj_replace");
	GetObjectReplaceFunctionInternal(set, LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::JSON());

	return set;
}

} // namespace duckdb
