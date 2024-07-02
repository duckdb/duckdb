#include "json_executors.hpp"

namespace duckdb {

//! Add a JSON Object or String to an object
yyjson_mut_val *ObjectAddJSON(yyjson_mut_val *obj, string_t key, string_t value, yyjson_alc *alc, Vector &result) {
	if (!yyjson_mut_is_obj(obj)) {
		throw InvalidInputException("JSON input not an JSON Object");
	}

	auto doc = JSONCommon::CreateDocument(alc);

	const char *_key = key.GetDataWriteable();
	auto mut_key = yyjson_mut_strcpy(doc, _key);

	auto vdoc = JSONCommon::ReadDocument(value, JSONCommon::READ_FLAG, alc);
	auto mut_vdoc = yyjson_doc_mut_copy(vdoc, alc);

	yyjson_mut_obj_add(obj, mut_key, mut_vdoc->root);
	return obj;
}

//! Add key-value pairs to a json object
static void ObjectAddFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto obj_type = args.data[0].GetType();
	D_ASSERT(obj_type == LogicalType::VARCHAR || obj_type == LogicalType::JSON());
	auto first_type = args.data[1].GetType();
	D_ASSERT(first_type == LogicalType::VARCHAR);
	auto second_type = args.data[2].GetType();
	D_ASSERT(second_type == LogicalType::JSON());

	JSONExecutors::TernaryMutExecute<string_t, string_t>(args, state, result, ObjectAddJSON);
}

static void GetObjectAddFunctionInternal(ScalarFunctionSet &set, const LogicalType &obj, const LogicalType &first,
                                         const LogicalType &second) {
	set.AddFunction(ScalarFunction("json_obj_add", {obj, first, second}, LogicalType::JSON(), ObjectAddFunction,
	                               nullptr, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetObjectAddFunction() {
	ScalarFunctionSet set("json_obj_add");
	GetObjectAddFunctionInternal(set, LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::JSON());

	return set;
}

} // namespace duckdb
