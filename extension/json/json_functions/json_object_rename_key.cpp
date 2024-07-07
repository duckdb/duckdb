#include "json_executors.hpp"

namespace duckdb {

//! Rename all existing matching key into the new given key
yyjson_mut_val *ObjectRenameKey(yyjson_mut_val *obj, string_t key, string_t new_key, yyjson_alc *alc, Vector &result) {
	if (!yyjson_mut_is_obj(obj)) {
		throw InvalidInputException("JSON input not an JSON Object");
	}

	auto doc = JSONCommon::CreateDocument(alc);

	const char *_key = key.GetDataWriteable();
	auto mut_key = yyjson_mut_strcpy(doc, _key);
	auto k = yyjson_mut_get_str(mut_key);

	const char *_new_key = new_key.GetDataWriteable();
	auto mut_new_key = yyjson_mut_strcpy(doc, _new_key);
	auto new_k = yyjson_mut_get_str(mut_new_key);

	yyjson_mut_obj_rename_key(doc, obj, k, new_k);
	return obj;
}

//! Rename keys in object
static void ObjectRenameKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto obj_type = args.data[0].GetType();
	D_ASSERT(obj_type == LogicalType::VARCHAR || obj_type == LogicalType::JSON());
	auto first_type = args.data[1].GetType();
	D_ASSERT(first_type == LogicalType::VARCHAR);
	auto second_type = args.data[2].GetType();
	D_ASSERT(second_type == LogicalType::VARCHAR);

	JSONExecutors::TernaryMutExecute<string_t, string_t>(args, state, result, ObjectRenameKey);
}

static void GetObjectRenameKeyFunctionInternal(ScalarFunctionSet &set, const LogicalType &obj, const LogicalType &first,
                                               const LogicalType &second) {
	set.AddFunction(ScalarFunction("json_obj_rename_key", {obj, first, second}, LogicalType::JSON(),
	                               ObjectRenameKeyFunction, nullptr, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetObjectRenameKeyFunction() {
	ScalarFunctionSet set("json_obj_rename_key");

	GetObjectRenameKeyFunctionInternal(set, LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::VARCHAR);

	return set;
}

} // namespace duckdb
