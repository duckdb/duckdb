#include "json_executors.hpp"

namespace duckdb {

//! Replace String or JSON value of a key in JSON object
yyjson_mut_val *ObjectReplaceStringOrJSON(yyjson_mut_val *obj, yyjson_mut_doc *doc, string_t key, string_t element,
                                          yyjson_alc *alc, Vector &result) {
	if (!yyjson_mut_is_obj(obj)) {
		throw InvalidInputException("JSON input not an JSON Object");
	}

	const char *_key = key.GetDataWriteable();
	auto mut_key = yyjson_mut_strcpy(doc, _key);

	auto edoc = JSONCommon::ReadDocument(element, JSONCommon::READ_FLAG, alc);
	auto mut_edoc = yyjson_doc_mut_copy(edoc, alc);

	yyjson_mut_obj_replace(obj, mut_key, mut_edoc->root);
	return obj;
}

//! Replace any yyjson_mut_ELEMENT_TYPE and function
template <class ELEMENT_TYPE>
std::function<yyjson_mut_val *(yyjson_mut_val *, yyjson_mut_doc *, string_t, ELEMENT_TYPE, yyjson_alc *,
                               Vector &result)>
ObjectReplace(std::function<yyjson_mut_val *(yyjson_mut_doc *, ELEMENT_TYPE)> fconvert) {
	return [&](yyjson_mut_val *obj, yyjson_mut_doc *doc, string_t key, ELEMENT_TYPE value, yyjson_alc *alc,
	           Vector &result) {
		if (!yyjson_mut_is_obj(obj)) {
			throw InvalidInputException("JSON input not an JSON Object");
		}

		const char *_key = key.GetDataWriteable();
		auto mut_key = yyjson_mut_strcpy(doc, _key);

		auto mut_value = fconvert(doc, value);

		yyjson_mut_obj_replace(obj, mut_key, mut_value);
		return obj;
	};
}

//! Replace key-value pairs to a json object
static void ObjectReplaceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto obj_type = args.data[0].GetType();
	D_ASSERT(obj_type == LogicalType::VARCHAR || obj_type == LogicalType::JSON());
	auto first_type = args.data[1].GetType();
	D_ASSERT(first_type == LogicalType::VARCHAR);

	auto second_type = args.data[2].GetType();

	switch (second_type.id()) {
	case LogicalType::VARCHAR:
		JSONExecutors::TernaryMutExecute<string_t, string_t>(args, state, result, ObjectReplaceStringOrJSON);
		break;
	case LogicalType::BOOLEAN:
		JSONExecutors::TernaryMutExecute<string_t, bool>(args, state, result, ObjectReplace<bool>(yyjson_mut_bool));
		break;
	case LogicalType::UBIGINT:
		JSONExecutors::TernaryMutExecute<string_t, uint64_t>(args, state, result,
		                                                     ObjectReplace<uint64_t>(yyjson_mut_uint));
		break;
	case LogicalType::BIGINT:
		JSONExecutors::TernaryMutExecute<string_t, int64_t>(args, state, result,
		                                                    ObjectReplace<int64_t>(yyjson_mut_sint));
		break;
	case LogicalType::DOUBLE:
		JSONExecutors::TernaryMutExecute<string_t, double>(args, state, result, ObjectReplace<double>(yyjson_mut_real));
		break;
	default:
		// Shouldn't be thrown except implicit casting changes
		throw InvalidInputException("Not a valid input type");
	}
}

static void GetObjectReplaceFunctionInternal(ScalarFunctionSet &set, const LogicalType &obj, const LogicalType &first,
                                             const LogicalType &second) {
	set.AddFunction(ScalarFunction("json_obj_replace", {obj, first, second}, LogicalType::JSON(),
	                               ObjectReplaceFunction, nullptr, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetObjectReplaceFunction() {
	ScalarFunctionSet set("json_obj_replace");

	// Use different executor for these

	// Boolean
	// GetObjectReplaceFunctionInternal(set, LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::BOOLEAN);

	// Integer Types

	// unsigned
	// GetObjectReplaceFunctionInternal(set, LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::UBIGINT);

	// signed
	// GetObjectReplaceFunctionInternal(set, LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::BIGINT);

	// Floating Types
	// GetObjectReplaceFunctionInternal(set, LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::DOUBLE);

	// JSON values
	GetObjectReplaceFunctionInternal(set, LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::JSON());
	// GetObjectReplaceFunctionInternal(set, LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::VARCHAR);

	return set;
}

} // namespace duckdb
