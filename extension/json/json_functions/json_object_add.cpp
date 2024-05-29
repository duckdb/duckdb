#include "json_executors.hpp"

namespace duckdb {

//! Add a JSON Object or String to an object
yyjson_mut_val *ObjectAddStringOrJSON(yyjson_mut_val *obj, yyjson_mut_doc *doc, string_t key, string_t value,
                                      yyjson_alc *alc, Vector &result) {
	if (!yyjson_mut_is_obj(obj)) {
		throw InvalidInputException("JSON input not an JSON Object");
	}

	const char *_key = key.GetDataWriteable();
	auto mut_key = yyjson_mut_strcpy(doc, _key);

	auto vdoc = JSONCommon::ReadDocument(value, JSONCommon::READ_FLAG, alc);
	auto mut_vdoc = yyjson_doc_mut_copy(vdoc, alc);

	yyjson_mut_obj_add(obj, mut_key, mut_vdoc->root);
	return obj;
}

//! Add any yyjson_mut_ELEMENT_TYPE type and function
template <class ELEMENT_TYPE>
std::function<yyjson_mut_val *(yyjson_mut_val *, yyjson_mut_doc *, string_t, ELEMENT_TYPE, yyjson_alc *, Vector &)>
ObjectAdd(std::function<yyjson_mut_val *(yyjson_mut_doc *, ELEMENT_TYPE)> fconvert) {
	return [&](yyjson_mut_val *obj, yyjson_mut_doc *doc, string_t key, ELEMENT_TYPE element, yyjson_alc *alc,
	           Vector &result) {
		if (!yyjson_mut_is_obj(obj)) {
			throw InvalidInputException("JSON input not an JSON Object");
		}

		const char *_key = key.GetDataWriteable();
		auto mut_key = yyjson_mut_strcpy(doc, _key);
		auto k = yyjson_mut_get_str(mut_key);

		auto mut_value = fconvert(doc, element);
		yyjson_mut_obj_add_val(doc, obj, k, mut_value);
		return obj;
	};
}

//! Add key-value pairs to a json object
static void ObjectAddFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto obj_type = args.data[0].GetType();
	D_ASSERT(obj_type == LogicalType::VARCHAR || obj_type == LogicalType::JSON());
	auto first_type = args.data[1].GetType();
	D_ASSERT(first_type == LogicalType::VARCHAR);

	auto second_type = args.data[2].GetType();

	switch (second_type.id()) {
	case LogicalType::VARCHAR:
		JSONExecutors::TernaryMutExecute<string_t, string_t>(args, state, result, ObjectAddStringOrJSON);
		break;
	case LogicalType::BOOLEAN:
		JSONExecutors::TernaryMutExecute<string_t, bool>(args, state, result, ObjectAdd<bool>(yyjson_mut_bool));
		break;
	case LogicalType::UBIGINT:
		JSONExecutors::TernaryMutExecute<string_t, uint64_t>(args, state, result,
															ObjectAdd<uint64_t>(yyjson_mut_uint));
		break;
	case LogicalType::BIGINT:
		JSONExecutors::TernaryMutExecute<string_t, int64_t>(args, state, result, ObjectAdd<int64_t>(yyjson_mut_sint));
		break;
	case LogicalType::DOUBLE:
		JSONExecutors::TernaryMutExecute<string_t, double>(args, state, result, ObjectAdd<double>(yyjson_mut_real));
		break;
	default:
		// Shouldn't be thrown except implicit casting changes
		throw InvalidInputException("Not a valid input type");
	}
}

static void GetObjectAddFunctionInternal(ScalarFunctionSet &set, const LogicalType &obj, const LogicalType &first,
                                         const LogicalType &second) {
	set.AddFunction(ScalarFunction("json_obj_add", {obj, first, second}, LogicalType::JSON(), ObjectAddFunction,
	                               nullptr, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetObjectAddFunction() {
	ScalarFunctionSet set("json_obj_add");

	// Use different executor for these

	// Boolean
	// GetObjectAddFunctionInternal(set, LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::BOOLEAN);

	// Integer Types

	// unsigned
	// GetObjectAddFunctionInternal(set, LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::UBIGINT);

	// signed
	// GetObjectAddFunctionInternal(set, LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::BIGINT);

	// Floating Types
	// GetObjectAddFunctionInternal(set, LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::DOUBLE);

	// JSON values
	GetObjectAddFunctionInternal(set, LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::JSON());
	// GetObjectAddFunctionInternal(set, LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::VARCHAR);

	return set;
}

} // namespace duckdb
