#include "json_executors.hpp"

namespace duckdb {

static inline string_t ExtractFromVal(yyjson_val *val, yyjson_alc *alc, Vector &, ValidityMask &, idx_t) {
	return JSONCommon::WriteVal<yyjson_val>(val, alc);
}

static inline string_t ExtractStringFromVal(yyjson_val *val, yyjson_alc *alc, Vector &, ValidityMask &mask, idx_t idx) {
	switch (yyjson_get_tag(val)) {
	case YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE:
		mask.SetInvalid(idx);
		return string_t {};
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NOESC:
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
		return string_t(unsafe_yyjson_get_str(val), unsafe_yyjson_get_len(val));
	default:
		return JSONCommon::WriteVal<yyjson_val>(val, alc);
	}
}

static void ExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::BinaryExecute<string_t>(args, state, result, ExtractFromVal);
}

static void ExtractManyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::ExecuteMany<string_t>(args, state, result, ExtractFromVal);
}

static void ExtractStringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::BinaryExecute<string_t>(args, state, result, ExtractStringFromVal);
}

static void ExtractStringManyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::ExecuteMany<string_t>(args, state, result, ExtractStringFromVal);
}

static void GetExtractFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type, LogicalType::BIGINT}, LogicalType::JSON(), ExtractFunction,
	                               JSONReadFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::JSON(), ExtractFunction,
	                               JSONReadFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::JSON()), ExtractManyFunction,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetExtractFunction() {
	// Generic extract function
	ScalarFunctionSet set("json_extract");
	GetExtractFunctionsInternal(set, LogicalType::VARCHAR);
	GetExtractFunctionsInternal(set, LogicalType::JSON());
	return set;
}

static void GetExtractStringFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type, LogicalType::BIGINT}, LogicalType::VARCHAR, ExtractStringFunction,
	                               JSONReadFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::VARCHAR, ExtractStringFunction,
	                               JSONReadFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::VARCHAR), ExtractStringManyFunction,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetExtractStringFunction() {
	// String extract function
	ScalarFunctionSet set("json_extract_string");
	GetExtractStringFunctionsInternal(set, LogicalType::VARCHAR);
	GetExtractStringFunctionsInternal(set, LogicalType::JSON());
	return set;
}

} // namespace duckdb
