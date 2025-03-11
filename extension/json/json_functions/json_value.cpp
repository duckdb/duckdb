#include "json_executors.hpp"

namespace duckdb {

static inline string_t ValueFromVal(yyjson_val *val, yyjson_alc *alc, Vector &, ValidityMask &mask, idx_t idx) {
	switch (yyjson_get_tag(val)) {
	case YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE:
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
		mask.SetInvalid(idx);
		return string_t {};
	default:
		return JSONCommon::WriteVal<yyjson_val>(val, alc);
	}
}

static void ValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::BinaryExecute<string_t>(args, state, result, ValueFromVal);
}

static void ValueManyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::ExecuteMany<string_t>(args, state, result, ValueFromVal);
}

static void GetValueFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type, LogicalType::BIGINT}, LogicalType::VARCHAR, ValueFunction,
	                               JSONReadFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::VARCHAR, ValueFunction,
	                               JSONReadFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::VARCHAR), ValueManyFunction,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetValueFunction() {
	// The value function is just like the extract function but returns NULL if the JSON is not a scalar value
	ScalarFunctionSet set("json_value");
	GetValueFunctionsInternal(set, LogicalType::VARCHAR);
	GetValueFunctionsInternal(set, LogicalType::JSON());
	return set;
}

} // namespace duckdb
