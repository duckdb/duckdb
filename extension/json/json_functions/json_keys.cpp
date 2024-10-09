#include "json_executors.hpp"

namespace duckdb {

static inline list_entry_t GetJSONKeys(yyjson_val *val, yyjson_alc *, Vector &result, ValidityMask &, idx_t) {
	auto num_keys = yyjson_obj_size(val);
	auto current_size = ListVector::GetListSize(result);
	auto new_size = current_size + num_keys;

	// Grow list if needed
	if (ListVector::GetListCapacity(result) < new_size) {
		ListVector::Reserve(result, new_size);
	}

	// Write the strings to the child vector
	auto keys = FlatVector::GetData<string_t>(ListVector::GetEntry(result));
	size_t idx, max;
	yyjson_val *key, *child_val;
	yyjson_obj_foreach(val, idx, max, key, child_val) {
		keys[current_size + idx] = string_t(unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key));
	}

	// Update size
	ListVector::SetListSize(result, current_size + num_keys);

	return {current_size, num_keys};
}

static void UnaryJSONKeysFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::UnaryExecute<list_entry_t>(args, state, result, GetJSONKeys);
}

static void BinaryJSONKeysFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::BinaryExecute<list_entry_t>(args, state, result, GetJSONKeys);
}

static void ManyJSONKeysFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::ExecuteMany<list_entry_t>(args, state, result, GetJSONKeys);
}

static void GetJSONKeysFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type}, LogicalType::LIST(LogicalType::VARCHAR), UnaryJSONKeysFunction,
	                               nullptr, nullptr, nullptr, JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::LIST(LogicalType::VARCHAR),
	                               BinaryJSONKeysFunction, JSONReadFunctionData::Bind, nullptr, nullptr,
	                               JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::LIST(LogicalType::VARCHAR)), ManyJSONKeysFunction,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetKeysFunction() {
	ScalarFunctionSet set("json_keys");
	GetJSONKeysFunctionsInternal(set, LogicalType::VARCHAR);
	GetJSONKeysFunctionsInternal(set, LogicalType::JSON());
	return set;
}

} // namespace duckdb
