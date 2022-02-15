#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

template <class T>
static void TemplatedTypedExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::BinaryJSONReadFunction<T>(args, state, result, JSONCommon::TemplatedGetValue<T>);
}

template <class T>
static void TemplatedTypedExtractManyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::JSONReadManyFunction<T>(args, state, result, JSONCommon::TemplatedGetValue<T>);
}

static inline bool ExtractFromVal(yyjson_val *val, string_t &result) {
	if (val) {
		result = JSONCommon::WriteVal(val);
	}
	return val;
}

static void ExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::BinaryJSONReadFunction<string_t>(args, state, result, ExtractFromVal);
}

static void ExtractManyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::JSONReadManyFunction<string_t>(args, state, result, ExtractFromVal);
}

template <class T>
static void AddFunction(vector<CreateScalarFunctionInfo> &functions, vector<string> names, LogicalType return_type) {
	ScalarFunctionSet set("");
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, return_type,
	                               TemplatedTypedExtractFunction<T>, false, JSONReadFunctionData::Bind, nullptr,
	                               nullptr));
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(return_type), TemplatedTypedExtractManyFunction<T>, false,
	                               JSONReadFunctionData::Bind, nullptr, nullptr));
	for (const auto &name : names) {
		set.name = name;
		functions.push_back(CreateScalarFunctionInfo(set));
	}
}

vector<CreateScalarFunctionInfo> JSONFunctions::GetExtractFunctions() {
	vector<CreateScalarFunctionInfo> functions;

	// Typed extract functions
	AddFunction<bool>(functions, {"json_extract_bool", "json_extract_boolean"}, LogicalType::BOOLEAN);
	AddFunction<int32_t>(functions, {"json_extract_int", "json_extract_integer"}, LogicalType::INTEGER);
	AddFunction<int64_t>(functions, {"json_extract_bigint"}, LogicalType::BIGINT);
	AddFunction<uint64_t>(functions, {"json_extract_ubigint"}, LogicalType::UBIGINT);
	AddFunction<double>(functions, {"json_extract_double"}, LogicalType::DOUBLE);
	AddFunction<string_t>(functions, {"json_extract_string", "json_extract_varchar"}, LogicalType::VARCHAR);

	// Generic extract function
	ScalarFunctionSet set("json_extract");
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, LogicalType::JSON, ExtractFunction, false,
	                               JSONReadFunctionData::Bind, nullptr, nullptr));
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::JSON), ExtractManyFunction, false,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr));
	functions.push_back(CreateScalarFunctionInfo(set));

	return functions;
}

} // namespace duckdb
