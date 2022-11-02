#include "json_functions.hpp"

#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "json_common.hpp"

namespace duckdb {

vector<CreateScalarFunctionInfo> JSONFunctions::GetScalarFunctions() {
	vector<CreateScalarFunctionInfo> functions;

	// Extract functions
	AddAliases({"json_extract", "json_extract_path"}, GetExtractFunction(), functions);
	AddAliases({"json_extract_string", "json_extract_path_text", "->>"}, GetExtractStringFunction(), functions);

	// Create functions
	functions.push_back(GetArrayFunction());
	functions.push_back(GetObjectFunction());
	AddAliases({"to_json", "json_quote"}, GetToJSONFunction(), functions);
	functions.push_back(GetArrayToJSONFunction());
	functions.push_back(GetRowToJSONFunction());
	functions.push_back(GetMergePatchFunction());

	// Structure/Transform
	functions.push_back(GetStructureFunction());
	AddAliases({"json_transform", "from_json"}, GetTransformFunction(), functions);
	AddAliases({"json_transform_strict", "from_json_strict"}, GetTransformStrictFunction(), functions);

	// Other
	functions.push_back(GetArrayLengthFunction());
	functions.push_back(GetContainsFunction());
	functions.push_back(GetTypeFunction());
	functions.push_back(GetValidFunction());

	return functions;
}

vector<CreateTableFunctionInfo> JSONFunctions::GetTableFunctions() {
	vector<CreateTableFunctionInfo> functions;

	functions.push_back(GetReadJSONObjectsFunction());

	return functions;
}

static void RegisterCastFunction(CastFunctionSet &casts, GetCastFunctionInput input, const LogicalType &source,
                                 const LogicalType &target) {
	auto cost = casts.ImplicitCastCost(source.id(), target.id());
	auto func = casts.GetCastFunction(source.id(), target.id(), input);
	casts.RegisterCastFunction(source, target, DefaultCasts::ReinterpretCast, cost);
}

void JSONFunctions::RegisterCastFunctions(CastFunctionSet &casts, GetCastFunctionInput input) {
	RegisterCastFunction(casts, input, JSONCommon::JSONType(), LogicalType::VARCHAR);
	RegisterCastFunction(casts, input, LogicalType::VARCHAR, JSONCommon::JSONType());
}

} // namespace duckdb
