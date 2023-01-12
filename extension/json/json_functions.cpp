#include "json_functions.hpp"

#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "json_common.hpp"
#include "json_scan.hpp"

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

	// Reads JSON as string
	functions.push_back(GetReadJSONObjectsFunction());
	functions.push_back(GetReadNDJSONObjectsFunction());

	return functions;
}

static bool CastVarcharToJSON(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	bool success = true;
	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
	    source, result, count, [&](string_t input, ValidityMask &mask, idx_t idx) {
		    auto data = (char *)(input.GetDataUnsafe());
		    auto length = input.GetSize();
		    yyjson_read_err error;

		    // We use YYJSON_INSITU to speed up the cast, then we restore the input string
		    auto doc = JSONCommon::ReadDocumentFromFileStop(data, length, &error);
		    JSONScan::RestoreParsedString(data, length);

		    if (doc.IsNull()) {
			    HandleCastError::AssignError(JSONCommon::FormatParseError(data, length, error),
			                                 parameters.error_message);
			    mask.SetInvalid(idx);
			    success = false;
		    }

		    return input;
	    });
	return success;
}

void JSONFunctions::RegisterCastFunctions(CastFunctionSet &casts) {
	// JSON to VARCHAR is free
	casts.RegisterCastFunction(JSONCommon::JSONType(), LogicalType::VARCHAR, DefaultCasts::ReinterpretCast, 0);
	// VARCHAR to JSON requires a parse so it's not free. Let's make it 1 more than VARCHAR to STRUCT
	auto varchar_to_json_cost = casts.ImplicitCastCost(LogicalTypeId::VARCHAR, LogicalTypeId::STRUCT) + 1;
	casts.RegisterCastFunction(LogicalType::VARCHAR, JSONCommon::JSONType(), CastVarcharToJSON, varchar_to_json_cost);
}

} // namespace duckdb
