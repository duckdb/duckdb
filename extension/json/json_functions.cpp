#include "json_functions.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

static void CheckPath(const Value &path_val, string &path, size_t &len) {
	string error;
	Value path_str_val;
	if (!path_val.DefaultTryCastAs(LogicalType::VARCHAR, path_str_val, &error)) {
		throw InvalidInputException(error);
	}
	auto path_str = path_str_val.GetValueUnsafe<string_t>();
	len = path_str.GetSize();
	auto ptr = path_str.GetDataUnsafe();
	// Empty strings and invalid $ paths yield an error
	if (len == 0) {
		throw InvalidInputException("Empty JSON path");
	}
	if (*ptr == '$') {
		JSONCommon::ValidatePathDollar(ptr, len);
	}
	// Copy over string to the bind data
	if (*ptr == '/' || *ptr == '$') {
		path = string(ptr, len);
	} else {
		path = "/" + string(ptr, len);
		len++;
	}
}

JSONReadFunctionData::JSONReadFunctionData(bool constant, string path_p, idx_t len)
    : constant(constant), path(std::move(path_p)), ptr(path.c_str()), len(len) {
}

unique_ptr<FunctionData> JSONReadFunctionData::Copy() const {
	return make_unique<JSONReadFunctionData>(constant, path, len);
}

bool JSONReadFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = (const JSONReadFunctionData &)other_p;
	return constant == other.constant && path == other.path && len == other.len;
}

unique_ptr<FunctionData> JSONReadFunctionData::Bind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	bool constant = false;
	string path = "";
	size_t len = 0;
	if (arguments[1]->return_type.id() != LogicalTypeId::SQLNULL && arguments[1]->IsFoldable()) {
		constant = true;
		const auto path_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
		CheckPath(path_val, path, len);
	}
	return make_unique<JSONReadFunctionData>(constant, std::move(path), len);
}

JSONReadManyFunctionData::JSONReadManyFunctionData(vector<string> paths_p, vector<size_t> lens_p)
    : paths(std::move(paths_p)), lens(std::move(lens_p)) {
	for (const auto &path : paths) {
		ptrs.push_back(path.c_str());
	}
}

unique_ptr<FunctionData> JSONReadManyFunctionData::Copy() const {
	return make_unique<JSONReadManyFunctionData>(paths, lens);
}

bool JSONReadManyFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = (const JSONReadManyFunctionData &)other_p;
	return paths == other.paths && lens == other.lens;
}

unique_ptr<FunctionData> JSONReadManyFunctionData::Bind(ClientContext &context, ScalarFunction &bound_function,
                                                        vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw InvalidInputException("List of paths must be constant");
	}
	if (arguments[1]->return_type.id() == LogicalTypeId::SQLNULL) {
		return make_unique<JSONReadManyFunctionData>(vector<string>(), vector<size_t>());
	}

	vector<string> paths;
	vector<size_t> lens;
	auto paths_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	for (auto &path_val : ListValue::GetChildren(paths_val)) {
		paths.emplace_back("");
		lens.push_back(0);
		CheckPath(path_val, paths.back(), lens.back());
	}

	return make_unique<JSONReadManyFunctionData>(std::move(paths), std::move(lens));
}

JSONFunctionLocalState::JSONFunctionLocalState(Allocator &allocator) : json_allocator(allocator) {
}
JSONFunctionLocalState::JSONFunctionLocalState(ClientContext &context)
    : JSONFunctionLocalState(BufferAllocator::Get(context)) {
}

unique_ptr<FunctionLocalState> JSONFunctionLocalState::Init(ExpressionState &state, const BoundFunctionExpression &expr,
                                                            FunctionData *bind_data) {
	return make_unique<JSONFunctionLocalState>(state.GetContext());
}

JSONFunctionLocalState &JSONFunctionLocalState::ResetAndGet(ExpressionState &state) {
	auto &lstate = (JSONFunctionLocalState &)*ExecuteFunctionState::GetFunctionState(state);
	lstate.json_allocator.Reset();
	return lstate;
}

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
	functions.push_back(GetKeysFunction());
	functions.push_back(GetTypeFunction());
	functions.push_back(GetValidFunction());

	return functions;
}

vector<CreateTableFunctionInfo> JSONFunctions::GetTableFunctions() {
	vector<CreateTableFunctionInfo> functions;

	// Reads JSON as string
	functions.push_back(GetReadJSONObjectsFunction());
	functions.push_back(GetReadNDJSONObjectsFunction());

	// Read JSON as columnar data
	functions.push_back(GetReadJSONFunction());
	functions.push_back(GetReadNDJSONFunction());
	functions.push_back(GetReadJSONAutoFunction());
	functions.push_back(GetReadNDJSONAutoFunction());

	return functions;
}

unique_ptr<TableRef> JSONFunctions::ReadJSONReplacement(ClientContext &context, const string &table_name,
                                                        ReplacementScanData *data) {
	auto lower_name = StringUtil::Lower(table_name);
	// remove any compression
	if (StringUtil::EndsWith(lower_name, ".gz")) {
		lower_name = lower_name.substr(0, lower_name.size() - 3);
	} else if (StringUtil::EndsWith(lower_name, ".zst")) {
		lower_name = lower_name.substr(0, lower_name.size() - 4);
	}
	if (!StringUtil::EndsWith(lower_name, ".json") && !StringUtil::Contains(lower_name, ".json?") &&
	    !StringUtil::EndsWith(lower_name, ".jsonl") && !StringUtil::Contains(lower_name, ".jsonl?") &&
	    !StringUtil::EndsWith(lower_name, ".ndjson") && !StringUtil::Contains(lower_name, ".ndjson?")) {
		return nullptr;
	}
	auto table_function = make_unique<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_unique<ConstantExpression>(Value(table_name)));
	table_function->function = make_unique<FunctionExpression>("read_json_auto", std::move(children));
	return std::move(table_function);
}

static unique_ptr<FunctionLocalState> InitJSONCastLocalState(CastLocalStateParameters &parameters) {
	if (parameters.context) {
		return make_unique<JSONFunctionLocalState>(*parameters.context);
	} else {
		return make_unique<JSONFunctionLocalState>(Allocator::DefaultAllocator());
	}
}

static bool CastVarcharToJSON(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &lstate = (JSONFunctionLocalState &)*parameters.local_state;
	lstate.json_allocator.Reset();
	auto alc = lstate.json_allocator.GetYYJSONAllocator();

	bool success = true;
	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
	    source, result, count, [&](string_t input, ValidityMask &mask, idx_t idx) {
		    auto data = (char *)(input.GetDataUnsafe());
		    auto length = input.GetSize();
		    yyjson_read_err error;

		    auto doc = JSONCommon::ReadDocumentUnsafe(data, length, JSONCommon::READ_FLAG, alc, &error);

		    if (!doc) {
			    HandleCastError::AssignError(JSONCommon::FormatParseError(data, length, error),
			                                 parameters.error_message);
			    mask.SetInvalid(idx);
			    success = false;
		    }
		    return input;
	    });
	result.Reinterpret(source);
	return success;
}

void JSONFunctions::RegisterCastFunctions(CastFunctionSet &casts) {
	// JSON to VARCHAR is basically free
	casts.RegisterCastFunction(JSONCommon::JSONType(), LogicalType::VARCHAR, DefaultCasts::ReinterpretCast, 1);

	// VARCHAR to JSON requires a parse so it's not free. Let's make it 1 more than a cast to STRUCT
	auto varchar_to_json_cost = casts.ImplicitCastCost(LogicalType::SQLNULL, LogicalTypeId::STRUCT) + 1;
	BoundCastInfo info(CastVarcharToJSON, nullptr, InitJSONCastLocalState);
	casts.RegisterCastFunction(LogicalType::VARCHAR, JSONCommon::JSONType(), std::move(info), varchar_to_json_cost);

	// Register NULL to JSON with a different cost than NULL to VARCHAR so the binder can disambiguate functions
	auto null_to_json_cost = casts.ImplicitCastCost(LogicalType::SQLNULL, LogicalTypeId::VARCHAR) + 1;
	casts.RegisterCastFunction(LogicalType::SQLNULL, JSONCommon::JSONType(), DefaultCasts::ReinterpretCast,
	                           null_to_json_cost);
}

} // namespace duckdb
