#include "json_functions.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

using JSONPathType = JSONCommon::JSONPathType;

static JSONPathType CheckPath(const Value &path_val, string &path, size_t &len) {
	if (path_val.IsNull()) {
		throw BinderException("JSON path cannot be NULL");
	}
	const auto path_str_val = path_val.DefaultCastAs(LogicalType::VARCHAR);
	auto path_str = path_str_val.GetValueUnsafe<string_t>();
	len = path_str.GetSize();
	const auto ptr = path_str.GetData();
	JSONPathType path_type = JSONPathType::REGULAR;
	// Copy over string to the bind data
	if (len != 0) {
		if (*ptr == '/' || *ptr == '$') {
			path = string(ptr, len);
		} else if (path_val.type().IsIntegral()) {
			path = "$[" + string(ptr, len) + "]";
		} else if (memchr(ptr, '"', len)) {
			path = "/" + string(ptr, len);
		} else {
			path = "$.\"" + string(ptr, len) + "\"";
		}
		len = path.length();
		if (*path.c_str() == '$') {
			path_type = JSONCommon::ValidatePath(path.c_str(), len, true);
		}
	}
	return path_type;
}

JSONReadFunctionData::JSONReadFunctionData(bool constant, string path_p, idx_t len, JSONPathType path_type_p)
    : constant(constant), path(std::move(path_p)), path_type(path_type_p), ptr(path.c_str()), len(len) {
}

unique_ptr<FunctionData> JSONReadFunctionData::Copy() const {
	return make_uniq<JSONReadFunctionData>(constant, path, len, path_type);
}

bool JSONReadFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<JSONReadFunctionData>();
	return constant == other.constant && path == other.path && len == other.len && path_type == other.path_type;
}

unique_ptr<FunctionData> JSONReadFunctionData::Bind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	bool constant = false;
	string path;
	size_t len = 0;
	JSONPathType path_type = JSONPathType::REGULAR;
	if (arguments[1]->IsFoldable()) {
		const auto path_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
		if (!path_val.IsNull()) {
			constant = true;
			path_type = CheckPath(path_val, path, len);
		}
	}
	if (arguments[1]->return_type.IsIntegral()) {
		bound_function.arguments[1] = LogicalType::BIGINT;
	} else {
		bound_function.arguments[1] = LogicalType::VARCHAR;
	}
	if (path_type == JSONCommon::JSONPathType::WILDCARD) {
		bound_function.return_type = LogicalType::LIST(bound_function.return_type);
	}
	return make_uniq<JSONReadFunctionData>(constant, std::move(path), len, path_type);
}

JSONReadManyFunctionData::JSONReadManyFunctionData(vector<string> paths_p, vector<size_t> lens_p)
    : paths(std::move(paths_p)), lens(std::move(lens_p)) {
	for (const auto &path : paths) {
		ptrs.push_back(path.c_str());
	}
}

unique_ptr<FunctionData> JSONReadManyFunctionData::Copy() const {
	return make_uniq<JSONReadManyFunctionData>(paths, lens);
}

bool JSONReadManyFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<JSONReadManyFunctionData>();
	return paths == other.paths && lens == other.lens;
}

unique_ptr<FunctionData> JSONReadManyFunctionData::Bind(ClientContext &context, ScalarFunction &bound_function,
                                                        vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("List of paths must be constant");
	}

	vector<string> paths;
	vector<size_t> lens;
	auto paths_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);

	for (auto &path_val : ListValue::GetChildren(paths_val)) {
		paths.emplace_back("");
		lens.push_back(0);
		if (CheckPath(path_val, paths.back(), lens.back()) == JSONPathType::WILDCARD) {
			throw BinderException("Cannot have wildcards in JSON path when supplying multiple paths");
		}
	}

	return make_uniq<JSONReadManyFunctionData>(std::move(paths), std::move(lens));
}

JSONFunctionLocalState::JSONFunctionLocalState(Allocator &allocator) : json_allocator(allocator) {
}

JSONFunctionLocalState::JSONFunctionLocalState(ClientContext &context)
    : JSONFunctionLocalState(BufferAllocator::Get(context)) {
}

unique_ptr<FunctionLocalState> JSONFunctionLocalState::Init(ExpressionState &state, const BoundFunctionExpression &expr,
                                                            FunctionData *bind_data) {
	return make_uniq<JSONFunctionLocalState>(state.GetContext());
}

unique_ptr<FunctionLocalState> JSONFunctionLocalState::InitCastLocalState(CastLocalStateParameters &parameters) {
	return parameters.context ? make_uniq<JSONFunctionLocalState>(*parameters.context)
	                          : make_uniq<JSONFunctionLocalState>(Allocator::DefaultAllocator());
}

JSONFunctionLocalState &JSONFunctionLocalState::ResetAndGet(ExpressionState &state) {
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<JSONFunctionLocalState>();
	lstate.json_allocator.Reset();
	return lstate;
}

vector<ScalarFunctionSet> JSONFunctions::GetScalarFunctions() {
	vector<ScalarFunctionSet> functions;

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
	functions.push_back(GetExistsFunction());
	functions.push_back(GetKeysFunction());
	functions.push_back(GetTypeFunction());
	functions.push_back(GetValidFunction());
	functions.push_back(GetValueFunction());
	functions.push_back(GetSerializePlanFunction());
	functions.push_back(GetSerializeSqlFunction());
	functions.push_back(GetDeserializeSqlFunction());

	functions.push_back(GetPrettyPrintFunction());

	return functions;
}

vector<PragmaFunctionSet> JSONFunctions::GetPragmaFunctions() {
	vector<PragmaFunctionSet> functions;
	functions.push_back(GetExecuteJsonSerializedSqlPragmaFunction());
	return functions;
}

vector<TableFunctionSet> JSONFunctions::GetTableFunctions() {
	vector<TableFunctionSet> functions;

	// Reads JSON as string
	functions.push_back(GetReadJSONObjectsFunction());
	functions.push_back(GetReadNDJSONObjectsFunction());
	functions.push_back(GetReadJSONObjectsAutoFunction());

	// Read JSON as columnar data
	functions.push_back(GetReadJSONFunction());
	functions.push_back(GetReadNDJSONFunction());
	functions.push_back(GetReadJSONAutoFunction());
	functions.push_back(GetReadNDJSONAutoFunction());
	functions.push_back(GetExecuteJsonSerializedSqlFunction());

	return functions;
}

unique_ptr<TableRef> JSONFunctions::ReadJSONReplacement(ClientContext &context, ReplacementScanInput &input,
                                                        optional_ptr<ReplacementScanData> data) {
	auto table_name = ReplacementScan::GetFullPath(input);
	if (!ReplacementScan::CanReplace(table_name, {"json", "jsonl", "ndjson"})) {
		return nullptr;
	}
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	table_function->function = make_uniq<FunctionExpression>("read_json_auto", std::move(children));

	if (!FileSystem::HasGlob(table_name)) {
		auto &fs = FileSystem::GetFileSystem(context);
		table_function->alias = fs.ExtractBaseName(table_name);
	}

	return std::move(table_function);
}

static bool CastVarcharToJSON(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &lstate = parameters.local_state->Cast<JSONFunctionLocalState>();
	lstate.json_allocator.Reset();
	auto alc = lstate.json_allocator.GetYYAlc();

	bool success = true;
	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
	    source, result, count, [&](string_t input, ValidityMask &mask, idx_t idx) {
		    auto data = input.GetDataWriteable();
		    const auto length = input.GetSize();

		    yyjson_read_err error;
		    auto doc = JSONCommon::ReadDocumentUnsafe(data, length, JSONCommon::READ_FLAG, alc, &error);

		    if (!doc) {
			    mask.SetInvalid(idx);
			    if (success) {
				    HandleCastError::AssignError(JSONCommon::FormatParseError(data, length, error), parameters);
				    success = false;
			    }
		    }

		    return input;
	    });
	StringVector::AddHeapReference(result, source);
	return success;
}

void JSONFunctions::RegisterSimpleCastFunctions(CastFunctionSet &casts) {
	// JSON to VARCHAR is basically free
	casts.RegisterCastFunction(LogicalType::JSON(), LogicalType::VARCHAR, DefaultCasts::ReinterpretCast, 1);

	// VARCHAR to JSON requires a parse so it's not free. Let's make it 1 more than a cast to STRUCT
	auto varchar_to_json_cost = casts.ImplicitCastCost(LogicalType::SQLNULL, LogicalTypeId::STRUCT) + 1;
	BoundCastInfo varchar_to_json_info(CastVarcharToJSON, nullptr, JSONFunctionLocalState::InitCastLocalState);
	casts.RegisterCastFunction(LogicalType::VARCHAR, LogicalType::JSON(), std::move(varchar_to_json_info),
	                           varchar_to_json_cost);

	// Register NULL to JSON with a different cost than NULL to VARCHAR so the binder can disambiguate functions
	auto null_to_json_cost = casts.ImplicitCastCost(LogicalType::SQLNULL, LogicalTypeId::VARCHAR) + 1;
	casts.RegisterCastFunction(LogicalType::SQLNULL, LogicalType::JSON(), DefaultCasts::TryVectorNullCast,
	                           null_to_json_cost);
}

} // namespace duckdb
