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

JSONPathType JSONReadFunctionData::CheckPath(const Value &path_val, string &path, idx_t &len) {
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
	idx_t len = 0;
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

JSONReadManyFunctionData::JSONReadManyFunctionData(vector<string> paths_p, vector<idx_t> lens_p)
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
	vector<idx_t> lens;
	auto paths_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);

	for (auto &path_val : ListValue::GetChildren(paths_val)) {
		paths.emplace_back("");
		lens.push_back(0);
		if (JSONReadFunctionData::CheckPath(path_val, paths.back(), lens.back()) == JSONPathType::WILDCARD) {
			throw BinderException("Cannot have wildcards in JSON path when supplying multiple paths");
		}
	}

	return make_uniq<JSONReadManyFunctionData>(std::move(paths), std::move(lens));
}

JSONFunctionLocalState::JSONFunctionLocalState(Allocator &allocator)
    : json_allocator(make_shared_ptr<JSONAllocator>(allocator)) {
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
	lstate.json_allocator->Reset();
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

	// Table in-out
	functions.push_back(GetJSONEachFunction());
	functions.push_back(GetJSONTreeFunction());

	// Serialized plan
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
	lstate.json_allocator->Reset();
	auto alc = lstate.json_allocator->GetYYAlc();

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

static bool CastJSONListToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &) {
	UnifiedVectorFormat child_format;
	ListVector::GetEntry(source).ToUnifiedFormat(ListVector::GetListSize(source), child_format);
	const auto input_jsons = UnifiedVectorFormat::GetData<string_t>(child_format);

	static constexpr char const *NULL_STRING = "NULL";
	static constexpr idx_t NULL_STRING_LENGTH = 4;

	UnaryExecutor::Execute<list_entry_t, string_t>(
	    source, result, count,
	    [&](const list_entry_t &input) {
		    // Compute len (start with [] and ,)
		    idx_t len = 2;
		    len += input.length == 0 ? 0 : (input.length - 1) * 2;
		    for (idx_t json_idx = input.offset; json_idx < input.offset + input.length; json_idx++) {
			    const auto sel_json_idx = child_format.sel->get_index(json_idx);
			    if (child_format.validity.RowIsValid(sel_json_idx)) {
				    len += input_jsons[sel_json_idx].GetSize();
			    } else {
				    len += NULL_STRING_LENGTH;
			    }
		    }

		    // Allocate string
		    auto res = StringVector::EmptyString(result, len);
		    auto ptr = res.GetDataWriteable();

		    // Populate string
		    *ptr++ = '[';
		    for (idx_t json_idx = input.offset; json_idx < input.offset + input.length; json_idx++) {
			    const auto sel_json_idx = child_format.sel->get_index(json_idx);
			    if (child_format.validity.RowIsValid(sel_json_idx)) {
				    auto &input_json = input_jsons[sel_json_idx];
				    memcpy(ptr, input_json.GetData(), input_json.GetSize());
				    ptr += input_json.GetSize();
			    } else {
				    memcpy(ptr, NULL_STRING, NULL_STRING_LENGTH);
				    ptr += NULL_STRING_LENGTH;
			    }
			    if (json_idx != input.offset + input.length - 1) {
				    *ptr++ = ',';
				    *ptr++ = ' ';
			    }
		    }
		    *ptr = ']';

		    res.Finalize();
		    return res;
	    },
	    FunctionErrors::CANNOT_ERROR);
	return true;
}

static bool CastVarcharToJSONList(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &lstate = parameters.local_state->Cast<JSONFunctionLocalState>();
	lstate.json_allocator->Reset();
	auto alc = lstate.json_allocator->GetYYAlc();

	bool success = true;
	UnaryExecutor::ExecuteWithNulls<string_t, list_entry_t>(
	    source, result, count, [&](const string_t &input, ValidityMask &mask, idx_t idx) -> list_entry_t {
		    // Figure out if the cast can succeed
		    yyjson_read_err error;
		    const auto doc = JSONCommon::ReadDocumentUnsafe(input.GetDataWriteable(), input.GetSize(),
		                                                    JSONCommon::READ_FLAG, alc, &error);
		    if (!doc || !unsafe_yyjson_is_arr(doc->root)) {
			    mask.SetInvalid(idx);
			    if (success) {
				    if (!doc) {
					    HandleCastError::AssignError(
					        JSONCommon::FormatParseError(input.GetDataWriteable(), input.GetSize(), error), parameters);
				    } else if (!unsafe_yyjson_is_arr(doc->root)) {
					    auto truncated_input =
					        input.GetSize() > 50 ? string(input.GetData(), 47) + "..." : input.GetString();
					    HandleCastError::AssignError(
					        StringUtil::Format("Cannot cast to list of JSON. Input \"%s\"", truncated_input),
					        parameters);
				    }
				    success = false;
			    }
			    return {};
		    }

		    auto current_size = ListVector::GetListSize(result);
		    const auto arr_len = unsafe_yyjson_get_len(doc->root);
		    const auto new_size = current_size + arr_len;

		    // Grow list if needed
		    if (ListVector::GetListCapacity(result) < new_size) {
			    ListVector::Reserve(result, new_size);
		    }

		    // Populate list
		    const auto result_jsons = FlatVector::GetData<string_t>(ListVector::GetEntry(result));
		    size_t arr_idx, max;
		    yyjson_val *val;
		    yyjson_arr_foreach(doc->root, arr_idx, max, val) {
			    result_jsons[current_size + arr_idx] = JSONCommon::WriteVal(val, alc);
		    }

		    // Update size
		    ListVector::SetListSize(result, current_size + arr_len);

		    return {current_size, arr_len};
	    });

	JSONAllocator::AddBuffer(ListVector::GetEntry(result), alc);
	return success;
}

void JSONFunctions::RegisterSimpleCastFunctions(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();

	// JSON to VARCHAR is basically free
	loader.RegisterCastFunction(LogicalType::JSON(), LogicalType::VARCHAR, DefaultCasts::ReinterpretCast, 1);

	// VARCHAR to JSON requires a parse so it's not free. Let's make it 1 more than a cast to STRUCT
	const auto varchar_to_json_cost =
	    CastFunctionSet::ImplicitCastCost(db, LogicalType::SQLNULL, LogicalTypeId::STRUCT) + 1;
	BoundCastInfo varchar_to_json_info(CastVarcharToJSON, nullptr, JSONFunctionLocalState::InitCastLocalState);
	loader.RegisterCastFunction(LogicalType::VARCHAR, LogicalType::JSON(), std::move(varchar_to_json_info),
	                            varchar_to_json_cost);

	// Register NULL to JSON with a different cost than NULL to VARCHAR so the binder can disambiguate functions
	const auto null_to_json_cost =
	    CastFunctionSet::ImplicitCastCost(db, LogicalType::SQLNULL, LogicalTypeId::VARCHAR) + 1;
	loader.RegisterCastFunction(LogicalType::SQLNULL, LogicalType::JSON(), DefaultCasts::TryVectorNullCast,
	                            null_to_json_cost);

	// JSON[] to VARCHAR (this needs a special case otherwise the cast will escape quotes)
	const auto json_list_to_varchar_cost =
	    CastFunctionSet::ImplicitCastCost(db, LogicalType::LIST(LogicalType::JSON()), LogicalTypeId::VARCHAR) - 1;
	loader.RegisterCastFunction(LogicalType::LIST(LogicalType::JSON()), LogicalTypeId::VARCHAR, CastJSONListToVarchar,
	                            json_list_to_varchar_cost);

	// VARCHAR to JSON[] (also needs a special case otherwise get a VARCHAR -> VARCHAR[] cast first)
	const auto varchar_to_json_list_cost =
	    CastFunctionSet::ImplicitCastCost(db, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::JSON())) - 1;
	BoundCastInfo varchar_to_json_list_info(CastVarcharToJSONList, nullptr, JSONFunctionLocalState::InitCastLocalState);
	loader.RegisterCastFunction(LogicalType::VARCHAR, LogicalType::LIST(LogicalType::JSON()),
	                            std::move(varchar_to_json_list_info), varchar_to_json_list_cost);
}

} // namespace duckdb
