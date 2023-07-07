//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/extension_util.hpp"
#include "json_common.hpp"

namespace duckdb {

class TableRef;
struct ReplacementScanData;
class CastFunctionSet;
struct CastParameters;
struct CastLocalStateParameters;
struct JSONScanInfo;
class BuiltinFunctions;

// Scalar function stuff
struct JSONReadFunctionData : public FunctionData {
public:
	JSONReadFunctionData(bool constant, string path_p, idx_t len, JSONCommon::JSONPathType path_type);
	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments);

public:
	const bool constant;
	const string path;
	const JSONCommon::JSONPathType path_type;
	const char *ptr;
	const size_t len;
};

struct JSONReadManyFunctionData : public FunctionData {
public:
	JSONReadManyFunctionData(vector<string> paths_p, vector<size_t> lens_p);
	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments);

public:
	const vector<string> paths;
	vector<const char *> ptrs;
	const vector<size_t> lens;
};

struct JSONFunctionLocalState : public FunctionLocalState {
public:
	explicit JSONFunctionLocalState(Allocator &allocator);
	explicit JSONFunctionLocalState(ClientContext &context);
	static unique_ptr<FunctionLocalState> Init(ExpressionState &state, const BoundFunctionExpression &expr,
	                                           FunctionData *bind_data);
	static unique_ptr<FunctionLocalState> InitCastLocalState(CastLocalStateParameters &parameters);
	static JSONFunctionLocalState &ResetAndGet(ExpressionState &state);

public:
	JSONAllocator json_allocator;
};

class JSONFunctions {
public:
	static vector<ScalarFunctionSet> GetScalarFunctions();
	static vector<PragmaFunctionSet> GetPragmaFunctions();
	static vector<TableFunctionSet> GetTableFunctions();
	static unique_ptr<TableRef> ReadJSONReplacement(ClientContext &context, const string &table_name,
	                                                ReplacementScanData *data);
	static TableFunction GetReadJSONTableFunction(shared_ptr<JSONScanInfo> function_info);
	static CopyFunction GetJSONCopyFunction();
	static void RegisterSimpleCastFunctions(CastFunctionSet &casts);
	static void RegisterJSONCreateCastFunctions(CastFunctionSet &casts);
	static void RegisterJSONTransformCastFunctions(CastFunctionSet &casts);

private:
	// Scalar functions
	static ScalarFunctionSet GetExtractFunction();
	static ScalarFunctionSet GetExtractStringFunction();

	static ScalarFunctionSet GetArrayFunction();
	static ScalarFunctionSet GetObjectFunction();
	static ScalarFunctionSet GetToJSONFunction();
	static ScalarFunctionSet GetArrayToJSONFunction();
	static ScalarFunctionSet GetRowToJSONFunction();
	static ScalarFunctionSet GetMergePatchFunction();

	static ScalarFunctionSet GetStructureFunction();
	static ScalarFunctionSet GetTransformFunction();
	static ScalarFunctionSet GetTransformStrictFunction();

	static ScalarFunctionSet GetArrayLengthFunction();
	static ScalarFunctionSet GetContainsFunction();
	static ScalarFunctionSet GetKeysFunction();
	static ScalarFunctionSet GetTypeFunction();
	static ScalarFunctionSet GetValidFunction();
	static ScalarFunctionSet GetSerializeSqlFunction();
	static ScalarFunctionSet GetDeserializeSqlFunction();

	static PragmaFunctionSet GetExecuteJsonSerializedSqlPragmaFunction();

	template <class FUNCTION_INFO>
	static void AddAliases(const vector<string> &names, FUNCTION_INFO fun, vector<FUNCTION_INFO> &functions) {
		for (auto &name : names) {
			fun.name = name;
			functions.push_back(fun);
		}
	}

private:
	// Table functions
	static TableFunctionSet GetReadJSONObjectsFunction();
	static TableFunctionSet GetReadNDJSONObjectsFunction();
	static TableFunctionSet GetReadJSONObjectsAutoFunction();

	static TableFunctionSet GetReadJSONFunction();
	static TableFunctionSet GetReadNDJSONFunction();
	static TableFunctionSet GetReadJSONAutoFunction();
	static TableFunctionSet GetReadNDJSONAutoFunction();

	static TableFunctionSet GetExecuteJsonSerializedSqlFunction();
};

} // namespace duckdb
