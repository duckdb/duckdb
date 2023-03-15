//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "json_common.hpp"

namespace duckdb {

class TableRef;
struct ReplacementScanData;
class CastFunctionSet;
struct CastParameters;
struct JSONScanInfo;
class BuiltinFunctions;

// Scalar function stuff
struct JSONReadFunctionData : public FunctionData {
public:
	JSONReadFunctionData(bool constant, string path_p, idx_t len);
	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments);

public:
	const bool constant;
	const string path;
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
	static JSONFunctionLocalState &ResetAndGet(ExpressionState &state);

public:
	JSONAllocator json_allocator;
};

class JSONFunctions {
public:
	static vector<CreateScalarFunctionInfo> GetScalarFunctions();
	static vector<CreateTableFunctionInfo> GetTableFunctions();
	static unique_ptr<TableRef> ReadJSONReplacement(ClientContext &context, const string &table_name,
	                                                ReplacementScanData *data);
	static TableFunction GetReadJSONTableFunction(bool list_parameter, shared_ptr<JSONScanInfo> function_info);
	static CreateCopyFunctionInfo GetJSONCopyFunction();
	static void RegisterCastFunctions(CastFunctionSet &casts);

private:
	// Scalar functions
	static CreateScalarFunctionInfo GetExtractFunction();
	static CreateScalarFunctionInfo GetExtractStringFunction();

	static CreateScalarFunctionInfo GetArrayFunction();
	static CreateScalarFunctionInfo GetObjectFunction();
	static CreateScalarFunctionInfo GetToJSONFunction();
	static CreateScalarFunctionInfo GetArrayToJSONFunction();
	static CreateScalarFunctionInfo GetRowToJSONFunction();
	static CreateScalarFunctionInfo GetMergePatchFunction();

	static CreateScalarFunctionInfo GetStructureFunction();
	static CreateScalarFunctionInfo GetTransformFunction();
	static CreateScalarFunctionInfo GetTransformStrictFunction();

	static CreateScalarFunctionInfo GetArrayLengthFunction();
	static CreateScalarFunctionInfo GetContainsFunction();
	static CreateScalarFunctionInfo GetKeysFunction();
	static CreateScalarFunctionInfo GetTypeFunction();
	static CreateScalarFunctionInfo GetValidFunction();
	static CreateScalarFunctionInfo GetSerializeSqlFunction();

	template <class FUNCTION_INFO>
	static void AddAliases(const vector<string> &names, FUNCTION_INFO fun, vector<FUNCTION_INFO> &functions) {
		for (auto &name : names) {
			fun.name = name;
			functions.push_back(fun);
		}
	}

private:
	// Table functions
	static CreateTableFunctionInfo GetReadJSONObjectsFunction();
	static CreateTableFunctionInfo GetReadNDJSONObjectsFunction();
	static CreateTableFunctionInfo GetReadJSONFunction();
	static CreateTableFunctionInfo GetReadNDJSONFunction();
	static CreateTableFunctionInfo GetReadJSONAutoFunction();
	static CreateTableFunctionInfo GetReadNDJSONAutoFunction();
};

} // namespace duckdb
