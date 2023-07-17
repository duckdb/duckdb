//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/system_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct PragmaCollations {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaTableInfo {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaStorageInfo {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaLastProfilingOutput {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaDetailedProfilingOutput {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaVersion {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaPlatform {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaDatabaseSize {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBSchemasFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBColumnsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBConstraintsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBDatabasesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBDependenciesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBExtensionsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBFunctionsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBKeywordsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBIndexesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBSequencesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBSettingsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBTablesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBTemporaryFilesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBTypesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBViewsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct TestType {
	TestType(LogicalType type_p, string name_p)
	    : type(std::move(type_p)), name(std::move(name_p)), min_value(Value::MinimumValue(type)),
	      max_value(Value::MaximumValue(type)) {
	}
	TestType(LogicalType type_p, string name_p, Value min, Value max)
	    : type(std::move(type_p)), name(std::move(name_p)), min_value(std::move(min)), max_value(std::move(max)) {
	}

	LogicalType type;
	string name;
	Value min_value;
	Value max_value;
};

struct TestAllTypesFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static vector<TestType> GetTestTypes(bool large_enum = false);
};

struct TestVectorTypesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
