//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {
struct CreateMacroInfo;
class DatabaseInstance;

//! The ExtensionUtil class contains methods that are useful for extensions
class ExtensionUtil {
public:
	//! Register a new scalar function - throw an exception if the function already exists
	DUCKDB_API static void RegisterFunction(DatabaseInstance &db, ScalarFunction function);
	//! Register a new scalar function set - throw an exception if the function already exists
	DUCKDB_API static void RegisterFunction(DatabaseInstance &db, ScalarFunctionSet function);
	//! Register a new aggregate function - throw an exception if the function already exists
	DUCKDB_API static void RegisterFunction(DatabaseInstance &db, AggregateFunction function);
	//! Register a new aggregate function set - throw an exception if the function already exists
	DUCKDB_API static void RegisterFunction(DatabaseInstance &db, AggregateFunctionSet function);
	//! Register a new table function - throw an exception if the function already exists
	DUCKDB_API static void RegisterFunction(DatabaseInstance &db, TableFunction function);
	//! Register a new table function set - throw an exception if the function already exists
	DUCKDB_API static void RegisterFunction(DatabaseInstance &db, TableFunctionSet function);
	//! Register a new pragma function - throw an exception if the function already exists
	DUCKDB_API static void RegisterFunction(DatabaseInstance &db, PragmaFunction function);
	//! Register a new pragma function set - throw an exception if the function already exists
	DUCKDB_API static void RegisterFunction(DatabaseInstance &db, PragmaFunctionSet function);
	//! Register a new copy function - throw an exception if the function already exists
	DUCKDB_API static void RegisterFunction(DatabaseInstance &db, CopyFunction function);
	//! Register a new macro function - throw an exception if the function already exists
	DUCKDB_API static void RegisterFunction(DatabaseInstance &db, CreateMacroInfo &info);

	//! Registers a new type
	DUCKDB_API static void RegisterType(DatabaseInstance &db, string type_name, LogicalType type);

	//! Registers a cast between two types
	DUCKDB_API static void RegisterCastFunction(DatabaseInstance &db, const LogicalType &source,
	                                            const LogicalType &target, BoundCastInfo function,
	                                            int64_t implicit_cast_cost = -1);
};

} // namespace duckdb
