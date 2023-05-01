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
class DatabaseInstance;

//! The ExtensionUtil class contains methods that are useful for extensions
class ExtensionUtil {
public:
	//! Register a new scalar function - throw an exception if the function already exists
	DUCKDB_API static void RegisterFunction(DatabaseInstance &db, ScalarFunction function);
	//! Register a new scalar function - throw an exception if the function already exists
	DUCKDB_API static void RegisterFunction(DatabaseInstance &db, ScalarFunctionSet function);
	//! Register a new table function - throw an exception if the function already exists
	DUCKDB_API static void RegisterFunction(DatabaseInstance &db, TableFunction function);
	//! Register a new pragma function - throw an exception if the function already exists
	DUCKDB_API static void RegisterFunction(DatabaseInstance &db, PragmaFunction function);
	//! Extend an existing function with a new overload
	DUCKDB_API static void ExtendFunction(DatabaseInstance &db, ScalarFunction function);

	//! Registers a new type
	DUCKDB_API static void RegisterType(DatabaseInstance &db, string type_name, LogicalType type);

	//! Registers a cast between two types
	DUCKDB_API static void RegisterCastFunction(DatabaseInstance &db, const LogicalType &source, const LogicalType &target, BoundCastInfo function,
	                                     int64_t implicit_cast_cost = -1);
};

} // namespace duckdb
