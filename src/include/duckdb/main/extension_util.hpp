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
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/main/extension_install_info.hpp"

namespace duckdb {
struct CreateMacroInfo;
struct CreateCollationInfo;
class DatabaseInstance;

//! The ExtensionUtil class contains methods that are useful for extensions
class ExtensionUtil {
public:
	//! Register a new DuckDB extension
	DUCKDB_API static void RegisterExtension(DatabaseInstance &db, const string &name, const ExtensionLoadedInfo &info);
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

	//! Register a CreateSecretFunction
	DUCKDB_API static void RegisterFunction(DatabaseInstance &db, CreateSecretFunction function);

	//! Register a new copy function - throw an exception if the function already exists
	DUCKDB_API static void RegisterFunction(DatabaseInstance &db, CopyFunction function);
	//! Register a new macro function - throw an exception if the function already exists
	DUCKDB_API static void RegisterFunction(DatabaseInstance &db, CreateMacroInfo &info);

	//! Register a new collation
	DUCKDB_API static void RegisterCollation(DatabaseInstance &db, CreateCollationInfo &info);

	//! Returns a reference to the function in the catalog - throws an exception if it does not exist
	DUCKDB_API static ScalarFunctionCatalogEntry &GetFunction(DatabaseInstance &db, const string &name);
	DUCKDB_API static TableFunctionCatalogEntry &GetTableFunction(DatabaseInstance &db, const string &name);

	//! Add a function overload
	DUCKDB_API static void AddFunctionOverload(DatabaseInstance &db, ScalarFunction function);
	DUCKDB_API static void AddFunctionOverload(DatabaseInstance &db, ScalarFunctionSet function);
	DUCKDB_API static void AddFunctionOverload(DatabaseInstance &db, TableFunctionSet function);

	//! Registers a new type
	DUCKDB_API static void RegisterType(DatabaseInstance &db, string type_name, LogicalType type,
	                                    bind_type_modifiers_function_t bind_type_modifiers = nullptr);

	//! Registers a new secret type
	DUCKDB_API static void RegisterSecretType(DatabaseInstance &db, SecretType secret_type);

	//! Registers a cast between two types
	DUCKDB_API static void RegisterCastFunction(DatabaseInstance &db, const LogicalType &source,
	                                            const LogicalType &target, BoundCastInfo function,
	                                            int64_t implicit_cast_cost = -1);
};

} // namespace duckdb
