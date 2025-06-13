#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/main/extension_install_info.hpp"

namespace duckdb {

class DatabaseInstance;
struct CreateMacroInfo;
struct CreateCollationInfo;
struct CreateAggregateFunctionInfo;
struct CreateScalarFunctionInfo;
struct CreateTableFunctionInfo;

class ExtensionLoader {
	friend class DuckDB;
	friend class ExtensionHelper;

public:
	ExtensionLoader(DatabaseInstance &db, const string &extension_name);

	//! Returns the DatabaseInstance associated with this extension loader
	DUCKDB_API DatabaseInstance &GetDatabaseInstance();

public:
	//! Set the description of the extension
	DUCKDB_API void SetDescription(const string &description);

public:
	//! Register a new scalar function - merge overloads if the function already exists
	DUCKDB_API void RegisterFunction(ScalarFunction function);
	DUCKDB_API void RegisterFunction(ScalarFunctionSet function);
	DUCKDB_API void RegisterFunction(CreateScalarFunctionInfo info);

	//! Register a new aggregate function - merge overloads if the function already exists
	DUCKDB_API void RegisterFunction(AggregateFunction function);
	DUCKDB_API void RegisterFunction(AggregateFunctionSet function);
	DUCKDB_API void RegisterFunction(CreateAggregateFunctionInfo info);

	//! Register a new table function - merge overloads if the function already exists
	DUCKDB_API void RegisterFunction(TableFunction function);
	DUCKDB_API void RegisterFunction(TableFunctionSet function);
	DUCKDB_API void RegisterFunction(CreateTableFunctionInfo info);

	//! Register a new pragma function - throw an exception if the function already exists
	DUCKDB_API void RegisterFunction(PragmaFunction function);

	//! Register a new pragma function set - throw an exception if the function already exists
	DUCKDB_API void RegisterFunction(PragmaFunctionSet function);

	//! Register a CreateSecretFunction
	DUCKDB_API void RegisterFunction(CreateSecretFunction function);

	//! Register a new copy function - throw an exception if the function already exists
	DUCKDB_API void RegisterFunction(CopyFunction function);
	//! Register a new macro function - throw an exception if the function already exists
	DUCKDB_API void RegisterFunction(CreateMacroInfo &info);

	//! Register a new collation
	DUCKDB_API void RegisterCollation(CreateCollationInfo &info);

	//! Returns a reference to the function in the catalog - throws an exception if it does not exist
	DUCKDB_API ScalarFunctionCatalogEntry &GetFunction(const string &name);
	DUCKDB_API TableFunctionCatalogEntry &GetTableFunction(const string &name);
	DUCKDB_API optional_ptr<CatalogEntry> TryGetFunction(const string &name);
	DUCKDB_API optional_ptr<CatalogEntry> TryGetTableFunction(const string &name);

	//! Add a function overload
	DUCKDB_API void AddFunctionOverload(ScalarFunction function);
	DUCKDB_API void AddFunctionOverload(ScalarFunctionSet function);
	DUCKDB_API void AddFunctionOverload(TableFunctionSet function);

	//! Registers a new type
	DUCKDB_API void RegisterType(string type_name, LogicalType type,
	                             bind_logical_type_function_t bind_function = nullptr);

	//! Registers a new secret type
	DUCKDB_API void RegisterSecretType(SecretType secret_type);

	//! Registers a cast between two types
	DUCKDB_API void RegisterCastFunction(const LogicalType &source, const LogicalType &target,
	                                     bind_cast_function_t function, int64_t implicit_cast_cost = -1);
	DUCKDB_API void RegisterCastFunction(const LogicalType &source, const LogicalType &target, BoundCastInfo function,
	                                     int64_t implicit_cast_cost = -1);

private:
	void FinalizeLoad();

private:
	DatabaseInstance &db;
	string extension_name;
	string extension_description;
};

} // namespace duckdb

//! Helper macro to define the entrypoint for a C++ extension
//! Usage:
//!
//!		DUCKDB_CPP_EXTENSION_ENTRY(my_extension, loader) {
//!			loader.RegisterFunction(...);
//!		}
//!
#define DUCKDB_CPP_EXTENSION_ENTRY(EXTENSION_NAME, LOADER_NAME)                                                        \
	DUCKDB_EXTENSION_API void EXTENSION_NAME##_duckdb_cpp_init(duckdb::ExtensionLoader &LOADER_NAME)
