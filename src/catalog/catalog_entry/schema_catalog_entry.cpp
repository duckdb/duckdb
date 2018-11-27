
#include "catalog/catalog_entry/schema_catalog_entry.hpp"
#include "catalog/catalog.hpp"

#include "common/exception.hpp"

#include "parser/expression/function_expression.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

SchemaCatalogEntry::SchemaCatalogEntry(Catalog *catalog, string name)
    : CatalogEntry(CatalogType::SCHEMA, catalog, name) {
}

void SchemaCatalogEntry::CreateTable(Transaction &transaction,
                                     CreateTableInformation *info) {
	auto table =
	    make_unique_base<CatalogEntry, TableCatalogEntry>(catalog, this, info);
	if (!tables.CreateEntry(transaction, info->table, move(table))) {
		if (!info->if_not_exists) {
			throw CatalogException("Table with name \"%s\" already exists!",
			                       info->table.c_str());
		}
	}
}

void SchemaCatalogEntry::DropTable(Transaction &transaction,
                                   DropTableInformation *info) {
	if (!tables.DropEntry(transaction, info->table, info->cascade)) {
		if (!info->if_exists) {
			throw CatalogException("Table with name \"%s\" does not exist!",
			                       info->table.c_str());
		}
	}
}

void SchemaCatalogEntry::AlterTable(Transaction &transaction,
                                    AlterTableInformation *info) {
	if (!tables.AlterEntry(transaction, info->table, info)) {
		throw CatalogException("Table with name \"%s\" does not exist!",
		                       info->table.c_str());
	}
}

bool SchemaCatalogEntry::TableExists(Transaction &transaction,
                                     const string &table_name) {
	return tables.EntryExists(transaction, table_name);
}

TableCatalogEntry *SchemaCatalogEntry::GetTable(Transaction &transaction,
                                                const string &table_name) {
	auto entry = tables.GetEntry(transaction, table_name);
	if (!entry) {
		throw CatalogException("Table with name %s does not exist!",
		                       table_name.c_str());
	}
	return (TableCatalogEntry *)entry;
}

TableFunctionCatalogEntry *
SchemaCatalogEntry::GetTableFunction(Transaction &transaction,
                                     FunctionExpression *expression) {
	auto entry =
	    table_functions.GetEntry(transaction, expression->function_name);
	if (!entry) {
		throw CatalogException("Table Function with name %s does not exist!",
		                       expression->function_name.c_str());
	}
	auto function_entry = (TableFunctionCatalogEntry *)entry;
	// check if the argument lengths match
	if (expression->children.size() != function_entry->arguments.size()) {
		throw CatalogException(
		    "Function with name %s exists, but argument length does not match! "
		    "Expected %d arguments but got %d.",
		    expression->function_name.c_str(),
		    (int)function_entry->arguments.size(),
		    (int)expression->children.size());
	}
	return function_entry;
}

void SchemaCatalogEntry::CreateTableFunction(
    Transaction &transaction, CreateTableFunctionInformation *info) {
	auto table_function =
	    make_unique_base<CatalogEntry, TableFunctionCatalogEntry>(catalog, this,
	                                                              info);
	if (!table_functions.CreateEntry(transaction, info->name,
	                                 move(table_function))) {
		if (!info->or_replace) {
			throw CatalogException(
			    "Table function with name \"%s\" already exists!",
			    info->name.c_str());
		} else {
			auto table_function =
			    make_unique_base<CatalogEntry, TableFunctionCatalogEntry>(
			        catalog, this, info);
			// function already exists: replace it
			if (!table_functions.DropEntry(transaction, info->name, false)) {
				throw CatalogException("CREATE OR REPLACE was specified, but "
				                       "function could not be dropped!");
			}
			if (!table_functions.CreateEntry(transaction, info->name,
			                                 move(table_function))) {
				throw CatalogException(
				    "Error in recreating function in CREATE OR REPLACE");
			}
		}
	}
}

void SchemaCatalogEntry::DropTableFunction(Transaction &transaction,
                                           DropTableFunctionInformation *info) {
	if (!table_functions.DropEntry(transaction, info->name, info->cascade)) {
		if (!info->if_exists) {
			throw CatalogException(
			    "Table function with name \"%s\" does not exist!",
			    info->name.c_str());
		}
	}
}

void SchemaCatalogEntry::CreateScalarFunction(
    Transaction &transaction, CreateScalarFunctionInformation *info) {
	auto scalar_function =
	    make_unique_base<CatalogEntry, ScalarFunctionCatalogEntry>(catalog,
	                                                               this, info);
	if (!scalar_functions.CreateEntry(transaction, info->name,
	                                  move(scalar_function))) {
		if (!info->or_replace) {
			throw CatalogException(
			    "Scalar function with name \"%s\" already exists!",
			    info->name.c_str());
		} else {
			auto scalar_function =
			    make_unique_base<CatalogEntry, ScalarFunctionCatalogEntry>(
			        catalog, this, info);
			// function already exists: replace it
			if (!scalar_functions.DropEntry(transaction, info->name, false)) {
				throw CatalogException("CREATE OR REPLACE was specified, but "
				                       "function could not be dropped!");
			}
			if (!scalar_functions.CreateEntry(transaction, info->name,
			                                  move(scalar_function))) {
				throw CatalogException(
				    "Error in recreating function in CREATE OR REPLACE");
			}
		}
	}
}

ScalarFunctionCatalogEntry *
SchemaCatalogEntry::GetScalarFunction(Transaction &transaction,
                                      const std::string &name) {
	auto entry = scalar_functions.GetEntry(transaction, name);
	if (!entry) {
		throw CatalogException("Scalar Function with name %s does not exist!",
		                       name.c_str());
	}
	return (ScalarFunctionCatalogEntry *)entry;
}

bool SchemaCatalogEntry::HasDependents(Transaction &transaction) {
	return !tables.IsEmpty(transaction) ||
	       !table_functions.IsEmpty(transaction) ||
	       !scalar_functions.IsEmpty(transaction);
}

void SchemaCatalogEntry::DropDependents(Transaction &transaction) {
	tables.DropAllEntries(transaction);
	table_functions.DropAllEntries(transaction);
	scalar_functions.DropAllEntries(transaction);
}
