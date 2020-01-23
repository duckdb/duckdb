#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

SchemaCatalogEntry::SchemaCatalogEntry(Catalog *catalog, string name)
    : CatalogEntry(CatalogType::SCHEMA, catalog, name), tables(*catalog), indexes(*catalog), table_functions(*catalog),
      functions(*catalog), sequences(*catalog) {
}

void SchemaCatalogEntry::CreateTable(Transaction &transaction, BoundCreateTableInfo *info) {

	auto table = make_unique_base<CatalogEntry, TableCatalogEntry>(catalog, this, info);
	if (!info->base->temporary) {
		info->dependencies.insert(this);
	} else {
		table->temporary = true;
	}
	if (!tables.CreateEntry(transaction, info->base->table, move(table), info->dependencies)) {
		if (!info->base->if_not_exists) {
			throw CatalogException("Table or view with name \"%s\" already exists!", info->base->table.c_str());
		}
	}
}

void SchemaCatalogEntry::CreateView(Transaction &transaction, CreateViewInfo *info) {
	auto view = make_unique_base<CatalogEntry, ViewCatalogEntry>(catalog, this, info);
	auto old_view = tables.GetEntry(transaction, info->view_name);
	if (info->replace && old_view) {
		if (old_view->type != CatalogType::VIEW) {
			throw CatalogException("Existing object %s is not a view", info->view_name.c_str());
		}
		tables.DropEntry(transaction, info->view_name, false);
	}

	unordered_set<CatalogEntry *> dependencies{this};
	if (!tables.CreateEntry(transaction, info->view_name, move(view), dependencies)) {
		throw CatalogException("T with name \"%s\" already exists!", info->view_name.c_str());
	}
}

void SchemaCatalogEntry::DropView(Transaction &transaction, DropInfo *info) {
	auto existing_view = tables.GetEntry(transaction, info->name);
	if (existing_view && existing_view->type != CatalogType::VIEW) {
		throw CatalogException("Existing object %s is not a view", info->name.c_str());
	}
	if (!tables.DropEntry(transaction, info->name, false)) {
		if (!info->if_exists) {
			throw CatalogException("View with name \"%s\" does not exist!", info->name.c_str());
		}
	}
}

void SchemaCatalogEntry::CreateSequence(Transaction &transaction, CreateSequenceInfo *info) {
	auto sequence = make_unique_base<CatalogEntry, SequenceCatalogEntry>(catalog, this, info);
	unordered_set<CatalogEntry *> dependencies{this};
	if (!sequences.CreateEntry(transaction, info->name, move(sequence), dependencies)) {
		if (!info->if_not_exists) {
			throw CatalogException("Sequence with name \"%s\" already exists!", info->name.c_str());
		}
	}
}

void SchemaCatalogEntry::DropSequence(Transaction &transaction, DropInfo *info) {
	if (!sequences.DropEntry(transaction, info->name, info->cascade)) {
		if (!info->if_exists) {
			throw CatalogException("Sequence with name \"%s\" does not exist!", info->name.c_str());
		}
	}
}

bool SchemaCatalogEntry::CreateIndex(Transaction &transaction, CreateIndexInfo *info) {
	auto index = make_unique_base<CatalogEntry, IndexCatalogEntry>(catalog, this, info);
	unordered_set<CatalogEntry *> dependencies;
	if (name != TEMP_SCHEMA) {
		dependencies.insert(this);
	}
	if (!indexes.CreateEntry(transaction, info->index_name, move(index), dependencies)) {
		if (!info->if_not_exists) {
			throw CatalogException("Index with name \"%s\" already exists!", info->index_name.c_str());
		}
		return false;
	}
	return true;
}

void SchemaCatalogEntry::DropIndex(Transaction &transaction, DropInfo *info) {
	if (!indexes.DropEntry(transaction, info->name, false)) {
		if (!info->if_exists) {
			throw CatalogException("Index with name \"%s\" does not exist!", info->name.c_str());
		}
	}
}

void SchemaCatalogEntry::DropTable(Transaction &transaction, DropInfo *info) {
	auto old_table = tables.GetEntry(transaction, info->name);
	if (old_table && old_table->type != CatalogType::TABLE) {
		throw CatalogException("Existing object %s is not a table", info->name.c_str());
	}
	if (!tables.DropEntry(transaction, info->name, info->cascade)) {
		if (!info->if_exists) {
			throw CatalogException("Table with name \"%s\" does not exist!", info->name.c_str());
		}
	}
}

void SchemaCatalogEntry::AlterTable(ClientContext &context, AlterTableInfo *info) {
	if (!tables.AlterEntry(context, info->table, info)) {
		throw CatalogException("Table with name \"%s\" does not exist!", info->table.c_str());
	}
}

TableCatalogEntry *SchemaCatalogEntry::GetTable(Transaction &transaction, const string &table_name) {
	auto table_or_view = GetTableOrView(transaction, table_name);
	if (table_or_view->type != CatalogType::TABLE) {
		throw CatalogException("%s is not a table", table_name.c_str());
	}
	return (TableCatalogEntry *)table_or_view;
}

CatalogEntry *SchemaCatalogEntry::GetTableOrView(Transaction &transaction, const string &table_name) {
	auto entry = tables.GetEntry(transaction, table_name);
	if (!entry) {
		throw CatalogException("Table or view with name %s does not exist!", table_name.c_str());
	}
	return entry;
}

TableCatalogEntry *SchemaCatalogEntry::GetTableOrNull(Transaction &transaction, const string &table_name) {
	auto entry = tables.GetEntry(transaction, table_name);
	if (!entry) {
		return nullptr;
	}
	if (entry->type != CatalogType::TABLE) {
		return nullptr;
	}
	return (TableCatalogEntry *)entry;
}

TableFunctionCatalogEntry *SchemaCatalogEntry::GetTableFunction(Transaction &transaction,
                                                                FunctionExpression *expression) {
	auto entry = table_functions.GetEntry(transaction, expression->function_name);
	if (!entry) {
		throw CatalogException("Table Function with name %s does not exist!", expression->function_name.c_str());
	}
	auto function_entry = (TableFunctionCatalogEntry *)entry;
	// check if the argument lengths match
	if (expression->children.size() != function_entry->function.arguments.size()) {
		throw CatalogException("Function with name %s exists, but argument length does not match! "
		                       "Expected %d arguments but got %d.",
		                       expression->function_name.c_str(), (int)function_entry->function.arguments.size(),
		                       (int)expression->children.size());
	}
	return function_entry;
}

void SchemaCatalogEntry::CreateTableFunction(Transaction &transaction, CreateTableFunctionInfo *info) {
	auto table_function = make_unique_base<CatalogEntry, TableFunctionCatalogEntry>(catalog, this, info);
	unordered_set<CatalogEntry *> dependencies{this};
	if (!table_functions.CreateEntry(transaction, info->name, move(table_function), dependencies)) {
		if (!info->or_replace) {
			throw CatalogException("Table function with name \"%s\" already exists!", info->name.c_str());
		} else {
			auto table_function = make_unique_base<CatalogEntry, TableFunctionCatalogEntry>(catalog, this, info);
			// function already exists: replace it
			if (!table_functions.DropEntry(transaction, info->name, false)) {
				throw CatalogException("CREATE OR REPLACE was specified, but "
				                       "function could not be dropped!");
			}
			if (!table_functions.CreateEntry(transaction, info->name, move(table_function), dependencies)) {
				throw CatalogException("Error in recreating function in CREATE OR REPLACE");
			}
		}
	}
}

void SchemaCatalogEntry::CreateFunction(Transaction &transaction, CreateFunctionInfo *info) {
	unique_ptr<CatalogEntry> function;
	if (info->type == FunctionType::SCALAR) {
		// create a scalar function
		function =
		    make_unique_base<CatalogEntry, ScalarFunctionCatalogEntry>(catalog, this, (CreateScalarFunctionInfo *)info);
	} else {
		// create an aggregate function
		function = make_unique_base<CatalogEntry, AggregateFunctionCatalogEntry>(catalog, this,
		                                                                         (CreateAggregateFunctionInfo *)info);
	}
	unordered_set<CatalogEntry *> dependencies{this};

	if (info->or_replace) {
		// replace is set: drop the function if it exists
		functions.DropEntry(transaction, info->name, false);
	}

	if (!functions.CreateEntry(transaction, info->name, move(function), dependencies)) {
		if (!info->or_replace) {
			throw CatalogException("Function with name \"%s\" already exists!", info->name.c_str());
		} else {
			throw CatalogException("Error in creating function in CREATE OR REPLACE");
		}
	}
}

CatalogEntry *SchemaCatalogEntry::GetFunction(Transaction &transaction, const string &name, bool if_exists) {
	auto entry = functions.GetEntry(transaction, name);
	if (!entry && !if_exists) {
		throw CatalogException("Function with name %s does not exist!", name.c_str());
	}
	return entry;
}

SequenceCatalogEntry *SchemaCatalogEntry::GetSequence(Transaction &transaction, const string &name) {
	auto entry = sequences.GetEntry(transaction, name);
	if (!entry) {
		throw CatalogException("Sequence Function with name %s does not exist!", name.c_str());
	}
	return (SequenceCatalogEntry *)entry;
}

void SchemaCatalogEntry::Serialize(Serializer &serializer) {
	serializer.WriteString(name);
}

unique_ptr<CreateSchemaInfo> SchemaCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateSchemaInfo>();
	info->schema = source.Read<string>();
	return info;
}
