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

bool SchemaCatalogEntry::AddEntry(Transaction &transaction, unique_ptr<StandardEntry> entry, OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies) {
	auto entry_name = entry->name;
	auto entry_type = entry->type;

	// first find the set for this entry
	auto &set = GetCatalogSet(entry_type);

	if (name != TEMP_SCHEMA) {
		dependencies.insert(this);
	} else {
		entry->temporary = true;
	}
	if (on_conflict == OnCreateConflict::REPLACE) {
		// CREATE OR REPLACE: first try to drop the entry
		auto old_entry = set.GetEntry(transaction, entry_name);
		if (old_entry) {
			if (old_entry->type != entry_type) {
				throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", entry_name.c_str(), CatalogTypeToString(old_entry->type).c_str(), CatalogTypeToString(entry_type).c_str());
			}
			(void) set.DropEntry(transaction, entry_name, false);
		}
	}
	// now try to add the entry
	if (!set.CreateEntry(transaction, entry_name, move(entry), dependencies)) {
		// entry already exists!
		if (on_conflict == OnCreateConflict::ERROR) {
			throw CatalogException("%s with name \"%s\" already exists!", CatalogTypeToString(entry_type).c_str(), entry_name.c_str());
		} else {
			return false;
		}
	}
	return true;
}

void SchemaCatalogEntry::CreateSequence(Transaction &transaction, CreateSequenceInfo *info) {
	auto sequence = make_unique<SequenceCatalogEntry>(catalog, this, info);
	AddEntry(transaction, move(sequence), info->on_conflict);
}

void SchemaCatalogEntry::CreateTable(Transaction &transaction, BoundCreateTableInfo *info) {
	auto table = make_unique<TableCatalogEntry>(catalog, this, info);
	AddEntry(transaction, move(table), info->Base().on_conflict, info->dependencies);
}

void SchemaCatalogEntry::CreateView(Transaction &transaction, CreateViewInfo *info) {
	auto view = make_unique<ViewCatalogEntry>(catalog, this, info);
	AddEntry(transaction, move(view), info->on_conflict);
}

bool SchemaCatalogEntry::CreateIndex(Transaction &transaction, CreateIndexInfo *info) {
	auto index = make_unique<IndexCatalogEntry>(catalog, this, info);
	return AddEntry(transaction, move(index), info->on_conflict);
}

void SchemaCatalogEntry::CreateTableFunction(Transaction &transaction, CreateTableFunctionInfo *info) {
	auto table_function = make_unique<TableFunctionCatalogEntry>(catalog, this, info);
	AddEntry(transaction, move(table_function), info->on_conflict);
}

void SchemaCatalogEntry::CreateFunction(Transaction &transaction, CreateFunctionInfo *info) {
	unique_ptr<StandardEntry> function;
	if (info->type == CatalogType::SCALAR_FUNCTION) {
		// create a scalar function
		function = make_unique_base<StandardEntry, ScalarFunctionCatalogEntry>(catalog, this, (CreateScalarFunctionInfo *)info);
	} else {
		assert(info->type == CatalogType::AGGREGATE_FUNCTION);
		// create an aggregate function
		function = make_unique_base<StandardEntry, AggregateFunctionCatalogEntry>(catalog, this,
		                                                                         (CreateAggregateFunctionInfo *)info);
	}
	AddEntry(transaction, move(function), info->on_conflict);
}

void SchemaCatalogEntry::DropEntry(Transaction &transaction, DropInfo *info) {
	auto &set = GetCatalogSet(info->type);
	// first find the entry
	auto existing_entry = set.GetEntry(transaction, info->name);
	if (!existing_entry) {
		if (info->if_exists) {
			throw CatalogException("%s with name \"%s\" does not exist!", CatalogTypeToString(info->type).c_str(), info->name.c_str());
		}
		return;
	}
	if (existing_entry->type != info->type) {
		throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", info->name.c_str(), CatalogTypeToString(existing_entry->type).c_str(), CatalogTypeToString(info->type).c_str());
	}
	if (!set.DropEntry(transaction, info->name, info->cascade)) {
		throw InternalException("Could not drop element because of an internal error");
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

CatalogEntry *SchemaCatalogEntry::GetFunction(Transaction &transaction, const string &name, bool if_exists) {
	auto entry = functions.GetEntry(transaction, name);
	if (!entry && !if_exists) {
		throw CatalogException("Function with name \"%s\" does not exist!", name.c_str());
	}
	return entry;
}

SequenceCatalogEntry *SchemaCatalogEntry::GetSequence(Transaction &transaction, const string &name) {
	auto entry = sequences.GetEntry(transaction, name);
	if (!entry) {
		throw CatalogException("Sequence Function with name \"%s\" does not exist!", name.c_str());
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

CatalogSet &SchemaCatalogEntry::GetCatalogSet(CatalogType type) {
	switch(type) {
	case CatalogType::VIEW:
	case CatalogType::TABLE:
		return tables;
	case CatalogType::INDEX:
		return indexes;
	case CatalogType::TABLE_FUNCTION:
		return table_functions;
	case CatalogType::AGGREGATE_FUNCTION:
	case CatalogType::SCALAR_FUNCTION:
		return functions;
	case CatalogType::SEQUENCE:
		return sequences;
	default:
		throw CatalogException("Unsupported catalog type in schema");
 	}
}
