#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/collate_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

SchemaCatalogEntry::SchemaCatalogEntry(Catalog *catalog, string name)
    : CatalogEntry(CatalogType::SCHEMA, catalog, name), tables(*catalog), indexes(*catalog), table_functions(*catalog),
      functions(*catalog), sequences(*catalog), collations(*catalog) {
}

CatalogEntry *SchemaCatalogEntry::AddEntry(ClientContext &context, unique_ptr<StandardEntry> entry,
                                           OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies) {
	auto entry_name = entry->name;
	auto entry_type = entry->type;
	auto result = entry.get();
	auto &transaction = Transaction::GetTransaction(context);

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
				throw CatalogException("Existing object %s is of type %s, trying to replace with type %s",
				                       entry_name.c_str(), CatalogTypeToString(old_entry->type).c_str(),
				                       CatalogTypeToString(entry_type).c_str());
			}
			(void)set.DropEntry(transaction, entry_name, false);
		}
	}
	// now try to add the entry
	if (!set.CreateEntry(transaction, entry_name, move(entry), dependencies)) {
		// entry already exists!
		if (on_conflict == OnCreateConflict::ERROR) {
			throw CatalogException("%s with name \"%s\" already exists!", CatalogTypeToString(entry_type).c_str(),
			                       entry_name.c_str());
		} else {
			return nullptr;
		}
	}
	return result;
}

CatalogEntry *SchemaCatalogEntry::AddEntry(ClientContext &context, unique_ptr<StandardEntry> entry,
                                           OnCreateConflict on_conflict) {
	unordered_set<CatalogEntry *> dependencies;
	return AddEntry(context, move(entry), on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreateSequence(ClientContext &context, CreateSequenceInfo *info) {
	auto sequence = make_unique<SequenceCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(sequence), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateTable(ClientContext &context, BoundCreateTableInfo *info) {
	auto table = make_unique<TableCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(table), info->Base().on_conflict, info->dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreateView(ClientContext &context, CreateViewInfo *info) {
	auto view = make_unique<ViewCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(view), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateIndex(ClientContext &context, CreateIndexInfo *info) {
	auto index = make_unique<IndexCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(index), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateCollation(ClientContext &context, CreateCollationInfo *info) {
	auto collation = make_unique<CollateCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(collation), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateTableFunction(ClientContext &context, CreateTableFunctionInfo *info) {
	auto table_function = make_unique<TableFunctionCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(table_function), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateFunction(ClientContext &context, CreateFunctionInfo *info) {
	unique_ptr<StandardEntry> function;
	if (info->type == CatalogType::SCALAR_FUNCTION) {
		// create a scalar function
		function = make_unique_base<StandardEntry, ScalarFunctionCatalogEntry>(catalog, this,
		                                                                       (CreateScalarFunctionInfo *)info);
	} else {
		assert(info->type == CatalogType::AGGREGATE_FUNCTION);
		// create an aggregate function
		function = make_unique_base<StandardEntry, AggregateFunctionCatalogEntry>(catalog, this,
		                                                                          (CreateAggregateFunctionInfo *)info);
	}
	return AddEntry(context, move(function), info->on_conflict);
}

void SchemaCatalogEntry::DropEntry(ClientContext &context, DropInfo *info) {
	auto &set = GetCatalogSet(info->type);
	auto &transaction = Transaction::GetTransaction(context);

	// first find the entry
	auto existing_entry = set.GetEntry(transaction, info->name);
	if (!existing_entry) {
		if (!info->if_exists) {
			throw CatalogException("%s with name \"%s\" does not exist!", CatalogTypeToString(info->type).c_str(),
			                       info->name.c_str());
		}
		return;
	}
	if (existing_entry->type != info->type) {
		throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", info->name.c_str(),
		                       CatalogTypeToString(existing_entry->type).c_str(),
		                       CatalogTypeToString(info->type).c_str());
	}
	if (!set.DropEntry(transaction, info->name, info->cascade)) {
		throw InternalException("Could not drop element because of an internal error");
	}
}

void SchemaCatalogEntry::AlterTable(ClientContext &context, AlterTableInfo *info) {
	switch (info->alter_table_type) {
	case AlterTableType::RENAME_TABLE: {
		auto &transaction = Transaction::GetTransaction(context);
		auto entry = tables.GetEntry(transaction, info->table);
		if (entry == nullptr) {
			throw CatalogException("Table \"%s\" doesn't exist!", info->table.c_str());
		}
		assert(entry->type == CatalogType::TABLE);

		auto copied_entry = entry->Copy(context);

		// Drop the old table entry
		if (!tables.DropEntry(transaction, info->table, false)) {
			throw CatalogException("Could not drop \"%s\" entry!", info->table.c_str());
		}

		// Create a new table entry
		auto &new_table = ((RenameTableInfo *)info)->new_table_name;
		unordered_set<CatalogEntry *> dependencies;
		copied_entry->name = new_table;
		if (!tables.CreateEntry(transaction, new_table, move(copied_entry), dependencies)) {
			throw CatalogException("Could not create \"%s\" entry!", new_table.c_str());
		}
		break;
	}
	default:
		if (!tables.AlterEntry(context, info->table, info)) {
			throw CatalogException("Table with name \"%s\" does not exist!", info->table.c_str());
		}
	} // end switch
}

CatalogEntry *SchemaCatalogEntry::GetEntry(ClientContext &context, CatalogType type, const string &name,
                                           bool if_exists) {
	auto &set = GetCatalogSet(type);
	auto &transaction = Transaction::GetTransaction(context);

	auto entry = set.GetEntry(transaction, name);
	if (!entry) {
		if (!if_exists) {
			throw CatalogException("%s with name %s does not exist!", CatalogTypeToString(type).c_str(), name.c_str());
		}
		return nullptr;
	}
	return entry;
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
	switch (type) {
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
	case CatalogType::COLLATION:
		return collations;
	default:
		throw CatalogException("Unsupported catalog type in schema");
	}
}
