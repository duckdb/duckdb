#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/collate_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/matview_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/catalog/default/default_views.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/constraints/bound_foreign_key_constraint.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/constraints/bound_foreign_key_constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/catalog/default/default_types.hpp"

#include <algorithm>
#include <sstream>

namespace duckdb {

void FindForeignKeyInformation(CatalogEntry *entry, AlterForeignKeyType alter_fk_type,
                               vector<unique_ptr<AlterForeignKeyInfo>> &fk_arrays) {
	if (entry->type != CatalogType::TABLE_ENTRY) {
		return;
	}
	auto *table_entry = (TableCatalogEntry *)entry;
	for (idx_t i = 0; i < table_entry->constraints.size(); i++) {
		auto &cond = table_entry->constraints[i];
		if (cond->type != ConstraintType::FOREIGN_KEY) {
			continue;
		}
		auto &fk = (ForeignKeyConstraint &)*cond;
		if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
			fk_arrays.push_back(make_unique<AlterForeignKeyInfo>(fk.info.schema, fk.info.table, entry->name,
			                                                     fk.pk_columns, fk.fk_columns, fk.info.pk_keys,
			                                                     fk.info.fk_keys, alter_fk_type));
		} else if (fk.info.type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE &&
		           alter_fk_type == AlterForeignKeyType::AFT_DELETE) {
			throw CatalogException("Could not drop the table because this table is main key table of the table \"%s\"",
			                       fk.info.table);
		}
	}
}

SchemaCatalogEntry::SchemaCatalogEntry(Catalog *catalog, string name_p, bool internal)
    : CatalogEntry(CatalogType::SCHEMA_ENTRY, catalog, move(name_p)),
      tables(*catalog, make_unique<DefaultViewGenerator>(*catalog, this)), indexes(*catalog), table_functions(*catalog),
      copy_functions(*catalog), pragma_functions(*catalog),
      functions(*catalog, make_unique<DefaultFunctionGenerator>(*catalog, this)), sequences(*catalog),
      collations(*catalog), types(*catalog, make_unique<DefaultTypeGenerator>(*catalog, this)) {
	this->internal = internal;
}

CatalogEntry *SchemaCatalogEntry::AddEntry(ClientContext &context, unique_ptr<StandardEntry> entry,
                                           OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies) {
	auto entry_name = entry->name;
	auto entry_type = entry->type;
	auto result = entry.get();

	// first find the set for this entry
	auto &set = GetCatalogSet(entry_type);

	if (name != TEMP_SCHEMA) {
		dependencies.insert(this);
	} else {
		entry->temporary = true;
	}
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// CREATE OR REPLACE: first try to drop the entry
		auto old_entry = set.GetEntry(context, entry_name);
		if (old_entry) {
			if (old_entry->type != entry_type) {
				throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", entry_name,
				                       CatalogTypeToString(old_entry->type), CatalogTypeToString(entry_type));
			}
			(void)set.DropEntry(context, entry_name, false);
		}
	}
	// now try to add the entry
	if (!set.CreateEntry(context, entry_name, move(entry), dependencies)) {
		// entry already exists!
		if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw CatalogException("%s with name \"%s\" already exists!", CatalogTypeToString(entry_type), entry_name);
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

CatalogEntry *SchemaCatalogEntry::CreateType(ClientContext &context, CreateTypeInfo *info) {
	auto type_entry = make_unique<TypeCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(type_entry), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateTable(ClientContext &context, BoundCreateTableInfo *info) {
	auto table = make_unique<TableCatalogEntry>(catalog, this, info);
	table->storage->info->cardinality = table->storage->GetTotalRows();
	CatalogEntry *entry = AddEntry(context, move(table), info->Base().on_conflict, info->dependencies);
	if (!entry) {
		return nullptr;
	}
	// add a foreign key constraint in main key table if there is a foreign key constraint
	vector<unique_ptr<AlterForeignKeyInfo>> fk_arrays;
	FindForeignKeyInformation(entry, AlterForeignKeyType::AFT_ADD, fk_arrays);
	for (idx_t i = 0; i < fk_arrays.size(); i++) {
		// alter primary key table
		AlterForeignKeyInfo *fk_info = fk_arrays[i].get();
		catalog->Alter(context, fk_info);

		// make a dependency between this table and referenced table
		auto &set = GetCatalogSet(CatalogType::TABLE_ENTRY);
		info->dependencies.insert(set.GetEntry(context, fk_info->name));
	}
	return entry;
}

CatalogEntry *SchemaCatalogEntry::CreateMatView(ClientContext &context, BoundCreateTableInfo *info) {
	auto table = make_unique<MatViewCatalogEntry>(catalog, this, info);
	table->storage->info->cardinality = table->storage->GetTotalRows();
	CatalogEntry *entry = AddEntry(context, move(table), info->Base().on_conflict, info->dependencies);
	return entry;
}

CatalogEntry *SchemaCatalogEntry::CreateView(ClientContext &context, CreateViewInfo *info) {
	auto view = make_unique<ViewCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(view), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateIndex(ClientContext &context, CreateIndexInfo *info, TableCatalogEntry *table) {
	unordered_set<CatalogEntry *> dependencies;
	dependencies.insert(table);
	auto index = make_unique<IndexCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(index), info->on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreateCollation(ClientContext &context, CreateCollationInfo *info) {
	auto collation = make_unique<CollateCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(collation), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateTableFunction(ClientContext &context, CreateTableFunctionInfo *info) {
	auto table_function = make_unique<TableFunctionCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(table_function), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateCopyFunction(ClientContext &context, CreateCopyFunctionInfo *info) {
	auto copy_function = make_unique<CopyFunctionCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(copy_function), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreatePragmaFunction(ClientContext &context, CreatePragmaFunctionInfo *info) {
	auto pragma_function = make_unique<PragmaFunctionCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(pragma_function), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateFunction(ClientContext &context, CreateFunctionInfo *info) {
	unique_ptr<StandardEntry> function;
	switch (info->type) {
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		function = make_unique_base<StandardEntry, ScalarFunctionCatalogEntry>(catalog, this,
		                                                                       (CreateScalarFunctionInfo *)info);
		break;
	case CatalogType::MACRO_ENTRY:
		// create a macro function
		function = make_unique_base<StandardEntry, ScalarMacroCatalogEntry>(catalog, this, (CreateMacroInfo *)info);
		break;

	case CatalogType::TABLE_MACRO_ENTRY:
		// create a macro function
		function = make_unique_base<StandardEntry, TableMacroCatalogEntry>(catalog, this, (CreateMacroInfo *)info);
		break;
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
		D_ASSERT(info->type == CatalogType::AGGREGATE_FUNCTION_ENTRY);
		// create an aggregate function
		function = make_unique_base<StandardEntry, AggregateFunctionCatalogEntry>(catalog, this,
		                                                                          (CreateAggregateFunctionInfo *)info);
		break;
	default:
		throw InternalException("Unknown function type \"%s\"", CatalogTypeToString(info->type));
	}
	return AddEntry(context, move(function), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::AddFunction(ClientContext &context, CreateFunctionInfo *info) {
	auto entry = GetCatalogSet(info->type).GetEntry(context, info->name);
	if (!entry) {
		return CreateFunction(context, info);
	}

	info->on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	switch (info->type) {
	case CatalogType::SCALAR_FUNCTION_ENTRY: {
		auto scalar_info = (CreateScalarFunctionInfo *)info;
		auto &scalars = *(ScalarFunctionCatalogEntry *)entry;
		for (const auto &scalar : scalars.functions) {
			scalar_info->functions.emplace_back(scalar);
		}
		break;
	}
	case CatalogType::AGGREGATE_FUNCTION_ENTRY: {
		auto agg_info = (CreateAggregateFunctionInfo *)info;
		auto &aggs = *(AggregateFunctionCatalogEntry *)entry;
		for (const auto &agg : aggs.functions) {
			agg_info->functions.AddFunction(agg);
		}
		break;
	}
	default:
		// Macros can only be replaced because there is only one of each name.
		throw InternalException("Unsupported function type \"%s\" for adding", CatalogTypeToString(info->type));
	}
	return CreateFunction(context, info);
}

void SchemaCatalogEntry::DropEntry(ClientContext &context, DropInfo *info) {
	auto &set = GetCatalogSet(info->type);

	// first find the entry
	auto existing_entry = set.GetEntry(context, info->name);
	if (!existing_entry) {
		if (!info->if_exists) {
			throw CatalogException("%s with name \"%s\" does not exist!", CatalogTypeToString(info->type), info->name);
		}
		return;
	}
	if (existing_entry->type != info->type) {
		throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", info->name,
		                       CatalogTypeToString(existing_entry->type), CatalogTypeToString(info->type));
	}

	// if there is a foreign key constraint, get that information
	vector<unique_ptr<AlterForeignKeyInfo>> fk_arrays;
	FindForeignKeyInformation(existing_entry, AlterForeignKeyType::AFT_DELETE, fk_arrays);

	if (!set.DropEntry(context, info->name, info->cascade)) {
		throw InternalException("Could not drop element because of an internal error");
	}

	// remove the foreign key constraint in main key table if main key table's name is valid
	for (idx_t i = 0; i < fk_arrays.size(); i++) {
		// alter primary key tablee
		Catalog::GetCatalog(context).Alter(context, fk_arrays[i].get());
	}
}

void SchemaCatalogEntry::Alter(ClientContext &context, AlterInfo *info) {
	CatalogType type = info->GetCatalogType();
	auto &set = GetCatalogSet(type);
	if (info->type == AlterType::CHANGE_OWNERSHIP) {
		if (!set.AlterOwnership(context, (ChangeOwnershipInfo *)info)) {
			throw CatalogException("Couldn't change ownership!");
		}
	} else {
		string name = info->name;
		if (!set.AlterEntry(context, name, info)) {
			throw CatalogException("Entry with name \"%s\" does not exist!", name);
		}
	}
}

void SchemaCatalogEntry::Scan(ClientContext &context, CatalogType type,
                              const std::function<void(CatalogEntry *)> &callback) {
	auto &set = GetCatalogSet(type);
	set.Scan(context, callback);
}

void SchemaCatalogEntry::Scan(CatalogType type, const std::function<void(CatalogEntry *)> &callback) {
	auto &set = GetCatalogSet(type);
	set.Scan(callback);
}

void SchemaCatalogEntry::Serialize(Serializer &serializer) {
	FieldWriter writer(serializer);
	writer.WriteString(name);
	writer.Finalize();
}

unique_ptr<CreateSchemaInfo> SchemaCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateSchemaInfo>();

	FieldReader reader(source);
	info->schema = reader.ReadRequired<string>();
	reader.Finalize();

	return info;
}

string SchemaCatalogEntry::ToSQL() {
	std::stringstream ss;
	ss << "CREATE SCHEMA " << name << ";";
	return ss.str();
}

CatalogSet &SchemaCatalogEntry::GetCatalogSet(CatalogType type) {
	switch (type) {
	case CatalogType::MATVIEW_ENTRY:
	case CatalogType::VIEW_ENTRY:
	case CatalogType::TABLE_ENTRY:
		return tables;
	case CatalogType::INDEX_ENTRY:
		return indexes;
	case CatalogType::TABLE_FUNCTION_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY:
		return table_functions;
	case CatalogType::COPY_FUNCTION_ENTRY:
		return copy_functions;
	case CatalogType::PRAGMA_FUNCTION_ENTRY:
		return pragma_functions;
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
	case CatalogType::SCALAR_FUNCTION_ENTRY:
	case CatalogType::MACRO_ENTRY:
		return functions;
	case CatalogType::SEQUENCE_ENTRY:
		return sequences;
	case CatalogType::COLLATION_ENTRY:
		return collations;
	case CatalogType::TYPE_ENTRY:
		return types;
	default:
		throw InternalException("Unsupported catalog type in schema");
	}
}

} // namespace duckdb
