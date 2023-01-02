#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/collate_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
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
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/catalog/default/default_views.hpp"
#include "duckdb/common/algorithm.hpp"
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
#include "duckdb/catalog/dependency_list.hpp"

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
			AlterEntryData alter_data(entry->catalog->GetName(), fk.info.schema, fk.info.table, false);
			fk_arrays.push_back(make_unique<AlterForeignKeyInfo>(std::move(alter_data), entry->name, fk.pk_columns,
			                                                     fk.fk_columns, fk.info.pk_keys, fk.info.fk_keys,
			                                                     alter_fk_type));
		} else if (fk.info.type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE &&
		           alter_fk_type == AlterForeignKeyType::AFT_DELETE) {
			throw CatalogException("Could not drop the table because this table is main key table of the table \"%s\"",
			                       fk.info.table);
		}
	}
}

SchemaCatalogEntry::SchemaCatalogEntry(Catalog *catalog, string name_p, bool internal)
    : CatalogEntry(CatalogType::SCHEMA_ENTRY, catalog, std::move(name_p)),
      tables(*catalog, make_unique<DefaultViewGenerator>(*catalog, this)), indexes(*catalog), table_functions(*catalog),
      copy_functions(*catalog), pragma_functions(*catalog),
      functions(*catalog, make_unique<DefaultFunctionGenerator>(*catalog, this)), sequences(*catalog),
      collations(*catalog), types(*catalog, make_unique<DefaultTypeGenerator>(*catalog, this)) {
	this->internal = internal;
}

CatalogTransaction SchemaCatalogEntry::GetCatalogTransaction(ClientContext &context) {
	return CatalogTransaction(*catalog, context);
}

CatalogEntry *SchemaCatalogEntry::AddEntry(CatalogTransaction transaction, unique_ptr<StandardEntry> entry,
                                           OnCreateConflict on_conflict, DependencyList dependencies) {
	auto entry_name = entry->name;
	auto entry_type = entry->type;
	auto result = entry.get();

	// first find the set for this entry
	auto &set = GetCatalogSet(entry_type);
	dependencies.AddDependency(this);
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// CREATE OR REPLACE: first try to drop the entry
		auto old_entry = set.GetEntry(transaction, entry_name);
		if (old_entry) {
			if (old_entry->type != entry_type) {
				throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", entry_name,
				                       CatalogTypeToString(old_entry->type), CatalogTypeToString(entry_type));
			}
			(void)set.DropEntry(transaction, entry_name, false, entry->internal);
		}
	}
	// now try to add the entry
	if (!set.CreateEntry(transaction, entry_name, std::move(entry), dependencies)) {
		// entry already exists!
		if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw CatalogException("%s with name \"%s\" already exists!", CatalogTypeToString(entry_type), entry_name);
		} else {
			return nullptr;
		}
	}
	return result;
}

CatalogEntry *SchemaCatalogEntry::AddEntry(CatalogTransaction transaction, unique_ptr<StandardEntry> entry,
                                           OnCreateConflict on_conflict) {
	DependencyList dependencies;
	return AddEntry(transaction, std::move(entry), on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo *info) {
	auto sequence = make_unique<SequenceCatalogEntry>(catalog, this, info);
	return AddEntry(transaction, std::move(sequence), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo *info) {
	auto type_entry = make_unique<TypeCatalogEntry>(catalog, this, info);
	return AddEntry(transaction, std::move(type_entry), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo *info) {
	auto table = make_unique<TableCatalogEntry>(catalog, this, info);
	table->storage->info->cardinality = table->storage->GetTotalRows();

	CatalogEntry *entry = AddEntry(transaction, std::move(table), info->Base().on_conflict, info->dependencies);
	if (!entry) {
		return nullptr;
	}

	// add a foreign key constraint in main key table if there is a foreign key constraint
	vector<unique_ptr<AlterForeignKeyInfo>> fk_arrays;
	FindForeignKeyInformation(entry, AlterForeignKeyType::AFT_ADD, fk_arrays);
	for (idx_t i = 0; i < fk_arrays.size(); i++) {
		// alter primary key table
		AlterForeignKeyInfo *fk_info = fk_arrays[i].get();
		catalog->Alter(transaction.GetContext(), fk_info);

		// make a dependency between this table and referenced table
		auto &set = GetCatalogSet(CatalogType::TABLE_ENTRY);
		info->dependencies.AddDependency(set.GetEntry(transaction, fk_info->name));
	}
	return entry;
}

CatalogEntry *SchemaCatalogEntry::CreateView(CatalogTransaction transaction, CreateViewInfo *info) {
	auto view = make_unique<ViewCatalogEntry>(catalog, this, info);
	return AddEntry(transaction, std::move(view), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateIndex(ClientContext &context, CreateIndexInfo *info, TableCatalogEntry *table) {
	DependencyList dependencies;
	dependencies.AddDependency(table);
	auto index = make_unique<IndexCatalogEntry>(catalog, this, info);
	return AddEntry(GetCatalogTransaction(context), std::move(index), info->on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo *info) {
	auto collation = make_unique<CollateCatalogEntry>(catalog, this, info);
	collation->internal = info->internal;
	return AddEntry(transaction, std::move(collation), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateTableFunction(CatalogTransaction transaction, CreateTableFunctionInfo *info) {
	auto table_function = make_unique<TableFunctionCatalogEntry>(catalog, this, info);
	table_function->internal = info->internal;
	return AddEntry(transaction, std::move(table_function), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo *info) {
	auto copy_function = make_unique<CopyFunctionCatalogEntry>(catalog, this, info);
	copy_function->internal = info->internal;
	return AddEntry(transaction, std::move(copy_function), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreatePragmaFunction(CatalogTransaction transaction, CreatePragmaFunctionInfo *info) {
	auto pragma_function = make_unique<PragmaFunctionCatalogEntry>(catalog, this, info);
	pragma_function->internal = info->internal;
	return AddEntry(transaction, std::move(pragma_function), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo *info) {
	if (info->on_conflict == OnCreateConflict::ALTER_ON_CONFLICT) {
		// check if the original entry exists
		auto &catalog_set = GetCatalogSet(info->type);
		auto current_entry = catalog_set.GetEntry(transaction, info->name);
		if (current_entry) {
			// the current entry exists - alter it instead
			auto alter_info = info->GetAlterInfo();
			Alter(transaction.GetContext(), alter_info.get());
			return nullptr;
		}
	}
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
		// create a macro table function
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
	function->internal = info->internal;
	return AddEntry(transaction, std::move(function), info->on_conflict);
}

void SchemaCatalogEntry::DropEntry(ClientContext &context, DropInfo *info) {
	auto &set = GetCatalogSet(info->type);

	// first find the entry
	auto transaction = GetCatalogTransaction(context);
	auto existing_entry = set.GetEntry(transaction, info->name);
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

	if (!set.DropEntry(transaction, info->name, info->cascade, info->allow_drop_internal)) {
		throw InternalException("Could not drop element because of an internal error");
	}

	// remove the foreign key constraint in main key table if main key table's name is valid
	for (idx_t i = 0; i < fk_arrays.size(); i++) {
		// alter primary key table
		catalog->Alter(context, fk_arrays[i].get());
	}
}

void SchemaCatalogEntry::Alter(ClientContext &context, AlterInfo *info) {
	CatalogType type = info->GetCatalogType();
	auto &set = GetCatalogSet(type);
	auto transaction = GetCatalogTransaction(context);
	if (info->type == AlterType::CHANGE_OWNERSHIP) {
		if (!set.AlterOwnership(transaction, (ChangeOwnershipInfo *)info)) {
			throw CatalogException("Couldn't change ownership!");
		}
	} else {
		string name = info->name;
		if (!set.AlterEntry(transaction, name, info)) {
			throw CatalogException("Entry with name \"%s\" does not exist!", name);
		}
	}
}

void SchemaCatalogEntry::Scan(ClientContext &context, CatalogType type,
                              const std::function<void(CatalogEntry *)> &callback) {
	auto &set = GetCatalogSet(type);
	set.Scan(GetCatalogTransaction(context), callback);
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

void SchemaCatalogEntry::Verify(Catalog &catalog) {
	CatalogEntry::Verify(catalog);

	tables.Verify(catalog);
	indexes.Verify(catalog);
	table_functions.Verify(catalog);
	copy_functions.Verify(catalog);
	pragma_functions.Verify(catalog);
	functions.Verify(catalog);
	sequences.Verify(catalog);
	collations.Verify(catalog);
	types.Verify(catalog);
}

} // namespace duckdb
