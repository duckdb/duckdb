#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/collate_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"

#include <sstream>

namespace duckdb {

SchemaCatalogEntry::SchemaCatalogEntry(Catalog *catalog, string name_p, bool internal)
    : CatalogEntry(CatalogType::SCHEMA_ENTRY, catalog, std::move(name_p)) {
	this->internal = internal;
}

CatalogTransaction SchemaCatalogEntry::GetCatalogTransaction(ClientContext &context) {
	return CatalogTransaction(*catalog, context);
}

CatalogEntry *SchemaCatalogEntry::AddEntry(CatalogTransaction transaction, unique_ptr<StandardEntry> entry,
                                           OnCreateConflict on_conflict) {
	DependencyList dependencies;
	return AddEntryInternal(transaction, std::move(entry), on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::AddEntryInternal(CatalogTransaction transaction, unique_ptr<StandardEntry> entry,
                                                   OnCreateConflict on_conflict, DependencyList dependencies) {
	throw InternalException("AddEntryInternal called on non-DuckDB SchemaCatalogEntry");
}

CatalogEntry *SchemaCatalogEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo *info) {
	auto sequence = make_unique<SequenceCatalogEntry>(catalog, this, info);
	return AddEntry(transaction, std::move(sequence), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo *info) {
	auto type_entry = make_unique<TypeCatalogEntry>(catalog, this, info);
	return AddEntry(transaction, std::move(type_entry), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateView(CatalogTransaction transaction, CreateViewInfo *info) {
	auto view = make_unique<ViewCatalogEntry>(catalog, this, info);
	return AddEntry(transaction, std::move(view), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateIndex(ClientContext &context, CreateIndexInfo *info, TableCatalogEntry *table) {
	DependencyList dependencies;
	dependencies.AddDependency(table);
	auto index = make_unique<IndexCatalogEntry>(catalog, this, info);
	return AddEntryInternal(GetCatalogTransaction(context), std::move(index), info->on_conflict, dependencies);
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

SimilarCatalogEntry SchemaCatalogEntry::GetSimilarEntry(CatalogTransaction transaction, CatalogType type,
                                                        const string &name) {
	SimilarCatalogEntry result;
	Scan(transaction.GetContext(), type, [&](CatalogEntry *entry) {
		auto ldist = StringUtil::LevenshteinDistance(entry->name, name);
		if (ldist < result.distance) {
			result.distance = ldist;
			result.name = entry->name;
		}
	});
	return result;
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

} // namespace duckdb
