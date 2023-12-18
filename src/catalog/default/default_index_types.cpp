#include "duckdb/catalog/default/default_index_types.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/index_type_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/create_index_type_info.hpp"

#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

DefaultIndexTypesGenerator::DefaultIndexTypesGenerator(Catalog &catalog, SchemaCatalogEntry &schema)
    : DefaultGenerator(catalog), schema(schema) {
}

unique_ptr<CatalogEntry> DefaultIndexTypesGenerator::CreateDefaultEntry(ClientContext &context,
                                                                        const string &entry_name) {
	if (schema.name != DEFAULT_SCHEMA) {
		return nullptr;
	}
	if (entry_name != "ART") {
		return nullptr;
	}
	CreateIndexTypeInfo info(entry_name);
	info.internal = true;
	info.temporary = true;
	info.schema = DEFAULT_SCHEMA;
	auto result = make_uniq_base<CatalogEntry, IndexTypeCatalogEntry>(catalog, schema, info);

	auto &index_entry = result->Cast<IndexTypeCatalogEntry>();
	index_entry.create_instance =
	    [](const string &name, const IndexConstraintType constraint_type, const vector<column_t> &column_ids,
	       const vector<unique_ptr<Expression>> &unbound_expressions, TableIOManager &table_io_manager,
	       AttachedDatabase &db, const IndexStorageInfo &storage_info) -> unique_ptr<Index> {
		auto art = make_uniq<ART>(name, constraint_type, column_ids, table_io_manager, unbound_expressions, db, nullptr,
		                          storage_info);
		return std::move(art);
	};

	return result;
}

vector<string> DefaultIndexTypesGenerator::GetDefaultEntries() {
	vector<string> result;
	if (schema.name != DEFAULT_SCHEMA) {
		return result;
	}

	// TODO: Change the DEFAULT_INDEX_TYPE in the parser too?
	result.emplace_back("ART");

	return result;
}

} // namespace duckdb
