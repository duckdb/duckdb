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
	if (entry_name != ART::TYPE_NAME) {
		return nullptr;
	}
	CreateIndexTypeInfo info(entry_name);
	info.internal = true;
	info.temporary = true;
	info.schema = DEFAULT_SCHEMA;
	auto result = make_uniq_base<CatalogEntry, IndexTypeCatalogEntry>(catalog, schema, info);

	auto &index_entry = result->Cast<IndexTypeCatalogEntry>();
	index_entry.create_instance = ART::Create;
	return result;
}

vector<string> DefaultIndexTypesGenerator::GetDefaultEntries() {
	vector<string> result;
	if (schema.name != DEFAULT_SCHEMA) {
		return result;
	}
	result.emplace_back(ART::TYPE_NAME);

	return result;
}

} // namespace duckdb
