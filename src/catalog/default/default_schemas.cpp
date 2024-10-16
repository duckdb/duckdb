#include "duckdb/catalog/default/default_schemas.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

struct DefaultSchema {
	const char *name;
};

static const DefaultSchema internal_schemas[] = {{"information_schema"}, {"pg_catalog"}, {nullptr}};

bool DefaultSchemaGenerator::IsDefaultSchema(const string &input_schema) {
	auto schema = StringUtil::Lower(input_schema);
	for (idx_t index = 0; internal_schemas[index].name != nullptr; index++) {
		if (internal_schemas[index].name == schema) {
			return true;
		}
	}
	return false;
}

DefaultSchemaGenerator::DefaultSchemaGenerator(Catalog &catalog) : DefaultGenerator(catalog) {
}

unique_ptr<CatalogEntry> DefaultSchemaGenerator::CreateDefaultEntry(CatalogTransaction transaction,
                                                                    const string &entry_name) {
	if (IsDefaultSchema(entry_name)) {
		CreateSchemaInfo info;
		info.schema = StringUtil::Lower(entry_name);
		info.internal = true;
		return make_uniq_base<CatalogEntry, DuckSchemaEntry>(catalog, info);
	}
	return nullptr;
}

vector<string> DefaultSchemaGenerator::GetDefaultEntries() {
	vector<string> result;
	for (idx_t index = 0; internal_schemas[index].name != nullptr; index++) {
		result.emplace_back(internal_schemas[index].name);
	}
	return result;
}

} // namespace duckdb
