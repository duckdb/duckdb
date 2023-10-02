#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CreateTableInfo::CreateTableInfo() : CreateInfo(CatalogType::TABLE_ENTRY, INVALID_SCHEMA) {
}

CreateTableInfo::CreateTableInfo(string catalog_p, string schema_p, string name_p)
    : CreateInfo(CatalogType::TABLE_ENTRY, std::move(schema_p), std::move(catalog_p)), table(std::move(name_p)) {
}

CreateTableInfo::CreateTableInfo(SchemaCatalogEntry &schema, string name_p)
    : CreateTableInfo(schema.catalog.GetName(), schema.name, std::move(name_p)) {
}

unique_ptr<CreateInfo> CreateTableInfo::Copy() const {
	auto result = make_uniq<CreateTableInfo>(catalog, schema, table);
	CopyProperties(*result);
	result->columns = columns.Copy();
	for (auto &constraint : constraints) {
		result->constraints.push_back(constraint->Copy());
	}
	if (query) {
		result->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
	}
	return std::move(result);
}

} // namespace duckdb
