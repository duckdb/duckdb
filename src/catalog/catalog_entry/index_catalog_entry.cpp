#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {

IndexCatalogEntry::IndexCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info)
    : StandardEntry(CatalogType::INDEX_ENTRY, schema, catalog, info.index_name), index(nullptr), sql(info.sql) {
	this->temporary = info.temporary;
}

string IndexCatalogEntry::ToSQL() const {
	if (sql.empty()) {
		return sql;
	}
	if (sql[sql.size() - 1] != ';') {
		return sql + ";";
	}
	return sql;
}

unique_ptr<CreateInfo> IndexCatalogEntry::GetInfo() const {
	auto result = make_uniq<CreateIndexInfo>();
	result->schema = GetSchemaName();
	result->table = GetTableName();
	result->index_name = name;
	result->sql = sql;
	result->index_type = index->type;
	result->constraint_type = index->constraint_type;
	for (auto &expr : expressions) {
		result->expressions.push_back(expr->Copy());
	}
	for (auto &expr : parsed_expressions) {
		result->parsed_expressions.push_back(expr->Copy());
	}
	result->column_ids = index->column_ids;
	result->temporary = temporary;
	return std::move(result);
}

} // namespace duckdb
