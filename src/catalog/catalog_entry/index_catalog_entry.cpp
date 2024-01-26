#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"

namespace duckdb {

IndexCatalogEntry::IndexCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info)
    : StandardEntry(CatalogType::INDEX_ENTRY, schema, catalog, info.index_name), sql(info.sql), options(info.options),
      index_type(info.index_type), index_constraint_type(info.constraint_type), column_ids(info.column_ids) {

	this->temporary = info.temporary;
	this->comment = info.comment;
}

unique_ptr<CreateInfo> IndexCatalogEntry::GetInfo() const {
	auto result = make_uniq<CreateIndexInfo>();
	result->schema = GetSchemaName();
	result->table = GetTableName();

	result->temporary = temporary;
	result->sql = sql;
	result->index_name = name;
	result->index_type = index_type;
	result->constraint_type = index_constraint_type;
	result->column_ids = column_ids;

	for (auto &expr : expressions) {
		result->expressions.push_back(expr->Copy());
	}
	for (auto &expr : parsed_expressions) {
		result->parsed_expressions.push_back(expr->Copy());
	}

	result->comment = comment;

	return std::move(result);
}

string IndexCatalogEntry::ToSQL() const {
	if (sql.empty()) {
		return sql;
	}
	if (sql.back() != ';') {
		return sql + ";";
	}
	return sql;
}

bool IndexCatalogEntry::IsUnique() {
	return (index_constraint_type == IndexConstraintType::UNIQUE ||
	        index_constraint_type == IndexConstraintType::PRIMARY);
}

bool IndexCatalogEntry::IsPrimary() {
	return (index_constraint_type == IndexConstraintType::PRIMARY);
}

} // namespace duckdb
