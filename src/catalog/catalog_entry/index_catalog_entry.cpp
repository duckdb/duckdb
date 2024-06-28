#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"

namespace duckdb {

IndexCatalogEntry::IndexCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info)
    : StandardEntry(CatalogType::INDEX_ENTRY, schema, catalog, std::move(info.index_name)), sql(std::move(info.sql)),
      options(std::move(info.options)), index_type(info.index_type), index_constraint_type(info.constraint_type),
      column_ids(std::move(info.column_ids)) {

	this->temporary = info.temporary;
	this->dependencies = std::move(info.dependencies);
	this->comment = std::move(info.comment);
	for (auto &expr : expressions) {
		expressions.push_back(std::move(expr));
	}
	for (auto &parsed_expr : info.parsed_expressions) {
		parsed_expressions.push_back(std::move(parsed_expr));
	}
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
	result->dependencies = dependencies;

	for (auto &expr : expressions) {
		result->expressions.push_back(expr->Copy());
	}
	for (auto &expr : parsed_expressions) {
		result->parsed_expressions.push_back(expr->Copy());
	}

	result->comment = comment;
	result->tags = tags;

	return std::move(result);
}

string IndexCatalogEntry::ToSQL() const {
	auto info = GetInfo();
	return info->ToString();
}

bool IndexCatalogEntry::IsUnique() const {
	return (index_constraint_type == IndexConstraintType::UNIQUE ||
	        index_constraint_type == IndexConstraintType::PRIMARY);
}

bool IndexCatalogEntry::IsPrimary() const {
	return (index_constraint_type == IndexConstraintType::PRIMARY);
}

} // namespace duckdb
