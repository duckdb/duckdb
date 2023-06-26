#include "duckdb/catalog/catalog_entry/duck_constraint_entry.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

DuckConstraintEntry::DuckConstraintEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateConstraintInfo &info)
    : ConstraintCatalogEntry(catalog, schema, info) {
}

DuckConstraintEntry::~DuckConstraintEntry() {
}

string DuckConstraintEntry::GetSchemaName() const {
	return "";
}

string DuckConstraintEntry::GetTableName() const {
    return "";
}

string DuckConstraintEntry::GetConstraintName() const {
	return "";
}

} // namespace duckdb

