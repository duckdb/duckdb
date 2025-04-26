#include "duckdb/catalog/catalog_entry/matview_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include <sstream>

namespace duckdb {

MatViewCatalogEntry::MatViewCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, BoundCreateTableInfo &info,
                                         shared_ptr<DataTable> inherited_storage)
    : DuckTableEntry(catalog, schema, info, std::move(inherited_storage)), query(std::move(info.query)) {
	this->type = CatalogType::MATVIEW_ENTRY;
}
} // namespace duckdb
