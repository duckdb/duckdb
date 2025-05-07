#pragma once
#include "duck_table_entry.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class DataTable;

//! A Materialized View catalog entry
class MaterializedViewCatalogEntry : public DuckTableEntry {
public:
	MaterializedViewCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, BoundCreateTableInfo &info,
	                             shared_ptr<DataTable> inherited_storage = nullptr);

	static constexpr const CatalogType Type = CatalogType::MATERIALIZED_VIEW_ENTRY;
	static constexpr const char *Name = "materialized view";

public:
	unique_ptr<LogicalOperator> query;
};
} // namespace duckdb
