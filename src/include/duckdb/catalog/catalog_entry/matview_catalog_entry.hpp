#pragma once
#include "duck_table_entry.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class DataTable;
struct CreateMatViewInfo;

//! A Materialized View catalog entry
class MatViewCatalogEntry : public DuckTableEntry {
public:
	MatViewCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, BoundCreateTableInfo &info,
	                    shared_ptr<DataTable> inherited_storage = nullptr);

	static constexpr const CatalogType Type = CatalogType::MATVIEW_ENTRY;
	static constexpr const char *Name = "matview";

public:
	unique_ptr<LogicalOperator> query;
};
} // namespace duckdb
