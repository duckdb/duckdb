#pragma once
#include "duck_table_entry.hpp"

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

private:
	//! A reference to the underlying storage unit used for this table
	shared_ptr<DataTable> storage;
};
} // namespace duckdb
