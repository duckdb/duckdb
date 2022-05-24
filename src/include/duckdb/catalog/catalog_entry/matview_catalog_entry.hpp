#pragma once
#include "duckdb/catalog/catalog_entry/matview_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_matview_info.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

class DataTable;
struct BoundCreateTableInfo;

//! A Materialized View catalog entry
class MatViewCatalogEntry : public TableCatalogEntry {
public:
	//! Create a real TableCatalogEntry and initialize storage for it
	MatViewCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, BoundCreateTableInfo *info,
	                    std::shared_ptr<DataTable> inherited_storage = nullptr);

    //! Serialize the meta information of the ViewCatalogEntry a serializer
    virtual void Serialize(Serializer &serializer);
    //! Deserializes to a CreateTableInfo
    static unique_ptr<CreateMatViewInfo> Deserialize(Deserializer &source);

	string ToSQL();

private:
    //! The SQL query (if any)
    string sql;
};
} // namespace duckdb
