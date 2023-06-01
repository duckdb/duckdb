//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_property_graph_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/column_dependency_manager.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/property_graph_table.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/catalog/catalog_entry.hpp"

namespace duckdb {

    struct CreatePropertyGraphInfo : public CreateInfo {
        CreatePropertyGraphInfo();
        CreatePropertyGraphInfo(string catalog, string schema, string name);
        //	explicit CreatePropertyGraphInfo(string property_graph_name);

        CreatePropertyGraphInfo(SchemaCatalogEntry *schema, string pg_name);

        //! Property graph name
        string property_graph_name;
        //! List of vector tables
        vector<shared_ptr<PropertyGraphTable>> vertex_tables;

        vector<shared_ptr<PropertyGraphTable>> edge_tables;

        //! Dictionary to point label to vector or edge table
        case_insensitive_map_t<shared_ptr<PropertyGraphTable>> label_map;

    protected:
        void SerializeInternal(Serializer &serializer) const override;

    public:
        //	DUCKDB_API static unique_ptr<CreatePropertyGraphInfo> Deserialize(Deserializer &deserializer);

        DUCKDB_API unique_ptr<CreateInfo> Copy() const override;
    };
} // namespace duckdb
