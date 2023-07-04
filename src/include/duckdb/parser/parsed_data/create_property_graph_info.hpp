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
	explicit CreatePropertyGraphInfo(string property_graph_name);

	//! Property graph name
	string property_graph_name;
	//! List of vector tables
	vector<unique_ptr<PropertyGraphTable>> vertex_tables;

	vector<unique_ptr<PropertyGraphTable>> edge_tables;

	//! Dictionary to point label to vector or edge table
	case_insensitive_map_t<PropertyGraphTable *> label_map;

public:
	//        string ToString() const;
	//        bool Equals(const BaseExpression *other_p) const;
	//	void SerializeInternal(Serializer &serializer) const override;
	unique_ptr<CreateInfo> Copy() const override;

	//! Serializes a blob into a CreatePropertyGraphInfo
	void SerializeInternal(Serializer &serializer) const override;
	//! Deserializes a blob back into a CreatePropertyGraphInfo
	static unique_ptr<CreateInfo> Deserialize(FieldReader &reader);
};
} // namespace duckdb
