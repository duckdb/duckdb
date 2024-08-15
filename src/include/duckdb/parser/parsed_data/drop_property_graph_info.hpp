//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/drop_property_graph_info.hpp
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
#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {

struct DropPropertyGraphInfo : public DropInfo {
	DropPropertyGraphInfo();
	explicit DropPropertyGraphInfo(string property_graph_name, bool missing_ok);

	//! Property graph name
	string property_graph_name;
	bool missing_ok;

public:
	unique_ptr<DropInfo> Copy() const override;

	string ToString() const override;

	//! Serializes a blob into a CreatePropertyGraphInfo
	void Serialize(Serializer &serializer) const override;
	//! Deserializes a blob back into a CreatePropertyGraphInfo
	static unique_ptr<DropInfo> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
