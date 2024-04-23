//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/column_data_ref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {
//! Represents a TableReference to a base table in the schema
class ColumnDataRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::COLUMN_DATA;

public:
	ColumnDataRef() : TableRef(TableReferenceType::COLUMN_DATA) {
	}

public:
	//! Expected SQL types
	vector<LogicalType> expected_types;
	//! The set of expected names
	vector<string> expected_names;

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Deserializes a blob back into a ExpressionListRef
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};
} // namespace duckdb
