//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_column_data_ref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {
//! Represents a TableReference to a base table in the schema
class BoundColumnDataRef : public BoundTableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::COLUMN_DATA;

public:
	BoundColumnDataRef() : BoundTableRef(TableReferenceType::COLUMN_DATA) {
	}
	//! The materialized column data to scan
	unique_ptr<ColumnDataCollection> collection;
	//! The names associated with the columns
	vector<string> names;
	//! The index in the bind context
	idx_t bind_index;
};
} // namespace duckdb
