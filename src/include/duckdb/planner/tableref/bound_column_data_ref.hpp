//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_column_data_ref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/common/optionally_owned_ptr.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {
//! Represents a TableReference to a base table in the schema
class BoundColumnDataRef : public BoundTableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::COLUMN_DATA;

public:
	explicit BoundColumnDataRef(optionally_owned_ptr<ColumnDataCollection> collection)
	    : BoundTableRef(TableReferenceType::COLUMN_DATA), collection(std::move(collection)) {
	}
	//! The (optionally owned) materialized column data to scan
	optionally_owned_ptr<ColumnDataCollection> collection;
	//! The index in the bind context
	idx_t bind_index;
};
} // namespace duckdb
