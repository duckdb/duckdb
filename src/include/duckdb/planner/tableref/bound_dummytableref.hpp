//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_dummytableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_tableref.hpp"

namespace duckdb {

//! Represents a cross product
class BoundEmptyTableRef : public BoundTableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::EMPTY_FROM;

public:
	explicit BoundEmptyTableRef(idx_t bind_index)
	    : BoundTableRef(TableReferenceType::EMPTY_FROM), bind_index(bind_index) {
	}
	idx_t bind_index;
};
} // namespace duckdb
