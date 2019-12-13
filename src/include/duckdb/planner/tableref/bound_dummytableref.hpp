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
	BoundEmptyTableRef(index_t bind_index) : BoundTableRef(TableReferenceType::EMPTY), bind_index(bind_index) {
	}
	index_t bind_index;
};
} // namespace duckdb
