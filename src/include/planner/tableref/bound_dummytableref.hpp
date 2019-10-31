//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/tableref/bound_dummytableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/bound_tableref.hpp"

namespace duckdb {

//! Represents a cross product
class BoundDummyTableRef : public BoundTableRef {
public:
	BoundDummyTableRef(index_t bind_index) : BoundTableRef(TableReferenceType::DUMMY), bind_index(bind_index) {
	}
	index_t bind_index;
};
} // namespace duckdb
