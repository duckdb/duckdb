//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/bound_tableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"

namespace duckdb {

class BoundTableRef {
public:
	BoundTableRef(TableReferenceType type) : type(type) {
	}
	virtual ~BoundTableRef() {
	}

	//! The type of table reference
	TableReferenceType type;
};
} // namespace duckdb
