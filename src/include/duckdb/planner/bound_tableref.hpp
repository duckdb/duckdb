//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bound_tableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/tableref_type.hpp"

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
