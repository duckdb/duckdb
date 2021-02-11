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
#include "duckdb/parser/parsed_data/sample_options.hpp"

namespace duckdb {

class BoundTableRef {
public:
	explicit BoundTableRef(TableReferenceType type) : type(type) {
	}
	virtual ~BoundTableRef() {
	}

	//! The type of table reference
	TableReferenceType type;
	//! The sample options (if any)
	unique_ptr<SampleOptions> sample;
};
} // namespace duckdb
