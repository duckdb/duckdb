//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_delimgetref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_tableref.hpp"

namespace duckdb {

class BoundDelimGetRef : public BoundTableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::DELIM_GET;

public:
	explicit BoundDelimGetRef(idx_t bind_index) : BoundTableRef(TableReferenceType::DELIM_GET), bind_index(bind_index) {
	}
	idx_t bind_index;
};
} // namespace duckdb
