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
	BoundDelimGetRef(idx_t bind_index, const vector<LogicalType> &column_types_p)
	    : BoundTableRef(TableReferenceType::DELIM_GET), bind_index(bind_index), column_types(column_types_p) {
	}
	idx_t bind_index;
	vector<LogicalType> column_types;
};
} // namespace duckdb
