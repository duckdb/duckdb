//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/column_segment_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/segment_tree.hpp"
#include "duckdb/storage/table/column_segment.hpp"

namespace duckdb {

class ColumnSegmentTree : public SegmentTree<ColumnSegment> {
public:
	void SetReadOnly(bool read_only) {
		this->is_read_only = read_only;
	}
	bool GetReadOnly() {
		return this->is_read_only;
	}
};

} // namespace duckdb
