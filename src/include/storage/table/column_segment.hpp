//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/table/column_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/table/segment_base.hpp"

namespace duckdb {

enum class ColumnSegmentType : uint8_t {
	TRANSIENT = 1,
	PERSISTENT = 2
};

class ColumnSegment : public SegmentBase {
public:
	ColumnSegment(ColumnSegmentType type, index_t start, index_t count);

	virtual ~ColumnSegment() = default;

	ColumnSegmentType type;
};

} // namespace duckdb
