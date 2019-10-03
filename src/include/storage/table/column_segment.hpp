//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/table/column_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/block.hpp"
#include "storage/table/segment_tree.hpp"
#include "common/types.hpp"
#include "storage/buffer_manager.hpp"

namespace duckdb {
class BlockManager;
class ColumnSegment;

enum class ColumnSegmentType : uint8_t { TRANSIENT, PERSISTENT };

//! The ColumnPointer is a class used for scanning ColumnSegments
struct ColumnPointer {
	//! The column segment
	ColumnSegment *segment;
	//! The offset inside the column segment
	index_t offset;
};

struct SegmentStatistics {
	SegmentStatistics(TypeId type, index_t type_size);

	//! The minimum value of the segment
	unique_ptr<data_t[]> minimum;
	//! The maximum value of the segment
	unique_ptr<data_t[]> maximum;
	//! Whether or not the segment has NULL values
	bool has_null;
	//! The maximum string length, only used for string columns
	index_t max_string_length;
};

class ColumnSegment : public SegmentBase {
public:
	//! Initialize an empty column segment of the specified type
	ColumnSegment(TypeId type, ColumnSegmentType segment_type, index_t start, index_t count = 0);
	virtual ~ColumnSegment() = default;

	//! The type stored in the column
	TypeId type;
	//! The size of the type
	index_t type_size;
	//! The column segment type (transient or persistent)
	ColumnSegmentType segment_type;
	//! The statistics for the segment
	SegmentStatistics stats;
};

} // namespace duckdb
