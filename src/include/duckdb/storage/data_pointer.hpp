//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/data_pointer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/common/enums/compression_type.hpp"

namespace duckdb {

class Serializer;
class Deserializer;
class CompressionInfo;

struct ColumnSegmentState {
	virtual ~ColumnSegmentState() {
	}

	virtual void Serialize(Serializer &serializer) const = 0;
	static unique_ptr<ColumnSegmentState> Deserialize(Deserializer &deserializer);

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct DataPointer {
	explicit DataPointer(BaseStatistics stats) : statistics(std::move(stats)) {
	}

	uint64_t row_start;
	uint64_t tuple_count;
	BlockPointer block_pointer;
	CompressionType compression_type;
	//! Type-specific statistics of the segment
	BaseStatistics statistics;
	//! Serialized segment state
	unique_ptr<ColumnSegmentState> segment_state;

	void Serialize(Serializer &serializer) const;
	static DataPointer Deserialize(Deserializer &source);
};

struct RowGroupPointer {
	uint64_t row_start;
	uint64_t tuple_count;
	//! The data pointers of the column segments stored in the row group
	vector<MetaBlockPointer> data_pointers;
	//! Data pointers to the delete information of the row group (if any)
	vector<MetaBlockPointer> deletes_pointers;
};

} // namespace duckdb
