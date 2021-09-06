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

struct DataPointer {
	uint64_t row_start;
	uint64_t tuple_count;
	BlockPointer block_pointer;
	CompressionType compression_type;
	//! Type-specific statistics of the segment
	unique_ptr<BaseStatistics> statistics;
};

struct RowGroupPointer {
	uint64_t row_start;
	uint64_t tuple_count;
	//! The data pointers of the column segments stored in the row group
	vector<BlockPointer> data_pointers;
	//! The per-column statistics of the row group
	vector<unique_ptr<BaseStatistics>> statistics;
	//! The versions information of the row group (if any)
	shared_ptr<VersionNode> versions;
};

} // namespace duckdb
