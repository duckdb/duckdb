//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/copy_batch_slicer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"

namespace duckdb {

class ColumnDataCollection;
struct TupleDataChunkState;

//! Sizes and slices chunks for byte-limited COPY batches.
class CopyBatchSlicer {
public:
	CopyBatchSlicer(const vector<LogicalType> &types, const optional_idx &batch_size_bytes);
	~CopyBatchSlicer();

public:
	//! Computes row sizes, including fixed-width values and variable payloads.
	const Vector &ComputeRowSizes(DataChunk &chunk);
	//! Returns the next slice and advances offset, reusing row sizes until the chunk is exhausted.
	DataChunk &Slice(DataChunk &chunk, idx_t &offset, const ColumnDataCollection &batch);

private:
	idx_t RowsThatFit(idx_t count, idx_t offset, idx_t budget) const;

private:
	const optional_idx batch_size_bytes;
	TupleDataLayout layout;
	idx_t fixed_row_bytes = 0;
	unique_ptr<TupleDataChunkState> row_bytes_state;
	DataChunk partial;
};

} // namespace duckdb
