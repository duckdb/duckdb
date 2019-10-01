//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/numeric_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/uncompressed_segment.hpp"

namespace duckdb {

class NumericSegment : public UncompressedSegment {
public:
	NumericSegment(BufferManager &manager, TypeId type);

	//! The size of this type
	index_t type_size;
public:
	//! Fetch the vector at index "vector_index" from the uncompressed segment, storing it in the result vector
	void Scan(Transaction &transaction, TransientScanState &state, index_t vector_index, Vector &result) override;
	//! Fetch the vector at index "vector_index" from the uncompressed segment, throwing an exception if there are any outstanding updates
	void IndexScan(TransientScanState &state, index_t vector_index, Vector &result) override;

	//! Fetch a single vector from the base table
	void Fetch(index_t vector_index, Vector &result) override;
	//! Fetch a single value and append it to the vector
	void Fetch(Transaction &transaction, row_t row_id, Vector &result) override;

	//! Append a part of a vector to the uncompressed segment with the given append state, updating the provided stats in the process. Returns the amount of tuples appended. If this is less than `count`, the uncompressed segment is full.
	index_t Append(SegmentStatistics &stats, TransientAppendState &state, Vector &data, index_t offset, index_t count) override;

	//! Rollback a previous update
	void RollbackUpdate(UpdateInfo *info) override;
	//! Cleanup an update, removing it from the version chain. This should only be called if an exclusive lock is held on the segment
	void CleanupUpdate(UpdateInfo *info) override;
protected:
	void Update(SegmentStatistics &stats, Transaction &transaction, Vector &update, row_t *ids, index_t vector_index, index_t vector_offset, UpdateInfo *node) override;
public:
	typedef void (*append_function_t)(SegmentStatistics &stats, data_ptr_t target, index_t target_offset, Vector &source, index_t offset, index_t count);
	typedef void (*update_function_t)(SegmentStatistics &stats, UpdateInfo *info, data_ptr_t base_data, Vector &update);
	typedef void (*update_info_fetch_function_t)(Transaction &transaction, UpdateInfo *info, Vector &result, index_t count);
	typedef void (*update_info_append_function_t)(Transaction &transaction, UpdateInfo *info, Vector &result, row_t row_id);
	typedef void (*rollback_update_function_t)(UpdateInfo *info, data_ptr_t base_data);
	typedef void (*merge_update_function_t)(SegmentStatistics &stats, UpdateInfo *node, data_ptr_t target, Vector &update, row_t *ids, index_t vector_offset);
private:
	append_function_t append_function;
	update_function_t update_function;
	update_info_fetch_function_t fetch_from_update_info;
	update_info_append_function_t append_from_update_info;
	rollback_update_function_t rollback_update;
	merge_update_function_t merge_update_function;
};

}
