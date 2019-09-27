//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/uncompressed_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/table/column_segment.hpp"
#include "storage/block.hpp"
#include "storage/storage_lock.hpp"
#include "storage/table/scan_state.hpp"

namespace duckdb {
class BufferManager;
class Transaction;

struct TransientAppendState;
struct UpdateInfo;

class UncompressedSegment {
public:
	//! Initialize a transient uncompressed segment, vector_count will be automatically chosen based on the type of the segment
	UncompressedSegment(BufferManager &manager, TypeId type);

	//! The buffer manager
	BufferManager &manager;
	//! Type of the uncompressed segment
	TypeId type;
	//! The size of this type
	index_t type_size;
	//! The size of a vector of this type
	index_t vector_size;
	//! The block id that this segment relates to
	block_id_t block_id;
	//! The maximum amount of vectors that can be stored in this segment
	index_t max_vector_count;
	//! The current amount of tuples that are stored in this segment
	index_t tuple_count;
	//! Version chains for each of the vectors
	unique_ptr<UpdateInfo*[]> versions;
	//! The lock for the uncompressed segment
	StorageLock lock;
	//! Whether or not this uncompressed segment is dirty
	bool dirty;
public:
	//! Fetch the vector at index "vector_index" from the uncompressed segment, storing it in the result vector
	void Scan(Transaction &transaction, index_t vector_index, Vector &result);
	//! Fetch the vector at index "vector_index" from the uncompressed segment, throwing an exception if there are any outstanding updates
	void IndexScan(UncompressedSegmentScanState &state, index_t vector_index, Vector &result);

	//! Fetch a single vector from the base table
	void Fetch(index_t vector_index, Vector &result);
	//! Fetch a single value and append it to the vector
	void Fetch(Transaction &transaction, row_t row_id, Vector &result);

	//! Append a part of a vector to the uncompressed segment with the given append state, updating the provided stats in the process. Returns the amount of tuples appended. If this is less than `count`, the uncompressed segment is full.
	index_t Append(SegmentStatistics &stats, TransientAppendState &state, Vector &data, index_t offset, index_t count);

	//! Update a set of row identifiers to the specified set of updated values
	void Update(SegmentStatistics &stats, Transaction &transaction, Vector &update, row_t *ids, row_t offset);

	//! Rollback a previous update
	void RollbackUpdate(UpdateInfo *info);
	//! Cleanup an update, removing it from the version chain. This should only be called if an exclusive lock is held on the segment
	void CleanupUpdate(UpdateInfo *info);
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

} // namespace duckdb
