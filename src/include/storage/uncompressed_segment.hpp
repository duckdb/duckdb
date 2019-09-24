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
struct VersionChain;

class UncompressedSegment {
public:
	typedef void (*append_function_t)(SegmentStatistics &stats, data_ptr_t target, index_t target_offset, Vector &source, index_t offset, index_t count);
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
	unique_ptr<VersionChain*[]> versions;
	//! The lock for the uncompressed segment
	StorageLock lock;
	//! Whether or not this uncompressed segment is dirty
	bool dirty;
public:
	//! Initialize a scan state, obtaining the necessary locks to fetch data from this uncompressed segment
	void InitializeScan(UncompressedSegmentScanState &state);
	//! Fetch the vector at index "vector_index" from the uncompressed segment, storing it in the result vector
	void Scan(Transaction &transaction, UncompressedSegmentScanState &state, index_t vector_index, Vector &result);

	//! Append a part of a vector to the uncompressed segment with the given append state, updating the provided stats in the process. Returns the amount of tuples appended. If this is less than `count`, the uncompressed segment is full.
	index_t Append(SegmentStatistics &stats, TransientAppendState &state, Vector &data, index_t offset, index_t count);

	//! Update a set of row identifiers to the specified set of updated values
	void Update(Transaction &transaction, Vector &update, Vector &row_ids);
private:
	append_function_t append_function;
};

} // namespace duckdb
