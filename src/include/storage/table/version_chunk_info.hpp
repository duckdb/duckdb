//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/table/version_chunk_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/string_heap.hpp"
#include "common/enums/undo_flags.hpp"
#include "storage/storage_lock.hpp"
#include "storage/table/segment_tree.hpp"
#include "storage/table/column_segment.hpp"

namespace duckdb {

class VersionChunk;

class VersionChunkInfo {
public:
	VersionChunkInfo(VersionChunk &chunk, index_t start) : chunk(chunk), start(start) {
		for(index_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			deleted[i] = NOT_DELETED_ID;
			inserted[i] = 0;
		}
	}

	//! The transaction ids of the transactions that deleted the tuples (if any)
	transaction_t deleted[STANDARD_VECTOR_SIZE];
	//! The transaction ids of the transactions that inserted the tuples (if any)
	transaction_t inserted[STANDARD_VECTOR_SIZE];
	//! The chunk this info belongs to
	VersionChunk &chunk;
	//! The start index
	index_t start;
};

} // namespace duckdb
