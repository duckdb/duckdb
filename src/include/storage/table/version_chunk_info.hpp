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
struct VersionInfo;

class VersionChunkInfo {
public:
	VersionChunkInfo(VersionChunk &chunk, index_t start);

	//! The transaction ids of the transactions that deleted the tuples (if any)
	transaction_t deleted[STANDARD_VECTOR_SIZE];
	//! The transaction ids of the transactions that inserted the tuples (if any)
	transaction_t inserted[STANDARD_VECTOR_SIZE];
	//! The version pointers
	VersionInfo *version_pointers[STANDARD_VECTOR_SIZE];
	//! The chunk this info belongs to
	VersionChunk &chunk;
	//! The start index
	index_t start;

public:
	// Cleanup the version information of a tuple
	void Cleanup(VersionInfo *info);
	// Undo the changes made by a tuple
	void Undo(VersionInfo *info);
};

} // namespace duckdb
