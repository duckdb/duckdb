//===----------------------------------------------------------------------===//
//                         DuckDB
//
// transaction/version_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

class ChunkInfo;
class DataTable;
class Transaction;
class UncompressedSegment;
class VersionChunkInfo;

struct DeleteInfo {
	ChunkInfo *vinfo;
	index_t count;
	row_t rows[1];

	DataTable &GetTable();
};

struct UpdateInfo {
	//! The uncompressed segment that this update info affects
	UncompressedSegment *segment;
	//! The version number
	transaction_t version_number;
	//! The vector index within the uncompressed segment
	index_t vector_index;
	//! The amount of updated tuples
	sel_t N;
	//! The maximum amount of tuples that can fit into this UpdateInfo
	sel_t max;
	//! The row ids of the tuples that have been updated. This should always be kept sorted!
	sel_t *tuples;
	//! The nullmask of the tuples
	nullmask_t nullmask;
	//! The data of the tuples
	data_ptr_t tuple_data;
	//! The previous update info (or nullptr if it is the base)
	UpdateInfo *prev;
	//! The next update info in the chain (or nullptr if it is the last)
	UpdateInfo *next;
};

struct Versioning {
	//! Returns true if the specified version number has a conflict with the specified transaction id
	static bool HasConflict(transaction_t version_number, transaction_t transaction_id);

	//! Returns true if the version number should be used in the specified transaction
	static bool UseVersion(Transaction &transaction, transaction_t id);
};

} // namespace duckdb
