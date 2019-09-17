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
class VersionChunkInfo;

struct DeleteInfo {
	ChunkInfo *vinfo;
	index_t count;
	row_t rows[1];

	DataTable &GetTable();
};

struct Versioning {
	//! Returns true if the specified version number has a conflict with the specified transaction id
	static bool HasConflict(transaction_t version_number, transaction_t transaction_id);

	//! Returns true if the version number should be used in the specified transaction
	static bool UseVersion(Transaction &transaction, transaction_t id);
};

} // namespace duckdb
