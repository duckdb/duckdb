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

class DataTable;
class Transaction;
class VersionChunkInfo;

struct DeleteInfo {
	VersionChunkInfo *vinfo;
	index_t row_id;

	DataTable &GetTable();
	index_t GetRowId();
};

struct Versioning {
	//! Returns true if the specified version number has a conflict with the specified transaction id
	static bool HasConflict(transaction_t version_number, transaction_t transaction_id);

	//! Returns true if the version number should be used in the specified transaction
	static bool UseVersion(Transaction &transaction, transaction_t id);
};

} // namespace duckdb
