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

struct VersionInfo {
	VersionChunkInfo *vinfo;
	index_t entry;
	VersionInfo *prev;
	VersionInfo *next;
	transaction_t version_number;
	data_ptr_t tuple_data;

	DataTable &GetTable();
	index_t GetRowId();

	//! Given a specific version info, follow the version info chain and retrieve the VersionInfo for a specific transaction (if any)
	static VersionInfo *GetVersionForTransaction(Transaction &transaction, VersionInfo *version);
};

} // namespace duckdb
