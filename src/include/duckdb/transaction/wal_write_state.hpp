//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/wal_write_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/undo_buffer.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {
class CatalogEntry;
class DataChunk;
class DuckTransaction;
class WriteAheadLog;
class ClientContext;

struct DataTableInfo;
struct DeleteInfo;
struct UpdateInfo;

class WALWriteState {
public:
	explicit WALWriteState(DuckTransaction &transaction, WriteAheadLog &log,
	                       optional_ptr<StorageCommitState> commit_state,
	                       const unordered_set<const DataTableInfo *> &dropped_tables);

public:
	void CommitEntry(UndoFlags type, data_ptr_t data);

private:
	void SwitchTable(DataTableInfo &table, UndoFlags new_op);
	//! Returns true if this table is dropped by the end of the transaction - data ops on it are skipped to avoid
	//! writing stale USE_TABLE entries that can't be replayed (see issue #22124).
	bool IsDroppedTable(const DataTableInfo &table) const;

	void WriteCatalogEntry(CatalogEntry &entry, data_ptr_t extra_data);
	void WriteDelete(DeleteInfo &info);
	void WriteUpdate(UpdateInfo &info);

private:
	DuckTransaction &transaction;
	WriteAheadLog &log;
	optional_ptr<StorageCommitState> commit_state;

	optional_ptr<DataTableInfo> current_table_info;
	//! Tables that are dropped by the end of this transaction - populated at PushCatalogEntry time.
	const unordered_set<const DataTableInfo *> &dropped_tables;

	unique_ptr<DataChunk> delete_chunk;
	unique_ptr<DataChunk> update_chunk;
};

} // namespace duckdb
