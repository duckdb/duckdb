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
	                       optional_ptr<StorageCommitState> commit_state);

public:
	//! First pass over the undo buffer: record the DataTableInfo of every table that is dropped by the end of this
	//! transaction. Data ops targeting such tables are skipped in CommitEntry, because their contents will be removed
	//! anyway and keeping them in the WAL can cause replay issues when the rows reference a pre-rename table name
	//! (see issue #22124).
	void CollectDroppedTable(UndoFlags type, data_ptr_t data);
	void CommitEntry(UndoFlags type, data_ptr_t data);

private:
	void SwitchTable(DataTableInfo &table, UndoFlags new_op);
	bool IsDroppedTable(const DataTableInfo &table) const;

	void WriteCatalogEntry(CatalogEntry &entry, data_ptr_t extra_data);
	void WriteDelete(DeleteInfo &info);
	void WriteUpdate(UpdateInfo &info);

private:
	DuckTransaction &transaction;
	WriteAheadLog &log;
	optional_ptr<StorageCommitState> commit_state;

	optional_ptr<DataTableInfo> current_table_info;
	//! Tables that are dropped by the end of this transaction - keyed by DataTableInfo identity.
	unordered_set<const DataTableInfo *> dropped_tables;

	unique_ptr<DataChunk> delete_chunk;
	unique_ptr<DataChunk> update_chunk;
};

} // namespace duckdb
