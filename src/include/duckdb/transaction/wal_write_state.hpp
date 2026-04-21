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
class DuckTableEntry;
class DuckTransaction;
class WriteAheadLog;
class ClientContext;

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
	void SwitchTable(DuckTableEntry &table_entry, UndoFlags new_op);
	bool IsDroppedTable(const DataTableInfo &table) const;

	void WriteCatalogEntry(CatalogEntry &entry, data_ptr_t extra_data);
	void WriteDelete(DeleteInfo &info);
	void WriteUpdate(UpdateInfo &info);

private:
	DuckTransaction &transaction;
	WriteAheadLog &log;
	optional_ptr<StorageCommitState> commit_state;

	optional_ptr<DuckTableEntry> current_table_entry;
	//! Tables dropped by the end of this transaction.
	const unordered_set<const DataTableInfo *> &dropped_tables;

	unique_ptr<DataChunk> delete_chunk;
	unique_ptr<DataChunk> update_chunk;
};

} // namespace duckdb
