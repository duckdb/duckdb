//===----------------------------------------------------------------------===//
//                         DuckDB
//
// transaction/commit_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "transaction/undo_buffer.hpp"

namespace duckdb {
class DataChunk;
class DataTable;
class WriteAheadLog;

template <bool HAS_LOG> class CommitState {
public:
	CommitState(transaction_t commit_id, WriteAheadLog *log = nullptr);

	WriteAheadLog *log;
	transaction_t commit_id;
	UndoFlags current_op;

	DataTable *current_table;
	unique_ptr<DataChunk> chunk;
	index_t row_identifiers[STANDARD_VECTOR_SIZE];

public:
	void CommitEntry(UndoFlags type, data_ptr_t data);

	void Flush(UndoFlags new_op);

private:
	void SwitchTable(DataTable *table, UndoFlags new_op);

	void PrepareAppend(UndoFlags op);

	void WriteCatalogEntry(CatalogEntry *entry);
	void WriteDelete(VersionInfo *info);
	void WriteUpdate(VersionInfo *info);
	void WriteInsert(VersionInfo *info);

	void AppendInfoData(VersionInfo *info);
	void AppendRowId(VersionInfo *info);
};

} // namespace duckdb
