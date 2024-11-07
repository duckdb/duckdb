//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/undo_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/undo_flags.hpp"
#include "duckdb/transaction/undo_buffer_allocator.hpp"

namespace duckdb {
class BufferManager;
class DuckTransaction;
class StorageCommitState;
class WriteAheadLog;
struct UndoBufferPointer;

struct UndoBufferProperties {
	idx_t estimated_size = 0;
	bool has_updates = false;
	bool has_deletes = false;
	bool has_catalog_changes = false;
	bool has_dropped_entries = false;
};

//! The undo buffer of a transaction is used to hold previous versions of tuples
//! that might be required in the future (because of rollbacks or previous
//! transactions accessing them)
class UndoBuffer {
public:
	struct IteratorState {
		BufferHandle handle;
		optional_ptr<UndoBufferEntry> current;
		data_ptr_t start;
		data_ptr_t end;
	};

public:
	explicit UndoBuffer(DuckTransaction &transaction, ClientContext &context);

	//! Write a specified entry to the undo buffer
	UndoBufferReference CreateEntry(UndoFlags type, idx_t len);

	bool ChangesMade();
	UndoBufferProperties GetProperties();

	//! Cleanup the undo buffer
	void Cleanup(transaction_t lowest_active_transaction);
	//! Commit the changes made in the UndoBuffer: should be called on commit
	void WriteToWAL(WriteAheadLog &wal, optional_ptr<StorageCommitState> commit_state);
	//! Commit the changes made in the UndoBuffer: should be called on commit
	void Commit(UndoBuffer::IteratorState &iterator_state, transaction_t commit_id);
	//! Revert committed changes made in the UndoBuffer up until the currently committed state
	void RevertCommit(UndoBuffer::IteratorState &iterator_state, transaction_t transaction_id);
	//! Rollback the changes made in this UndoBuffer: should be called on
	//! rollback
	void Rollback();

private:
	DuckTransaction &transaction;
	UndoBufferAllocator allocator;

private:
	template <class T>
	void IterateEntries(UndoBuffer::IteratorState &state, T &&callback);
	template <class T>
	void IterateEntries(UndoBuffer::IteratorState &state, UndoBuffer::IteratorState &end_state, T &&callback);
	template <class T>
	void ReverseIterateEntries(T &&callback);
};

} // namespace duckdb
