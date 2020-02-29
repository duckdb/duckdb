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

#include <memory>
#include <vector>

namespace duckdb {

class WriteAheadLog;

struct UndoChunk {
	UndoChunk(idx_t size);
	~UndoChunk();

	data_ptr_t WriteEntry(UndoFlags type, uint32_t len);

	unique_ptr<data_t[]> data;
	idx_t current_position;
	idx_t maximum_size;
	unique_ptr<UndoChunk> next;
	UndoChunk *prev;
};

//! The undo buffer of a transaction is used to hold previous versions of tuples
//! that might be required in the future (because of rollbacks or previous
//! transactions accessing them)
class UndoBuffer {
public:
	struct IteratorState {
		UndoChunk *current;
		data_ptr_t start;
		data_ptr_t end;
	};

public:
	UndoBuffer();

	//! Reserve space for an entry of the specified type and length in the undo
	//! buffer
	data_ptr_t CreateEntry(UndoFlags type, idx_t len);

	bool ChangesMade();

	//! Cleanup the undo buffer
	void Cleanup();
	//! Commit the changes made in the UndoBuffer: should be called on commit
	void Commit(UndoBuffer::IteratorState &iterator_state, WriteAheadLog *log, transaction_t commit_id);
	//! Revert committed changes made in the UndoBuffer up until the currently committed state
	void RevertCommit(UndoBuffer::IteratorState &iterator_state, transaction_t transaction_id);
	//! Rollback the changes made in this UndoBuffer: should be called on
	//! rollback
	void Rollback() noexcept;

private:
	unique_ptr<UndoChunk> head;
	UndoChunk *tail;

private:
	template <class T> void IterateEntries(UndoBuffer::IteratorState &state, T &&callback);
	template <class T>
	void IterateEntries(UndoBuffer::IteratorState &state, UndoBuffer::IteratorState &end_state, T &&callback);
	template <class T> void ReverseIterateEntries(T &&callback);
};

} // namespace duckdb
