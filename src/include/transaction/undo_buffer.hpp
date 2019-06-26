//===----------------------------------------------------------------------===//
//                         DuckDB
//
// transaction/undo_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"

#include <memory>
#include <vector>

namespace duckdb {

class WriteAheadLog;

enum class UndoFlags : uint8_t {
	EMPTY_ENTRY = 0,
	CATALOG_ENTRY = 1,
	INSERT_TUPLE = 2,
	DELETE_TUPLE = 3,
	UPDATE_TUPLE = 4,
	QUERY = 5
};

struct UndoChunk {
	UndoChunk(index_t size);
	~UndoChunk();

	data_ptr_t WriteEntry(UndoFlags type, uint32_t len);

	unique_ptr<data_t[]> data;
	index_t current_position;
	index_t maximum_size;
	unique_ptr<UndoChunk> next;
	UndoChunk *prev;
};

//! The undo buffer of a transaction is used to hold previous versions of tuples
//! that might be required in the future (because of rollbacks or previous
//! transactions accessing them)
class UndoBuffer {
public:
	UndoBuffer();

	//! Reserve space for an entry of the specified type and length in the undo
	//! buffer
	data_ptr_t CreateEntry(UndoFlags type, index_t len);

	bool ChangesMade();

	//! Cleanup the undo buffer
	void Cleanup();
	//! Commit the changes made in the UndoBuffer: should be called on commit
	void Commit(WriteAheadLog *log, transaction_t commit_id);
	//! Rollback the changes made in this UndoBuffer: should be called on
	//! rollback
	void Rollback();

private:
	unique_ptr<UndoChunk> head;
	UndoChunk *tail;

private:
	template <class T> void IterateEntries(T &&callback);
	template <class T> void ReverseIterateEntries(T &&callback);
};

} // namespace duckdb
