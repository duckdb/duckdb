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

struct UndoEntry {
	UndoFlags type;
	index_t length;
	unique_ptr<data_t[]> data;
};

//! The undo buffer of a transaction is used to hold previous versions of tuples
//! that might be required in the future (because of rollbacks or previous
//! transactions accessing them)
class UndoBuffer {
public:
	UndoBuffer() {
	}

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
	// List of UndoEntries, FIXME: this can be more efficient
	vector<UndoEntry> entries;

	UndoBuffer(const UndoBuffer &) = delete;
};

} // namespace duckdb
