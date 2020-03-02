//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer/buffer_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_buffer.hpp"

namespace duckdb {

struct BufferEntry {
	BufferEntry(unique_ptr<FileBuffer> buffer) : buffer(move(buffer)), ref_count(1), prev(nullptr) {
	}
	~BufferEntry() {
		while (next) {
			next = move(next->next);
		}
	}

	//! The actual buffer
	unique_ptr<FileBuffer> buffer;
	//! The amount of references to this entry
	idx_t ref_count;
	//! Next node
	unique_ptr<BufferEntry> next;
	//! Prev entry
	BufferEntry *prev;
};

class BufferList {
public:
	BufferList() : last(nullptr), count(0) {
	}

public:
	//! Removes the first element (root) from the buffer list and returns it, O(1)
	unique_ptr<BufferEntry> Pop();
	//! Erase the specified element from the list and returns it, O(1)
	unique_ptr<BufferEntry> Erase(BufferEntry *entry);
	//! Insert an entry to the back of the list
	void Append(unique_ptr<BufferEntry> entry);

private:
	//! Root pointer
	unique_ptr<BufferEntry> root;
	//! Pointer to last element in list
	BufferEntry *last;
	//! The amount of entries in the list
	idx_t count;
};

} // namespace duckdb
