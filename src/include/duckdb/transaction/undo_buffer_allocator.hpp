//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/undo_buffer_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"

namespace duckdb {
class BufferManager;
class BlockHandle;
struct UndoBufferEntry;
struct UndoBufferPointer;

struct UndoBufferEntry {
	explicit UndoBufferEntry(BufferManager &buffer_manager) : buffer_manager(buffer_manager) {
	}
	~UndoBufferEntry();

	BufferManager &buffer_manager;
	shared_ptr<BlockHandle> block;
	idx_t position = 0;
	idx_t capacity = 0;
	unique_ptr<UndoBufferEntry> next;
	optional_ptr<UndoBufferEntry> prev;
};

struct UndoBufferReference {
	UndoBufferReference() : entry(nullptr), position(0) {
	}
	UndoBufferReference(UndoBufferEntry &entry_p, BufferHandle handle_p, idx_t position)
	    : entry(&entry_p), handle(std::move(handle_p)), position(position) {
	}

	optional_ptr<UndoBufferEntry> entry;
	BufferHandle handle;
	idx_t position;

	data_ptr_t Ptr() {
		return handle.Ptr() + position;
	}
	bool IsSet() const {
		return entry;
	}

	UndoBufferPointer GetBufferPointer();
};

struct UndoBufferPointer {
	UndoBufferPointer() : entry(nullptr), position(0) {
	}
	UndoBufferPointer(UndoBufferEntry &entry_p, idx_t position) : entry(&entry_p), position(position) {
	}

	UndoBufferEntry *entry;
	idx_t position;

	UndoBufferReference Pin() const;
	bool IsSet() const {
		return entry;
	}
};

struct UndoBufferAllocator {
	explicit UndoBufferAllocator(BufferManager &buffer_manager);

	UndoBufferReference Allocate(idx_t alloc_len);

	BufferManager &buffer_manager;
	unique_ptr<UndoBufferEntry> head;
	optional_ptr<UndoBufferEntry> tail;
};

} // namespace duckdb
