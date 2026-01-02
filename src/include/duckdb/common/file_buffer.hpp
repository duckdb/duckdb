//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/debug_initialize.hpp"

namespace duckdb {

class BlockAllocator;
class BlockManager;
class QueryContext;

struct FileHandle;

enum class FileBufferType : uint8_t { BLOCK = 1, MANAGED_BUFFER = 2, TINY_BUFFER = 3, EXTERNAL_FILE = 4 };

static constexpr idx_t FILE_BUFFER_TYPE_COUNT = 4;

//! The FileBuffer represents a buffer that can be read or written to a Direct IO FileHandle.
class FileBuffer {
public:
	//! Allocates a buffer of the specified size, with room for additional header bytes
	//! (typically 8 bytes). On return, this->AllocSize() >= this->size >= user_size.
	//! Our allocation size will always be page-aligned, which is necessary to support
	//! DIRECT_IO
	FileBuffer(BlockAllocator &allocator, FileBufferType type, uint64_t user_size, idx_t block_header_size);
	FileBuffer(BlockAllocator &allocator, FileBufferType type, BlockManager &block_manager);
	FileBuffer(FileBuffer &source, FileBufferType type, idx_t block_header_size);

	virtual ~FileBuffer();

	BlockAllocator &allocator;
	//! The buffer that users can write to
	data_ptr_t buffer;
	//! The user-facing size of the buffer.
	//! This is equivalent to internal_size - block_header_size.
	uint64_t size;

public:
	//! Read into the FileBuffer from the location.
	void Read(QueryContext context, FileHandle &handle, uint64_t location);
	//! Write the FileBuffer to the location.
	void Write(QueryContext context, FileHandle &handle, const uint64_t location);

	void Clear();

	FileBufferType GetBufferType() const {
		return type;
	}

	// Same rules as the constructor. We add room for a header, in addition to
	// the requested user bytes. We then sector-align the result.
	void Resize(uint64_t user_size, BlockManager &block_manager);
	void Resize(BlockManager &block_manager);

	idx_t GetHeaderSize() const {
		return internal_size - size;
	}

	uint64_t AllocSize() const {
		return internal_size;
	}
	uint64_t Size() const {
		return size;
	}
	data_ptr_t InternalBuffer() {
		return internal_buffer;
	}

	struct MemoryRequirement {
		idx_t alloc_size;
		idx_t header_size;
	};

	MemoryRequirement CalculateMemory(uint64_t user_size, uint64_t block_header_size) const;
	void Initialize(DebugInitialize info);

protected:
	//! The type of the buffer.
	FileBufferType type;
	//! The pointer to the internal buffer that will be read from or written to.
	//! This includes the buffer header.
	data_ptr_t internal_buffer;
	//! The aligned size as passed to the constructor.
	//! This is the size that is read from or written to disk.
	uint64_t internal_size;

	void ReallocBuffer(idx_t new_size);
	void Init();

private:
	void ResizeInternal(uint64_t user_size, uint64_t block_header_size);
};

} // namespace duckdb
