#include "duckdb/common/file_buffer.hpp"

#include "duckdb/storage/block_allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/storage_info.hpp"

#include <cstring>

namespace duckdb {

FileBuffer::FileBuffer(BlockAllocator &allocator, FileBufferType type, uint64_t user_size, idx_t block_header_size)
    : allocator(allocator), type(type) {
	Init();
	if (user_size) {
		ResizeInternal(user_size, block_header_size);
	}
}

FileBuffer::FileBuffer(BlockAllocator &allocator, FileBufferType type, BlockManager &block_manager)
    : allocator(allocator), type(type) {
	Init();
	Resize(block_manager);
}

void FileBuffer::Init() {
	buffer = nullptr;
	size = 0;
	internal_buffer = nullptr;
	internal_size = 0;
}

FileBuffer::FileBuffer(FileBuffer &source, FileBufferType type_p, idx_t block_header_size)
    : allocator(source.allocator), type(type_p) {
	// take over the structures of the source buffer
	buffer = source.internal_buffer + block_header_size;
	size = source.internal_size - block_header_size;
	internal_buffer = source.internal_buffer;
	internal_size = source.internal_size;

	source.Init();
}

FileBuffer::~FileBuffer() {
	if (!internal_buffer) {
		return;
	}
	allocator.FreeData(internal_buffer, internal_size);
}

void FileBuffer::ReallocBuffer(idx_t new_size) {
	data_ptr_t new_buffer;
	if (internal_buffer) {
		new_buffer = allocator.ReallocateData(internal_buffer, internal_size, new_size);
	} else {
		new_buffer = allocator.AllocateData(new_size);
	}

	// FIXME: should we throw one of our exceptions here?
	if (!new_buffer) {
		throw std::bad_alloc();
	}

	internal_buffer = new_buffer;
	internal_size = new_size;

	// The caller must update these.
	buffer = nullptr;
	size = 0;
}

FileBuffer::MemoryRequirement FileBuffer::CalculateMemory(uint64_t user_size, uint64_t block_header_size) const {
	FileBuffer::MemoryRequirement result;
	if (type == FileBufferType::TINY_BUFFER) {
		// We never do IO on tiny buffers, so there's no need to add a header or sector-align.
		result.header_size = 0;
		result.alloc_size = user_size;
	} else {
		result.header_size = block_header_size;
		result.alloc_size = AlignValue<idx_t, Storage::SECTOR_SIZE>(result.header_size + user_size);
	}
	return result;
}

void FileBuffer::ResizeInternal(uint64_t new_size, uint64_t block_header_size) {
	auto req = CalculateMemory(new_size, block_header_size);
	ReallocBuffer(req.alloc_size);

	if (new_size > 0) {
		buffer = internal_buffer + req.header_size;
		size = internal_size - req.header_size;
	}
}

void FileBuffer::Resize(uint64_t new_size, BlockManager &block_manager) {
	ResizeInternal(new_size, block_manager.GetBlockHeaderSize());
}

void FileBuffer::Resize(BlockManager &block_manager) {
	ResizeInternal(block_manager.GetBlockSize(), block_manager.GetBlockHeaderSize());
}

void FileBuffer::Read(QueryContext context, FileHandle &handle, uint64_t location) {
	D_ASSERT(type != FileBufferType::TINY_BUFFER);
	handle.Read(context, internal_buffer, internal_size, location);
}

void FileBuffer::Write(QueryContext context, FileHandle &handle, const uint64_t location) {
	D_ASSERT(type != FileBufferType::TINY_BUFFER);
	handle.Write(context, internal_buffer, internal_size, location);
}

void FileBuffer::Clear() {
	memset(internal_buffer, 0, internal_size);
}

void FileBuffer::Initialize(DebugInitialize initialize) {
	if (initialize == DebugInitialize::NO_INITIALIZE) {
		return;
	}
	uint8_t value = initialize == DebugInitialize::DEBUG_ZERO_INITIALIZE ? 0 : 0xFF;
	memset(internal_buffer, value, internal_size);
}

} // namespace duckdb
