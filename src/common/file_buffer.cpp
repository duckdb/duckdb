#include "duckdb/common/file_buffer.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"

#include <cstring>

namespace duckdb {

FileBuffer::FileBuffer(Allocator &allocator, FileBufferType type, uint64_t user_size)
    : allocator(allocator), type(type) {
	Init();
	if (user_size) {
		Resize(user_size);
	}
}

void FileBuffer::Init() {
	buffer = nullptr;
	size = 0;
	internal_buffer = nullptr;
	internal_size = 0;
	malloced_buffer = nullptr;
	malloced_size = 0;
}

FileBuffer::FileBuffer(FileBuffer &source, FileBufferType type_p) : allocator(source.allocator), type(type_p) {
	// take over the structures of the source buffer
	buffer = source.buffer;
	size = source.size;
	internal_buffer = source.internal_buffer;
	internal_size = source.internal_size;
	malloced_buffer = source.malloced_buffer;
	malloced_size = source.malloced_size;

	source.Init();
}

FileBuffer::~FileBuffer() {
	if (!malloced_buffer) {
		return;
	}
	allocator.FreeData(malloced_buffer, malloced_size);
}

void FileBuffer::ReallocBuffer(size_t new_size) {
	if (malloced_buffer) {
		malloced_buffer = allocator.ReallocateData(malloced_buffer, malloced_size, new_size);
	} else {
		malloced_buffer = allocator.AllocateData(new_size);
	}
	if (!malloced_buffer) {
		throw std::bad_alloc();
	}
	malloced_size = new_size;
	internal_buffer = malloced_buffer;
	internal_size = malloced_size;
	// Caller must update these.
	buffer = nullptr;
	size = 0;
}

FileBuffer::MemoryRequirement FileBuffer::CalculateMemory(uint64_t user_size) {
	FileBuffer::MemoryRequirement result;

	if (type == FileBufferType::TINY_BUFFER) {
		// We never do IO on tiny buffers, so there's no need to add a header or sector-align.
		result.header_size = 0;
		result.alloc_size = user_size;
	} else {
		result.header_size = Storage::BLOCK_HEADER_SIZE;
		result.alloc_size = AlignValue<uint32_t, Storage::SECTOR_SIZE>(result.header_size + user_size);
	}
	return result;
}

void FileBuffer::Resize(uint64_t new_size) {
	auto req = CalculateMemory(new_size);
	ReallocBuffer(req.alloc_size);

	if (new_size > 0) {
		buffer = internal_buffer + req.header_size;
		size = internal_size - req.header_size;
	}
}

void FileBuffer::Read(FileHandle &handle, uint64_t location) {
	D_ASSERT(type != FileBufferType::TINY_BUFFER);
	handle.Read(internal_buffer, internal_size, location);
}

void FileBuffer::ReadAndChecksum(FileHandle &handle, uint64_t location) {
	// read the buffer from disk
	Read(handle, location);
	// compute the checksum
	auto stored_checksum = Load<uint64_t>(internal_buffer);
	uint64_t computed_checksum = Checksum(buffer, size);
	// verify the checksum
	if (stored_checksum != computed_checksum) {
		throw IOException("Corrupt database file: computed checksum %llu does not match stored checksum %llu in block",
		                  computed_checksum, stored_checksum);
	}
}

void FileBuffer::Write(FileHandle &handle, uint64_t location) {
	D_ASSERT(type != FileBufferType::TINY_BUFFER);
	handle.Write(internal_buffer, internal_size, location);
}

void FileBuffer::ChecksumAndWrite(FileHandle &handle, uint64_t location) {
	// compute the checksum and write it to the start of the buffer (if not temp buffer)
	uint64_t checksum = Checksum(buffer, size);
	Store<uint64_t>(checksum, internal_buffer);
	// now write the buffer
	Write(handle, location);
}

void FileBuffer::Clear() {
	memset(internal_buffer, 0, internal_size);
}

} // namespace duckdb
