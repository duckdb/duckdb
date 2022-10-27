//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
class Allocator;
struct FileHandle;

enum class FileBufferType : uint8_t { BLOCK = 1, MANAGED_BUFFER = 2, TINY_BUFFER = 3 };

//! The FileBuffer represents a buffer that can be read or written to a Direct IO FileHandle.
class FileBuffer {
public:
	//! Allocates a buffer of the specified size, with room for additional header bytes
	//! (typically 8 bytes). On return, this->AllocSize() >= this->size >= user_size.
	//! Our allocation size will always be page-aligned, which is necessary to support
	//! DIRECT_IO
	FileBuffer(Allocator &allocator, FileBufferType type, uint64_t user_size);
	FileBuffer(FileBuffer &source, FileBufferType type);

	virtual ~FileBuffer();

	Allocator &allocator;
	//! The type of the buffer
	FileBufferType type;
	//! The buffer that users can write to
	data_ptr_t buffer;
	//! The size of the portion that users can write to, this is equivalent to internal_size - BLOCK_HEADER_SIZE
	uint64_t size;

public:
	//! Read into the FileBuffer from the specified location.
	void Read(FileHandle &handle, uint64_t location);
	//! Read into the FileBuffer from the specified location. Automatically verifies the checksum, and throws an
	//! exception if the checksum does not match correctly.
	virtual void ReadAndChecksum(FileHandle &handle, uint64_t location);
	//! Write the contents of the FileBuffer to the specified location.
	void Write(FileHandle &handle, uint64_t location);
	//! Write the contents of the FileBuffer to the specified location. Automatically adds a checksum of the contents of
	//! the filebuffer in front of the written data.
	virtual void ChecksumAndWrite(FileHandle &handle, uint64_t location);

	void Clear();

	// Same rules as the constructor. We will add room for a header, in additio to
	// the requested user bytes. We will then sector-align the result.
	virtual void Resize(uint64_t user_size);

	uint64_t AllocSize() const {
		return internal_size;
	}

protected:
	//! The pointer to the internal buffer that will be read or written, including the buffer header
	data_ptr_t internal_buffer;
	//! The aligned size as passed to the constructor. This is the size that is read or written to disk.
	uint64_t internal_size;

	void ReallocBuffer(size_t malloc_size);

private:
	//! The buffer that was actually malloc'd, i.e. the pointer that must be freed when the FileBuffer is destroyed
	data_ptr_t malloced_buffer;
	uint64_t malloced_size;

protected:
	uint64_t GetMallocedSize() {
		return malloced_size;
	}
	void Init();
};

} // namespace duckdb
