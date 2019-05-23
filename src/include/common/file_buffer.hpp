//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/file_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {
struct FileHandle;

//! The FileBuffer represents a buffer that can be read or written to a Direct IO FileHandle.
class FileBuffer {
	constexpr static int FILE_BUFFER_BLOCK_SIZE = 4096;
	constexpr static int FILE_BUFFER_HEADER_SIZE = sizeof(uint64_t);

public:
	//! Allocates a buffer of the specified size that is sector-aligned. bufsiz must be a multiple of
	//! FILE_BUFFER_BLOCK_SIZE. The content in this buffer can be written to FileHandles that have been opened with
	//! DIRECT_IO on all operating systems, however, the entire buffer must be written to the file. Note that the
	//! returned size is 8 bytes less than the allocation size to account for the checksum.
	FileBuffer(uint64_t bufsiz);
	~FileBuffer();

	//! The buffer that users can write to
	data_ptr_t buffer;
	//! The size of the portion that users can write to, this is equivalent to internal_size - FILE_BUFFER_HEADER_SIZE
	uint64_t size;

	//! Read into the FileBuffer from the specified location. Automatically verifies the checksum, and throws an
	//! exception if the checksum does not match correctly.
	void Read(FileHandle &handle, uint64_t location);
	//! Write the contents of the FileBuffer to the specified location. Automatically adds a checksum of the contents of
	//! the filebuffer in front of the written data.
	void Write(FileHandle &handle, uint64_t location);

	void Clear();

private:
	//! The pointer to the internal buffer that will be read or written, including the buffer header
	data_ptr_t internal_buffer;
	//! The aligned size as passed to the constructor. This is the size that is read or written to disk.
	uint64_t internal_size;

	//! The buffer that was actually malloc'd, i.e. the pointer that must be freed when the FileBuffer is destroyed
	data_ptr_t malloced_buffer;
};

} // namespace duckdb
