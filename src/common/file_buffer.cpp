#include "common/file_buffer.hpp"
#include "common/file_system.hpp"
#include "common/helper.hpp"
#include "common/checksum.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

FileBuffer::FileBuffer(uint64_t bufsiz) {
	assert(bufsiz % FILE_BUFFER_BLOCK_SIZE == 0);
	assert(bufsiz >= FILE_BUFFER_BLOCK_SIZE);
	// we add (FILE_BUFFER_BLOCK_SIZE - 1) to ensure that we can align the buffer to FILE_BUFFER_BLOCK_SIZE
	malloced_buffer = (data_ptr_t)malloc(bufsiz + (FILE_BUFFER_BLOCK_SIZE - 1));
	if (!malloced_buffer) {
		throw std::bad_alloc();
	}
	// round to multiple of FILE_BUFFER_BLOCK_SIZE
	uint64_t num = (uint64_t)malloced_buffer;
	uint64_t remainder = num % FILE_BUFFER_BLOCK_SIZE;
	if (remainder != 0) {
		num = num + FILE_BUFFER_BLOCK_SIZE - remainder;
	}
	assert(num % FILE_BUFFER_BLOCK_SIZE == 0);
	assert(num + bufsiz <= ((uint64_t)malloced_buffer + bufsiz + (FILE_BUFFER_BLOCK_SIZE - 1)));
	assert(num >= (uint64_t)malloced_buffer);
	// construct the FileBuffer object
	internal_buffer = (data_ptr_t)num;
	internal_size = bufsiz;
	buffer = internal_buffer + FILE_BUFFER_HEADER_SIZE;
	size = internal_size - FILE_BUFFER_HEADER_SIZE;
}

FileBuffer::~FileBuffer() {
	free(malloced_buffer);
}

void FileBuffer::Read(FileHandle &handle, uint64_t location) {
	// read the buffer from disk
	handle.Read(internal_buffer, internal_size, location);
	// compute the checksum
	uint64_t stored_checksum = *((uint64_t *)internal_buffer);
	uint64_t computed_checksum = Checksum(buffer, size);
	// verify the checksum
	if (stored_checksum != computed_checksum) {
		throw IOException("Corrupt database file: computed checksum %llu does not match stored checksum %llu in block",
		                  computed_checksum, stored_checksum);
	}
}

void FileBuffer::Write(FileHandle &handle, uint64_t location) {
	// compute the checksum and write it to the start of the buffer
	uint64_t checksum = Checksum(buffer, size);
	*((uint64_t *)internal_buffer) = checksum;
	// now write the buffer
	handle.Write(internal_buffer, internal_size, location);
}

void FileBuffer::Clear() {
	memset(internal_buffer, 0, internal_size);
}
