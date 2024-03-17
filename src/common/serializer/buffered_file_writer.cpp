#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/typedefs.hpp"
#include <cstring>

namespace duckdb {

// Remove this when we switch C++17: https://stackoverflow.com/a/53350948
constexpr uint8_t BufferedFileWriter::DEFAULT_OPEN_FLAGS;

BufferedFileWriter::BufferedFileWriter(FileSystem &fs, const string &path_p, uint8_t open_flags)
    : fs(fs), path(path_p), data(make_unsafe_uniq_array<data_t>(FILE_BUFFER_SIZE)), offset(0), total_written(0) {
	handle = fs.OpenFile(path, open_flags, FileLockType::WRITE_LOCK);
}

int64_t BufferedFileWriter::GetFileSize() {
	return fs.GetFileSize(*handle) + offset;
}

idx_t BufferedFileWriter::GetTotalWritten() {
	return total_written + offset;
}

void BufferedFileWriter::WriteData(const_data_ptr_t buffer, idx_t write_size) {
	if (write_size >= (FILE_BUFFER_SIZE + offset)) {
		idx_t to_copy = 0;
		// Check before performing direct IO if there is some data in the current internal buffer.
		// If so, then fill the buffer (to avoid to small write operation), flush it and then write
		// all the remain data directly.
		// This is to avoid to split a large buffer into N*FILE_BUFFER_SIZE buffers
		if (offset != 0) {
			// Some data are still present in the buffer let write them before
			to_copy = FILE_BUFFER_SIZE - offset;
			memcpy(data.get() + offset, buffer, to_copy);
			offset += to_copy;
			Flush(); // Flush buffer before writing every things else
		}
		idx_t remaining_to_write = write_size - to_copy;
		fs.Write(*handle, const_cast<data_ptr_t>(buffer + to_copy), remaining_to_write);
		total_written += remaining_to_write;
	} else {
		// first copy anything we can from the buffer
		const_data_ptr_t end_ptr = buffer + write_size;
		while (buffer < end_ptr) {
			idx_t to_write = MinValue<idx_t>((end_ptr - buffer), FILE_BUFFER_SIZE - offset);
			D_ASSERT(to_write > 0);
			memcpy(data.get() + offset, buffer, to_write);
			offset += to_write;
			buffer += to_write;
			if (offset == FILE_BUFFER_SIZE) {
				Flush();
			}
		}
	}
}

void BufferedFileWriter::Flush() {
	if (offset == 0) {
		return;
	}
	fs.Write(*handle, data.get(), offset);
	total_written += offset;
	offset = 0;
}

void BufferedFileWriter::Sync() {
	Flush();
	handle->Sync();
}

void BufferedFileWriter::Truncate(int64_t size) {
	uint64_t persistent = fs.GetFileSize(*handle);
	D_ASSERT((uint64_t)size <= persistent + offset);
	if (persistent <= (uint64_t)size) {
		// truncating into the pending write buffer.
		offset = size - persistent;
	} else {
		// truncate the physical file on disk
		handle->Truncate(size);
		// reset anything written in the buffer
		offset = 0;
	}
}

} // namespace duckdb
