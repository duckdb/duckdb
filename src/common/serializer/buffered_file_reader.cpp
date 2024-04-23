#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/exception.hpp"

#include <cstring>
#include <algorithm>

namespace duckdb {

BufferedFileReader::BufferedFileReader(FileSystem &fs, const char *path, FileLockType lock_type,
                                       optional_ptr<FileOpener> opener)
    : fs(fs), data(make_unsafe_uniq_array<data_t>(FILE_BUFFER_SIZE)), offset(0), read_data(0), total_read(0) {
	handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ | lock_type, opener.get());
	file_size = NumericCast<idx_t>(fs.GetFileSize(*handle));
}

BufferedFileReader::BufferedFileReader(FileSystem &fs, unique_ptr<FileHandle> handle_p)
    : fs(fs), data(make_unsafe_uniq_array<data_t>(FILE_BUFFER_SIZE)), offset(0), read_data(0),
      handle(std::move(handle_p)), total_read(0) {
	file_size = NumericCast<idx_t>(fs.GetFileSize(*handle));
}

void BufferedFileReader::ReadData(data_ptr_t target_buffer, uint64_t read_size) {
	// first copy anything we can from the buffer
	data_ptr_t end_ptr = target_buffer + read_size;
	while (true) {
		idx_t to_read = MinValue<idx_t>(UnsafeNumericCast<idx_t>(end_ptr - target_buffer), read_data - offset);
		if (to_read > 0) {
			memcpy(target_buffer, data.get() + offset, to_read);
			offset += to_read;
			target_buffer += to_read;
		}
		if (target_buffer < end_ptr) {
			D_ASSERT(offset == read_data);
			total_read += read_data;
			// did not finish reading yet but exhausted buffer
			// read data into buffer
			offset = 0;
			read_data = UnsafeNumericCast<idx_t>(fs.Read(*handle, data.get(), FILE_BUFFER_SIZE));
			if (read_data == 0) {
				throw SerializationException("not enough data in file to deserialize result");
			}
		} else {
			return;
		}
	}
}

bool BufferedFileReader::Finished() {
	return total_read + offset == file_size;
}

void BufferedFileReader::Seek(uint64_t location) {
	D_ASSERT(location <= file_size);
	handle->Seek(location);
	total_read = location;
	read_data = offset = 0;
}

void BufferedFileReader::Reset() {
	handle->Reset();
	total_read = 0;
	read_data = offset = 0;
}

uint64_t BufferedFileReader::CurrentOffset() {
	return total_read + offset;
}

} // namespace duckdb
