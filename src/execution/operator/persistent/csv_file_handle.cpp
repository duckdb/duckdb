#include "duckdb/execution/operator/persistent/csv_file_handle.hpp"

namespace duckdb {

CSVFileHandle::CSVFileHandle(FileSystem &fs, Allocator &allocator, unique_ptr<FileHandle> file_handle_p,
                             const string &path_p, FileCompressionType compression, bool enable_reset)
    : fs(fs), allocator(allocator), file_handle(std::move(file_handle_p)), path(path_p), compression(compression),
      reset_enabled(enable_reset) {
	can_seek = file_handle->CanSeek();
	on_disk_file = file_handle->OnDiskFile();
	file_size = file_handle->GetFileSize();
}

unique_ptr<FileHandle> CSVFileHandle::OpenFileHandle(FileSystem &fs, Allocator &allocator, const string &path,
                                                     FileCompressionType compression) {
	auto file_handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ, FileLockType::NO_LOCK, compression);
	if (file_handle->CanSeek()) {
		file_handle->Reset();
	}
	return file_handle;
}

unique_ptr<CSVFileHandle> CSVFileHandle::OpenFile(FileSystem &fs, Allocator &allocator, const string &path,
                                                  FileCompressionType compression, bool enable_reset) {
	auto file_handle = CSVFileHandle::OpenFileHandle(fs, allocator, path, compression);
	return make_uniq<CSVFileHandle>(fs, allocator, std::move(file_handle), path, compression, enable_reset);
}

bool CSVFileHandle::CanSeek() {
	return can_seek;
}

void CSVFileHandle::Seek(idx_t position) {
	if (!can_seek) {
		throw InternalException("Cannot seek in this file");
	}
	file_handle->Seek(position);
}

idx_t CSVFileHandle::SeekPosition() {
	if (!can_seek) {
		throw InternalException("Cannot seek in this file");
	}
	return file_handle->SeekPosition();
}

void CSVFileHandle::Reset() {
	requested_bytes = 0;
	read_position = 0;
	if (can_seek) {
		// we can seek - reset the file handle
		file_handle->Reset();
	} else if (on_disk_file) {
		// we cannot seek but it is an on-disk file - re-open the file
		file_handle = CSVFileHandle::OpenFileHandle(fs, allocator, path, compression);
	} else {
		if (!reset_enabled) {
			throw InternalException("Reset called but reset is not enabled for this CSV Handle");
		}
		read_position = 0;
	}
}
bool CSVFileHandle::OnDiskFile() {
	return on_disk_file;
}

idx_t CSVFileHandle::FileSize() {
	return file_size;
}

bool CSVFileHandle::FinishedReading() {
	return requested_bytes >= file_size;
}

idx_t CSVFileHandle::Read(void *buffer, idx_t nr_bytes) {
	requested_bytes += nr_bytes;
	if (on_disk_file || can_seek) {
		// if this is a plain file source OR we can seek we are not caching anything
		return file_handle->Read(buffer, nr_bytes);
	}
	// not a plain file source: we need to do some bookkeeping around the reset functionality
	idx_t result_offset = 0;
	if (read_position < buffer_size) {
		// we need to read from our cached buffer
		auto buffer_read_count = MinValue<idx_t>(nr_bytes, buffer_size - read_position);
		memcpy(buffer, cached_buffer.get() + read_position, buffer_read_count);
		result_offset += buffer_read_count;
		read_position += buffer_read_count;
		if (result_offset == nr_bytes) {
			return nr_bytes;
		}
	} else if (!reset_enabled && cached_buffer.IsSet()) {
		// reset is disabled, but we still have cached data
		// we can remove any cached data
		cached_buffer.Reset();
		buffer_size = 0;
		buffer_capacity = 0;
		read_position = 0;
	}
	// we have data left to read from the file
	// read directly into the buffer
	auto bytes_read = file_handle->Read(char_ptr_cast(buffer) + result_offset, nr_bytes - result_offset);
	file_size = file_handle->GetFileSize();
	read_position += bytes_read;
	if (reset_enabled) {
		// if reset caching is enabled, we need to cache the bytes that we have read
		if (buffer_size + bytes_read >= buffer_capacity) {
			// no space; first enlarge the buffer
			buffer_capacity = MaxValue<idx_t>(NextPowerOfTwo(buffer_size + bytes_read), buffer_capacity * 2);

			auto new_buffer = allocator.Allocate(buffer_capacity);
			if (buffer_size > 0) {
				memcpy(new_buffer.get(), cached_buffer.get(), buffer_size);
			}
			cached_buffer = std::move(new_buffer);
		}
		memcpy(cached_buffer.get() + buffer_size, char_ptr_cast(buffer) + result_offset, bytes_read);
		buffer_size += bytes_read;
	}

	return result_offset + bytes_read;
}

string CSVFileHandle::ReadLine() {
	bool carriage_return = false;
	string result;
	char buffer[1];
	while (true) {
		idx_t bytes_read = Read(buffer, 1);
		if (bytes_read == 0) {
			return result;
		}
		if (carriage_return) {
			if (buffer[0] != '\n') {
				if (!file_handle->CanSeek()) {
					throw BinderException(
					    "Carriage return newlines not supported when reading CSV files in which we cannot seek");
				}
				file_handle->Seek(file_handle->SeekPosition() - 1);
				return result;
			}
		}
		if (buffer[0] == '\n') {
			return result;
		}
		if (buffer[0] != '\r') {
			result += buffer[0];
		} else {
			carriage_return = true;
		}
	}
}

void CSVFileHandle::DisableReset() {
	this->reset_enabled = false;
}

} // namespace duckdb
