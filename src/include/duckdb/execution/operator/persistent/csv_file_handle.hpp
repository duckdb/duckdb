//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/csv_file_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"

namespace duckdb {

struct CSVFileHandle {
public:
	explicit CSVFileHandle(unique_ptr<FileHandle> file_handle_p) : file_handle(move(file_handle_p)) {
		can_seek = file_handle->CanSeek();
		plain_file_source = file_handle->OnDiskFile() && can_seek;
		file_size = file_handle->GetFileSize();
	}

	bool CanSeek() {
		return can_seek;
	}
	void Seek(idx_t position) {
		if (!can_seek) {
			throw InternalException("Cannot seek in this file");
		}
		file_handle->Seek(position);
	}
	idx_t SeekPosition() {
		if (!can_seek) {
			throw InternalException("Cannot seek in this file");
		}
		return file_handle->SeekPosition();
	}
	void Reset() {
		if (plain_file_source) {
			file_handle->Reset();
		} else {
			if (!reset_enabled) {
				throw InternalException("Reset called but reset is not enabled for this CSV Handle");
			}
			read_position = 0;
		}
	}
	bool PlainFileSource() {
		return plain_file_source;
	}

	bool OnDiskFile() {
		return file_handle->OnDiskFile();
	}

	idx_t FileSize() {
		return file_size;
	}

	bool FinishedReading() {
		return requested_bytes >= file_size;
	}

	idx_t Read(void *buffer, idx_t nr_bytes) {
		requested_bytes += nr_bytes;
		if (!plain_file_source) {
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
			} else if (!reset_enabled && cached_buffer) {
				// reset is disabled, but we still have cached data
				// we can remove any cached data
				cached_buffer.reset();
				buffer_size = 0;
				buffer_capacity = 0;
				read_position = 0;
			}
			// we have data left to read from the file
			// read directly into the buffer
			auto bytes_read = file_handle->Read((char *)buffer + result_offset, nr_bytes - result_offset);
			read_position += bytes_read;
			if (reset_enabled) {
				// if reset caching is enabled, we need to cache the bytes that we have read
				if (buffer_size + bytes_read >= buffer_capacity) {
					// no space; first enlarge the buffer
					buffer_capacity = MaxValue<idx_t>(NextPowerOfTwo(buffer_size + bytes_read), buffer_capacity * 2);

					auto new_buffer = unique_ptr<data_t[]>(new data_t[buffer_capacity]);
					if (buffer_size > 0) {
						memcpy(new_buffer.get(), cached_buffer.get(), buffer_size);
					}
					cached_buffer = move(new_buffer);
				}
				memcpy(cached_buffer.get() + buffer_size, (char *)buffer + result_offset, bytes_read);
				buffer_size += bytes_read;
			}

			return result_offset + bytes_read;
		} else {
			return file_handle->Read(buffer, nr_bytes);
		}
	}

	string ReadLine() {
		string result;
		char buffer[1];
		while (true) {
			idx_t tuples_read = Read(buffer, 1);
			if (tuples_read == 0 || buffer[0] == '\n') {
				return result;
			}
			if (buffer[0] != '\r') {
				result += buffer[0];
			}
		}
	}

	void DisableReset() {
		this->reset_enabled = false;
	}
	mutex main_mutex;
	idx_t count = 0;

private:
	unique_ptr<FileHandle> file_handle;
	bool reset_enabled = true;
	bool can_seek = false;
	bool plain_file_source = false;
	idx_t file_size = 0;
	// reset support
	unique_ptr<data_t[]> cached_buffer;
	idx_t read_position = 0;
	idx_t buffer_size = 0;
	idx_t buffer_capacity = 0;
	idx_t requested_bytes = 0;
};

} // namespace duckdb
