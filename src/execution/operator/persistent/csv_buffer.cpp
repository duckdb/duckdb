#include "duckdb/execution/operator/persistent/csv_buffer.hpp"

namespace duckdb {

CSVBuffer::CSVBuffer(idx_t buffer_size_p, CSVFileHandle &file_handle) : buffer_size(buffer_size_p) {
	buffer = unique_ptr<char[]>(new char[buffer_size_p]);
	actual_size = file_handle.Read(buffer.get(), buffer_size_p);
	if (actual_size >= 3 && buffer[0] == '\xEF' && buffer[1] == '\xBB' && buffer[2] == '\xBF') {
		start_position += 3;
	}
	last_buffer = file_handle.FinishedReading();
}

CSVBuffer::CSVBuffer(unique_ptr<char[]> buffer_p, idx_t buffer_size_p, idx_t actual_size_p, bool final_buffer)
    : buffer(move(buffer_p)), buffer_size(buffer_size_p), actual_size(actual_size_p), last_buffer(final_buffer) {
}

unique_ptr<CSVBuffer> CSVBuffer::Next(CSVFileHandle &file_handle, idx_t set_buffer_size) {
	if (file_handle.FinishedReading()) {
		// this was the last buffer
		return nullptr;
	}
	// Figure out the remaining part of this buffer
	int64_t remaining_start_pos = buffer_size - 1;
	for (; remaining_start_pos >= 0; remaining_start_pos--) {
		if (StringUtil::CharacterIsNewline(buffer[remaining_start_pos])) {
			break;
		}
	}
	// Next point right after new line
	remaining_start_pos++;

	idx_t remaining = buffer_size - remaining_start_pos;
	idx_t next_buffer_size = set_buffer_size + remaining;

	auto next_buffer = unique_ptr<char[]>(new char[next_buffer_size]);

	if (remaining > 0) {
		// remaining from this buffer: copy it here
		memcpy(next_buffer.get(), this->buffer.get() + remaining_start_pos, remaining);
	}
	idx_t next_buffer_actual_size = file_handle.Read(next_buffer.get() + remaining, set_buffer_size) + remaining;

	return make_unique<CSVBuffer>(move(next_buffer), next_buffer_size, next_buffer_actual_size, file_handle.FinishedReading());
}

idx_t CSVBuffer::GetBufferSize() {
	return actual_size;
}

idx_t CSVBuffer::GetStart() {
	return start_position;
}

bool CSVBuffer::IsCSVFileLastBuffer() {
	return last_buffer;
}

} // namespace duckdb
