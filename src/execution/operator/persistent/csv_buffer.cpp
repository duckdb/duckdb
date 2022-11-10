#include "duckdb/execution/operator/persistent/csv_buffer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

CSVBuffer::CSVBuffer(idx_t buffer_size_p, CSVFileHandle &file_handle) : first_buffer(true) {
	buffer = unique_ptr<char[]>(new char[buffer_size_p]);
	actual_size = file_handle.Read(buffer.get(), buffer_size_p);
	if (actual_size >= 3 && buffer[0] == '\xEF' && buffer[1] == '\xBB' && buffer[2] == '\xBF') {
		start_position += 3;
	}
	last_buffer = file_handle.FinishedReading();
}

CSVBuffer::CSVBuffer(unique_ptr<char[]> buffer_p, idx_t buffer_size_p, idx_t actual_size_p, bool final_buffer)
    : buffer(move(buffer_p)), actual_size(actual_size_p), last_buffer(final_buffer) {
}

unique_ptr<CSVBuffer> CSVBuffer::Next(CSVFileHandle &file_handle, idx_t set_buffer_size) {
	if (file_handle.FinishedReading()) {
		// this was the last buffer
		return nullptr;
	}

	auto next_buffer = unique_ptr<char[]>(new char[set_buffer_size]);

	idx_t next_buffer_actual_size = file_handle.Read(next_buffer.get(), set_buffer_size);

	return make_unique<CSVBuffer>(move(next_buffer), set_buffer_size, next_buffer_actual_size,
	                              file_handle.FinishedReading());
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

bool CSVBuffer::IsCSVFileFirstBuffer() {
	return first_buffer;
}

} // namespace duckdb
