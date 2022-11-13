#include "duckdb/execution/operator/persistent/csv_buffer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

CSVBuffer::CSVBuffer(ClientContext &context, idx_t buffer_size_p, CSVFileHandle &file_handle,
                     idx_t &global_csv_current_position)
    : context(context), first_buffer(true) {
	this->handle = AllocateBuffer(buffer_size_p);

	auto buffer = Ptr();
	actual_size = file_handle.Read(buffer, buffer_size_p);
	global_csv_start = global_csv_current_position;
	global_csv_current_position += actual_size;
	if (actual_size >= 3 && buffer[0] == '\xEF' && buffer[1] == '\xBB' && buffer[2] == '\xBF') {
		start_position += 3;
	}
	last_buffer = file_handle.FinishedReading();
}

CSVBuffer::CSVBuffer(ClientContext &context, BufferHandle buffer_p, idx_t buffer_size_p, idx_t actual_size_p,
                     bool final_buffer, idx_t global_csv_current_position)
    : context(context), handle(move(buffer_p)), actual_size(actual_size_p), last_buffer(final_buffer),
      global_csv_start(global_csv_current_position) {
}

unique_ptr<CSVBuffer> CSVBuffer::Next(CSVFileHandle &file_handle, idx_t buffer_size,
                                      idx_t &global_csv_current_position) {
	if (file_handle.FinishedReading()) {
		// this was the last buffer
		return nullptr;
	}

	auto next_buffer = AllocateBuffer(buffer_size);
	idx_t next_buffer_actual_size = file_handle.Read(next_buffer.Ptr(), buffer_size);

	auto next_csv_buffer = make_unique<CSVBuffer>(context, move(next_buffer), buffer_size, next_buffer_actual_size,
	                                              file_handle.FinishedReading(), global_csv_current_position);
	global_csv_current_position += next_buffer_actual_size;
	return next_csv_buffer;
}

BufferHandle CSVBuffer::AllocateBuffer(idx_t buffer_size) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	return buffer_manager.Allocate(MaxValue<idx_t>(Storage::BLOCK_SIZE, buffer_size));
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

idx_t CSVBuffer::GetCSVGlobalStart() {
	return global_csv_start;
}

} // namespace duckdb
