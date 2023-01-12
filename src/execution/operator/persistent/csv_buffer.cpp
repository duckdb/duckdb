#include "duckdb/execution/operator/persistent/csv_buffer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

CSVBuffer::CSVBuffer(ClientContext &context, idx_t buffer_size_p, CSVFileHandle &file_handle)
    : context(context), first_buffer(true) {
	this->handle = AllocateBuffer(buffer_size_p);

	auto buffer = Ptr();
	actual_size = file_handle.Read(buffer, buffer_size_p);
	if (actual_size >= 3 && buffer[0] == '\xEF' && buffer[1] == '\xBB' && buffer[2] == '\xBF') {
		start_position += 3;
	}
	last_buffer = file_handle.FinishedReading();
}

CSVBuffer::CSVBuffer(ClientContext &context, BufferHandle buffer_p, idx_t buffer_size_p, idx_t actual_size_p,
                     bool final_buffer)
    : context(context), handle(std::move(buffer_p)), actual_size(actual_size_p), last_buffer(final_buffer) {
}

unique_ptr<CSVBuffer> CSVBuffer::Next(CSVFileHandle &file_handle, idx_t set_buffer_size) {
	if (file_handle.FinishedReading()) {
		// this was the last buffer
		return nullptr;
	}

	auto next_buffer = AllocateBuffer(set_buffer_size);
	idx_t next_buffer_actual_size = file_handle.Read(next_buffer.Ptr(), set_buffer_size);

	return make_unique<CSVBuffer>(context, std::move(next_buffer), set_buffer_size, next_buffer_actual_size,
	                              file_handle.FinishedReading());
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

} // namespace duckdb
