#include "duckdb/execution/operator/scan/csv/csv_buffer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

CSVBuffer::CSVBuffer(ClientContext &context, idx_t buffer_size_p, CSVFileHandle &file_handle,
                     idx_t &global_csv_current_position, idx_t file_number_p)
    : context(context), first_buffer(true), file_number(file_number_p), can_seek(file_handle.CanSeek()) {
	AllocateBuffer(buffer_size_p);
	auto buffer = Ptr();
	file_size = file_handle.Read(buffer, buffer_size_p);
	global_csv_start = global_csv_current_position;
	// BOM check (https://en.wikipedia.org/wiki/Byte_order_mark)
	if (file_size >= 3 && buffer[0] == '\xEF' && buffer[1] == '\xBB' && buffer[2] == '\xBF') {
		start_position += 3;
	}
	last_buffer = file_handle.FinishedReading();
}

CSVBuffer::CSVBuffer(CSVFileHandle &file_handle, ClientContext &context, idx_t buffer_size,
                     idx_t global_csv_current_position, idx_t file_number_p)
    : context(context), global_csv_start(global_csv_current_position), file_number(file_number_p),
      can_seek(file_handle.CanSeek()) {
	AllocateBuffer(buffer_size);
	file_size = file_handle.Read(handle.Ptr(), buffer_size);
	last_buffer = file_handle.FinishedReading();
}

shared_ptr<CSVBuffer> CSVBuffer::Next(CSVFileHandle &file_handle, idx_t buffer_size, idx_t file_number_p) {
	auto next_csv_buffer =
	    make_shared<CSVBuffer>(file_handle, context, buffer_size, global_csv_start + file_size, file_number_p);
	if (next_csv_buffer->GetBufferSize() == 0) {
		// We are done reading
		return nullptr;
	}
	return next_csv_buffer;
}

void CSVBuffer::AllocateBuffer(idx_t buffer_size) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	bool can_destroy = can_seek;
	handle = buffer_manager.Allocate(MaxValue<idx_t>(Storage::BLOCK_SIZE, buffer_size), can_destroy, &block);
}

idx_t CSVBuffer::GetBufferSize() {
	return file_size;
}

void CSVBuffer::Reload(CSVFileHandle &file_handle) {
	AllocateBuffer(file_size);
	file_handle.Seek(global_csv_start);
	file_handle.Read(handle.Ptr(), file_size);
}

unique_ptr<CSVBufferHandle> CSVBuffer::Pin(CSVFileHandle &file_handle) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	if (can_seek && block->IsUnloaded()) {
		// We have to reload it from disk
		block = nullptr;
		Reload(file_handle);
	}
	return make_uniq<CSVBufferHandle>(buffer_manager.Pin(block), file_size, first_buffer, last_buffer, global_csv_start,
	                                  start_position, file_number);
}

void CSVBuffer::Unpin() {
	if (handle.IsValid()) {
		handle.Destroy();
	}
}

idx_t CSVBuffer::GetStart() {
	return start_position;
}

bool CSVBuffer::IsCSVFileLastBuffer() {
	return last_buffer;
}

} // namespace duckdb
