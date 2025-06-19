#include "duckdb/execution/operator/csv_scanner/csv_buffer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

CSVBuffer::CSVBuffer(ClientContext &context, idx_t buffer_size_p, CSVFileHandle &file_handle,
                     const idx_t &global_csv_current_position)
    : context(context), requested_size(buffer_size_p), can_seek(file_handle.CanSeek()), is_pipe(file_handle.IsPipe()) {
	AllocateBuffer(buffer_size_p);
	auto buffer = Ptr();
	actual_buffer_size = file_handle.Read(buffer, buffer_size_p);
	while (actual_buffer_size < buffer_size_p && !file_handle.FinishedReading()) {
		// We keep reading until this block is full
		actual_buffer_size += file_handle.Read(&buffer[actual_buffer_size], buffer_size_p - actual_buffer_size);
	}
	global_csv_start = global_csv_current_position;
	last_buffer = file_handle.FinishedReading();
}

CSVBuffer::CSVBuffer(CSVFileHandle &file_handle, ClientContext &context, idx_t buffer_size,
                     idx_t global_csv_current_position, idx_t buffer_idx_p)
    : context(context), requested_size(buffer_size), global_csv_start(global_csv_current_position),
      can_seek(file_handle.CanSeek()), is_pipe(file_handle.IsPipe()), buffer_idx(buffer_idx_p) {
	AllocateBuffer(buffer_size);
	auto buffer = handle.Ptr();
	actual_buffer_size = file_handle.Read(handle.Ptr(), buffer_size);
	while (actual_buffer_size < buffer_size && !file_handle.FinishedReading()) {
		// We keep reading until this block is full
		actual_buffer_size += file_handle.Read(&buffer[actual_buffer_size], buffer_size - actual_buffer_size);
	}
	last_buffer = file_handle.FinishedReading();
}

shared_ptr<CSVBuffer> CSVBuffer::Next(CSVFileHandle &file_handle, idx_t buffer_size, bool &has_seaked) const {
	if (has_seaked) {
		// This means that at some point a reload was done, and we are currently on the incorrect position in our file
		// handle
		file_handle.Seek(global_csv_start + actual_buffer_size);
		has_seaked = false;
	}
	auto next_csv_buffer = make_shared_ptr<CSVBuffer>(file_handle, context, buffer_size,
	                                                  global_csv_start + actual_buffer_size, buffer_idx + 1);
	if (next_csv_buffer->GetBufferSize() == 0) {
		// We are done reading
		return nullptr;
	}
	return next_csv_buffer;
}

void CSVBuffer::AllocateBuffer(idx_t buffer_size) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	bool can_destroy = !is_pipe;
	handle = buffer_manager.Allocate(MemoryTag::CSV_READER, MaxValue<idx_t>(buffer_manager.GetBlockSize(), buffer_size),
	                                 can_destroy);
	block = handle.GetBlockHandle();
}

idx_t CSVBuffer::GetBufferSize() const {
	return actual_buffer_size;
}

void CSVBuffer::Reload(CSVFileHandle &file_handle) {
	AllocateBuffer(actual_buffer_size);
	// If we can seek, we seek and return the correct pointers
	file_handle.Seek(global_csv_start);
	file_handle.Read(handle.Ptr(), actual_buffer_size);
}

shared_ptr<CSVBufferHandle> CSVBuffer::Pin(CSVFileHandle &file_handle, bool &has_seeked) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	if (!is_pipe && block->IsUnloaded()) {
		// We have to reload it from disk
		block = nullptr;
		Reload(file_handle);
		has_seeked = true;
	}
	return make_shared_ptr<CSVBufferHandle>(buffer_manager.Pin(block), actual_buffer_size, requested_size, last_buffer,
	                                        buffer_idx);
}

void CSVBuffer::Unpin() {
	if (handle.IsValid()) {
		handle.Destroy();
	}
}

bool CSVBuffer::IsCSVFileLastBuffer() const {
	return last_buffer;
}

} // namespace duckdb
