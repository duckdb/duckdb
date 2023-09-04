#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_buffer.hpp"
namespace duckdb {

CSVBufferManager::CSVBufferManager(ClientContext &context_p, unique_ptr<CSVFileHandle> file_handle_p,
                                   const CSVReaderOptions &options, idx_t file_idx_p)
    : file_handle(std::move(file_handle_p)), context(context_p), file_idx(file_idx_p),
      buffer_size(CSVBuffer::CSV_BUFFER_SIZE) {
	if (options.skip_rows_set) {
		// Skip rows if they are set
		skip_rows = options.dialect_options.skip_rows;
	}
	auto file_size = file_handle->FileSize();
	if (file_size > 0 && file_size < buffer_size) {
		buffer_size = CSVBuffer::CSV_MINIMUM_BUFFER_SIZE;
	}
	if (options.buffer_size < buffer_size) {
		buffer_size = options.buffer_size;
	}
	for (idx_t i = 0; i < skip_rows; i++) {
		file_handle->ReadLine();
	}
	Initialize();
}

void CSVBufferManager::UnpinBuffer(idx_t cache_idx) {
	if (cache_idx < cached_buffers.size()) {
		cached_buffers[cache_idx]->Unpin();
	}
}

void CSVBufferManager::Initialize() {
	if (cached_buffers.empty()) {
		cached_buffers.emplace_back(
		    make_shared<CSVBuffer>(context, buffer_size, *file_handle, global_csv_pos, file_idx));
		last_buffer = cached_buffers.front();
	}
	start_pos = last_buffer->GetStart();
}

idx_t CSVBufferManager::GetStartPos() {
	return start_pos;
}
bool CSVBufferManager::ReadNextAndCacheIt() {
	D_ASSERT(last_buffer);
	if (!last_buffer->IsCSVFileLastBuffer()) {
		auto maybe_last_buffer = last_buffer->Next(*file_handle, buffer_size, file_idx);
		if (!maybe_last_buffer) {
			last_buffer->last_buffer = true;
			return false;
		}
		last_buffer = std::move(maybe_last_buffer);
		cached_buffers.emplace_back(last_buffer);
		return true;
	}
	return false;
}

unique_ptr<CSVBufferHandle> CSVBufferManager::GetBuffer(const idx_t pos) {
	while (pos >= cached_buffers.size()) {
		if (done) {
			return nullptr;
		}
		if (!ReadNextAndCacheIt()) {
			done = true;
		}
	}
	if (pos != 0) {
		cached_buffers[pos - 1]->Unpin();
	}
	return cached_buffers[pos]->Pin(*file_handle);
}

bool CSVBufferIterator::Finished() {
	return !cur_buffer_handle;
}

void CSVBufferIterator::Reset() {
	if (cur_buffer_handle) {
		cur_buffer_handle.reset();
	}
	if (cur_buffer_idx > 0) {
		buffer_manager->UnpinBuffer(cur_buffer_idx - 1);
	}
	cur_buffer_idx = 0;
	buffer_manager->Initialize();
	cur_pos = buffer_manager->GetStartPos();
}

} // namespace duckdb
