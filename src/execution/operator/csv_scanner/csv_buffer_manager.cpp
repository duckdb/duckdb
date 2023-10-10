#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_buffer.hpp"
#include "duckdb/function/table/read_csv.hpp"
namespace duckdb {

CSVBufferManager::CSVBufferManager(ClientContext &context_p, const CSVReaderOptions &options,
                                   const vector<string> &file_path)
    : context(context_p), buffer_size(CSVBuffer::CSV_BUFFER_SIZE) {
	D_ASSERT(!file_path.empty());
	cached_buffers.resize(file_path.size());
	file_handle = ReadCSV::OpenCSV(file_path[0], options.compression, context);
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

void CSVBufferManager::UnpinBuffer(const idx_t file_idx, const idx_t cache_idx) {
	if (file_idx < cached_buffers.size() && cache_idx < cached_buffers[file_idx].size()) {
		cached_buffers[file_idx][cache_idx]->Unpin();
	}
}

void CSVBufferManager::Initialize() {
	if (cached_buffers.empty()) {
		cached_buffers.resize(file_path.size());
		cached_buffers[0].emplace_back(make_shared<CSVBuffer>(context, buffer_size, *file_handle, global_csv_pos, 0));
		last_buffer = cached_buffers.front().front();
	}
	start_pos = last_buffer->GetStart();
}

idx_t CSVBufferManager::GetStartPos() {
	return start_pos;
}
bool CSVBufferManager::ReadNextAndCacheIt(const idx_t file_idx) {
	D_ASSERT(last_buffer);
	if (!last_buffer->IsCSVFileLastBuffer()) {
		auto maybe_last_buffer = last_buffer->Next(*file_handle, buffer_size, file_idx);
		if (!maybe_last_buffer) {
			last_buffer->last_buffer = true;
			return false;
		}
		last_buffer = std::move(maybe_last_buffer);
		cached_buffers[file_idx].emplace_back(last_buffer);
		return true;
	}
	return false;
}

unique_ptr<CSVBufferHandle> CSVBufferManager::GetBuffer(const idx_t file_idx, const idx_t pos) {
	while (pos >= cached_buffers[file_idx].size()) {
		if (done) {
			return nullptr;
		}
		if (!ReadNextAndCacheIt(file_idx)) {
			done = true;
		}
	}
	if (pos != 0) {
		cached_buffers[file_idx][pos - 1]->Unpin();
	}
	return cached_buffers[file_idx][pos]->Pin(*file_handle);
}

idx_t CSVBufferManager::GetBufferSize(){
	return buffer_size;
}
idx_t CSVBufferManager::FileCount(){
	return file_path.size();
}

} // namespace duckdb
