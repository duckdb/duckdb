#include "duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_buffer.hpp"
#include "duckdb/function/table/read_csv.hpp"
namespace duckdb {

CSVBufferManager::CSVBufferManager(ClientContext &context_p, const CSVReaderOptions &options, const string &file_path_p,
                                   const idx_t file_idx_p)
    : context(context_p), file_idx(file_idx_p), file_path(file_path_p), buffer_size(CSVBuffer::CSV_BUFFER_SIZE) {
	D_ASSERT(!file_path.empty());
	file_handle = ReadCSV::OpenCSV(file_path, options.compression, context);
	skip_rows = options.dialect_options.skip_rows.GetValue();
	auto file_size = file_handle->FileSize();
	if (file_size > 0 && file_size < buffer_size) {
		buffer_size = CSVBuffer::CSV_MINIMUM_BUFFER_SIZE;
	}
	if (options.buffer_size < buffer_size) {
		buffer_size = options.buffer_size;
	}
	Initialize();
}

void CSVBufferManager::UnpinBuffer(const idx_t cache_idx) {
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
}

bool CSVBufferManager::ReadNextAndCacheIt() {
	D_ASSERT(last_buffer);
	for (idx_t i = 0; i < 2; i++) {
		if (!last_buffer->IsCSVFileLastBuffer()) {
			auto cur_buffer_size = buffer_size;
			if (file_handle->uncompressed) {
				if (file_handle->FileSize() - bytes_read) {
					cur_buffer_size = file_handle->FileSize() - bytes_read;
				}
			}
			if (cur_buffer_size == 0) {
				last_buffer->last_buffer = true;
				return false;
			}
			auto maybe_last_buffer = last_buffer->Next(*file_handle, cur_buffer_size, file_idx, has_seeked);
			if (!maybe_last_buffer) {
				last_buffer->last_buffer = true;
				return false;
			}
			last_buffer = std::move(maybe_last_buffer);
			bytes_read += last_buffer->GetBufferSize();
			cached_buffers.emplace_back(last_buffer);
			return true;
		}
	}
	return false;
}

shared_ptr<CSVBufferHandle> CSVBufferManager::GetBuffer(const idx_t pos) {
	lock_guard<mutex> parallel_lock(main_mutex);
	while (pos >= cached_buffers.size()) {
		if (done) {
			return nullptr;
		}
		if (!ReadNextAndCacheIt()) {
			done = true;
		}
	}
	if (pos != 0) {
		if (cached_buffers[pos - 1]) {
			cached_buffers[pos - 1]->Unpin();
		}
	}
	return cached_buffers[pos]->Pin(*file_handle, has_seeked);
}

void CSVBufferManager::ResetBuffer(const idx_t buffer_idx) {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (buffer_idx >= cached_buffers.size()) {
		// Nothing to reset
		return;
	}
	D_ASSERT(cached_buffers[buffer_idx]);
	if (buffer_idx == 0 && cached_buffers.size() > 1) {
		cached_buffers[buffer_idx].reset();
		idx_t cur_buffer = buffer_idx + 1;
		while (reset_when_possible.find(cur_buffer) != reset_when_possible.end()) {
			cached_buffers[cur_buffer].reset();
			reset_when_possible.erase(cur_buffer);
			cur_buffer++;
		}
		return;
	}
	// We only reset if previous one was also already reset
	if (buffer_idx > 0 && !cached_buffers[buffer_idx - 1]) {
		cached_buffers[buffer_idx].reset();
		idx_t cur_buffer = buffer_idx + 1;
		while (reset_when_possible.find(cur_buffer) != reset_when_possible.end()) {
			cached_buffers[cur_buffer].reset();
			reset_when_possible.erase(cur_buffer);
			cur_buffer++;
		}
	} else {
		reset_when_possible.insert(buffer_idx);
	}
}

idx_t CSVBufferManager::GetBufferSize() {
	return buffer_size;
}

idx_t CSVBufferManager::BufferCount() {
	return cached_buffers.size();
}

bool CSVBufferManager::Done() {
	return done;
}

string CSVBufferManager::GetFilePath() {
	return file_path;
}

} // namespace duckdb
