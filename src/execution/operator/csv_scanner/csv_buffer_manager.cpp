#include "duckdb/execution/operator/persistent/csv_scanner/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/persistent/csv_scanner/csv_buffer.hpp"
namespace duckdb {

CSVBufferManager::CSVBufferManager(ClientContext &context_p, unique_ptr<CSVFileHandle> file_handle_p,
                                   CSVReaderOptions &options, bool cache_buffers_p)
    : file_handle(std::move(file_handle_p)), context(context_p), cache_buffers(cache_buffers_p),
      buffer_size(CSV_BUFFER_SIZE) {
	if (options.skip_rows_set) {
		// Skip rows if they are set
		skip_rows = options.skip_rows;
	}
	auto file_size = file_handle->FileSize();
	if (file_size > 0 && file_size < buffer_size) {
		buffer_size = CSV_MINIMUM_BUFFER_SIZE;
	}
	Initialize();
}

void CSVBufferManager::Initialize() {
	file_handle->Reset();
	global_csv_pos = 0;
	for (idx_t i = 0; i < skip_rows; i++) {
		file_handle->ReadLine();
	}
	if (cache_buffers) {
		cached_buffers.emplace_back(make_shared<CSVBuffer>(context, buffer_size, *file_handle, global_csv_pos, 0));
		last_buffer = cached_buffers.front();
	} else {
		last_buffer = make_shared<CSVBuffer>(context, buffer_size, *file_handle, global_csv_pos, 0);
	}
	start_pos = last_buffer->GetStart();
}

idx_t CSVBufferManager::GetStartPos() {
	return start_pos;
}
bool CSVBufferManager::ReadNextAndCacheIt() {
	D_ASSERT(last_buffer);
	if (!last_buffer->IsCSVFileLastBuffer()) {
		last_buffer = last_buffer->Next(*file_handle, buffer_size, global_csv_pos, 0);
		cached_buffers.emplace_back(last_buffer);
		return true;
	}
	return false;
}

shared_ptr<CSVBuffer> CSVBufferManager::GetBuffer(idx_t pos, bool auto_detection) {
	if (auto_detection) {
		D_ASSERT(pos <= cached_buffers.size());
		if (pos == cached_buffers.size()) {
			if (!ReadNextAndCacheIt()) {
				return nullptr;
			}
		}
		return cached_buffers[pos];
	} else {
		if (pos < cached_buffers.size()) {
			auto buffer = cached_buffers[pos];
			// Invalidate this buffer
			cached_buffers[pos] = nullptr;
			return buffer;
		} else {
			if (!last_buffer) {
				last_buffer = make_shared<CSVBuffer>(context, buffer_size, *file_handle, global_csv_pos, 0);
			} else {
				if (last_buffer->GetCSVGlobalStart() == 0 && pos == 0) {
					return last_buffer;
				}
				if (!last_buffer->IsCSVFileLastBuffer()) {
					last_buffer = last_buffer->Next(*file_handle, buffer_size, global_csv_pos, 0);
				} else {
					return nullptr;
				}
			}
			return last_buffer;
		}
	}
}

char CSVBufferIterator::GetNextChar() {
	// If current buffer is not set we try to get a new one
	if (!cur_buffer) {
		cur_pos = 0;
		if (cur_buffer_idx == 0) {
			cur_pos = buffer_manager->GetStartPos();
		}
		cur_buffer = buffer_manager->GetBuffer(cur_buffer_idx++, false);
		if (!cur_buffer) {
			return '\0';
		}
	}
	// If we finished the current buffer we try to get a new one
	if (cur_pos >= cur_buffer->GetBufferSize()) {
		cur_buffer = buffer_manager->GetBuffer(cur_buffer_idx++, false);
		if (!cur_buffer) {
			return '\0';
		}
		cur_pos = 0;
	}
	// We return the next char
	return cur_buffer->Ptr()[cur_pos++];
}

bool CSVBufferIterator::Finished() {
	return !cur_buffer;
}

void CSVBufferIterator::Reset() {
	cur_buffer_idx = 0;
	cur_buffer = nullptr;
	buffer_manager->Initialize();
	cur_pos = buffer_manager->GetStartPos();
}

} // namespace duckdb
