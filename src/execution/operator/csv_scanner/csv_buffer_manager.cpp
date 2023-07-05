#include "duckdb/execution/operator/persistent/csv_scanner/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/persistent/csv_scanner/csv_buffer.hpp"
namespace duckdb {

CSVBufferManager::CSVBufferManager(ClientContext &context_p, CSVFileHandle &file_handle_p)
    : context(context_p), file_handle(file_handle_p) {
	auto file_size = file_handle.FileSize();
	if (file_size > 0 && file_size < buffer_size) {
		buffer_size = file_size;
	}
	cached_buffers.emplace_back(make_shared<CSVBuffer>(context, buffer_size, file_handle, global_csv_pos, 0));
	last_buffer = cached_buffers.front();
}
bool CSVBufferManager::ReadNextAndCacheIt() {
	D_ASSERT(last_buffer);
	if (!last_buffer->IsCSVFileLastBuffer()) {
		last_buffer = last_buffer->Next(file_handle, buffer_size, global_csv_pos, 0);
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
			if (!last_buffer->IsCSVFileLastBuffer()) {
				last_buffer = last_buffer->Next(file_handle, buffer_size, global_csv_pos, 0);
				return last_buffer;
			}
		}
	}
	return nullptr;
}

char CSVBufferIterator::GetNextChar() {
	// If current buffer is not set we try to get a new one
	if (!cur_buffer) {
		cur_buffer = buffer_manager->GetBuffer(cur_buffer_idx++, true);
		cur_pos = 0;
		if (!cur_buffer) {
			return '\0';
		}
	}
	// If we finished the current buffer we try to get a new one
	if (cur_pos >= cur_buffer->GetBufferSize()) {
		cur_buffer = buffer_manager->GetBuffer(cur_buffer_idx++, true);
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
} // namespace duckdb
