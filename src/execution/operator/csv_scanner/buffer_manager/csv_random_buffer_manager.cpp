#include "duckdb/execution/operator/csv_scanner/csv_random_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_buffer.hpp"

namespace duckdb {

CSVRandomBufferManager::CSVRandomBufferManager(ClientContext &context_p, const CSVReaderOptions &options,
                                               const OpenFileInfo &file_p, bool per_file_single_threaded_p,
                                               unique_ptr<CSVFileHandle> file_handle_p)
    : CSVBufferManager(context_p, options, file_p, per_file_single_threaded_p, std::move(file_handle_p)) {
	Initialize();
}

void CSVRandomBufferManager::Initialize() {
	const idx_t buffer_count = KnownBufferCount();
	cached_buffers.resize(buffer_count);
	cached_buffers[0] = make_shared_ptr<CSVBuffer>(context, buffer_size, KnownBufferSize(0), 0, 0, buffer_count == 1);
	cached_buffers[0]->LoadRandomAccess(*file_handle);
}

shared_ptr<CSVBufferHandle> CSVRandomBufferManager::GetBuffer(const idx_t pos) {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (pos >= cached_buffers.size()) {
		return nullptr;
	}
	if (!cached_buffers[pos]) {
		cached_buffers[pos] = make_shared_ptr<CSVBuffer>(context, buffer_size, KnownBufferSize(pos), pos * buffer_size,
		                                                 pos, pos + 1 == cached_buffers.size());
		cached_buffers[pos]->LoadRandomAccess(*file_handle);
	}
	// an evicted buffer reloads inside Pin through the same positional read
	bool has_seeked = false;
	return cached_buffers[pos]->Pin(*file_handle, has_seeked);
}

CSVBufferResidency CSVRandomBufferManager::GetBufferResidency(const idx_t pos, shared_ptr<CSVBufferHandle> &handle) {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (pos >= cached_buffers.size()) {
		return CSVBufferResidency::END_OF_FILE;
	}
	if (!cached_buffers[pos] || !cached_buffers[pos]->IsInMemory()) {
		return CSVBufferResidency::NEEDS_LOAD;
	}
	bool has_seeked = false;
	handle = cached_buffers[pos]->Pin(*file_handle, has_seeked);
	return CSVBufferResidency::IN_MEMORY;
}

void CSVRandomBufferManager::ResetBuffer(const idx_t buffer_idx) {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (buffer_idx >= cached_buffers.size()) {
		// Nothing to reset
		return;
	}
	cached_buffers[buffer_idx].reset();
}

bool CSVRandomBufferManager::Done() const {
	// all buffer extents are known from construction
	return true;
}

void CSVRandomBufferManager::ResetBufferManager() {
	cached_buffers.clear();
	Initialize();
}

} // namespace duckdb
