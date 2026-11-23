#include "duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_sequential_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_random_access_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_buffer.hpp"
#include "duckdb/function/table/read_csv.hpp"
namespace duckdb {

CSVBufferManager::CSVBufferManager(ClientContext &context_p, const CSVReaderOptions &options,
                                   const OpenFileInfo &file_p, bool per_file_single_threaded_p,
                                   unique_ptr<CSVFileHandle> file_handle_p)
    : file_handle(std::move(file_handle_p)), context(context_p), per_file_single_threaded(per_file_single_threaded_p),
      file(file_p), buffer_size(options.buffer_size_option.GetValue()) {
	D_ASSERT(file_handle);
}

shared_ptr<CSVBufferManager> CSVBufferManager::Open(ClientContext &context, const CSVReaderOptions &options,
                                                    const OpenFileInfo &file, bool per_file_single_threaded,
                                                    unique_ptr<CSVFileHandle> file_handle) {
	D_ASSERT(!file.path.empty());
	if (!file_handle) {
		file_handle = ReadCSV::OpenCSV(file, options, context);
	}
	if (file_handle->HasKnownBufferRanges()) {
		return make_shared_ptr<CSVRandomAccessBufferManager>(context, options, file, per_file_single_threaded,
		                                                     std::move(file_handle));
	}
	return make_shared_ptr<CSVSequentialBufferManager>(context, options, file, per_file_single_threaded,
	                                                   std::move(file_handle));
}

bool CSVBufferManager::HasKnownBufferRanges() const {
	return file_handle->HasKnownBufferRanges();
}

idx_t CSVBufferManager::KnownBufferCount() const {
	D_ASSERT(HasKnownBufferRanges());
	const idx_t file_size = file_handle->FileSize();
	if (file_size == 0) {
		// an empty file still materializes a single empty buffer
		return 1;
	}
	return (file_size + buffer_size - 1) / buffer_size;
}

idx_t CSVBufferManager::KnownBufferSize(const idx_t buffer_idx) const {
	D_ASSERT(HasKnownBufferRanges() && buffer_idx < KnownBufferCount());
	const idx_t file_size = file_handle->FileSize();
	const idx_t buffer_start = buffer_idx * buffer_size;
	return MinValue<idx_t>(buffer_size, file_size - buffer_start);
}

void CSVBufferManager::RememberTail(idx_t buffer_idx, CSVBufferHandle &handle) {
	if (buffer_idx >= buffer_tails.size()) {
		buffer_tails.resize(buffer_idx + 1);
	}
	if (buffer_tails[buffer_idx].empty() && handle.actual_size > 0) {
		// the two runs of bytes the buffer ends in are kept whole, so that escapes before a line break keep their count
		const char *bytes = handle.Ptr();
		idx_t tail = 0;
		for (idx_t runs = 0; runs < 2 && tail < handle.actual_size && tail < TAIL_CAP; runs++) {
			const char byte = bytes[handle.actual_size - 1 - tail];
			while (tail < handle.actual_size && tail < TAIL_CAP && bytes[handle.actual_size - 1 - tail] == byte) {
				tail++;
			}
		}
		tail = MinValue<idx_t>(tail + TAIL_BYTES, handle.actual_size);
		buffer_tails[buffer_idx] = string(bytes + handle.actual_size - tail, tail);
	}
}

bool CSVBufferManager::ByteBeforeBufferEnd(idx_t buffer_idx, idx_t distance, char &byte) {
	for (idx_t attempt = 0; attempt < 2; attempt++) {
		{
			lock_guard<mutex> parallel_lock(main_mutex);
			if (buffer_idx < buffer_tails.size() && !buffer_tails[buffer_idx].empty()) {
				const auto &tail = buffer_tails[buffer_idx];
				if (tail.size() < distance) {
					return false;
				}
				byte = tail[tail.size() - distance];
				return true;
			}
		}
		// the buffer was not handed out yet, its tail is kept once it is
		GetBuffer(buffer_idx);
	}
	return false;
}

idx_t CSVBufferManager::GetBufferSize() const {
	return buffer_size;
}

idx_t CSVBufferManager::BufferCount() const {
	lock_guard<mutex> parallel_lock(main_mutex);
	return cached_buffers.size();
}

string CSVBufferManager::GetFilePath() const {
	return file.path;
}

bool CSVBufferManager::IsBlockUnloaded(idx_t block_idx) {
	if (block_idx < cached_buffers.size()) {
		return cached_buffers[block_idx]->IsUnloaded();
	}
	return false;
}

} // namespace duckdb
