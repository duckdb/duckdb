//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_file_handle.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"

namespace duckdb {
class CSVBuffer;
class CSVStateMachine;

//! This class is used to manage the CSV buffers.  Buffers are cached when used for auto detection.
//! When parsing, buffer are not cached and just returned.
//! A CSV Buffer Manager is created for each separate CSV File.
class CSVBufferManager {
public:
	CSVBufferManager(ClientContext &context, const CSVReaderOptions &options, const string &file_path,
	                 bool per_file_single_threaded = false, unique_ptr<CSVFileHandle> file_handle = nullptr);

	//! Returns a buffer from a buffer id (starting from 0). If it's in the auto-detection then we cache new buffers
	//! Otherwise we remove them from the cache if they are already there, or just return them bypassing the cache.
	shared_ptr<CSVBufferHandle> GetBuffer(const idx_t buffer_idx);

	void ResetBuffer(const idx_t buffer_idx);
	//! unique_ptr to the file handle, gets stolen after sniffing
	unique_ptr<CSVFileHandle> file_handle;
	//! Initializes the buffer manager, during it's construction/reset
	void Initialize();

	void UnpinBuffer(const idx_t cache_idx);
	//! Returns the buffer size set for this CSV buffer manager
	idx_t GetBufferSize() const;
	//! Returns the number of buffers in the cached_buffers cache
	idx_t BufferCount() const;
	//! If this buffer manager is done. In the context of a buffer manager it means that it read all buffers at least
	//! once.
	bool Done() const;

	void ResetBufferManager();
	string GetFilePath() const;

	bool IsBlockUnloaded(idx_t block_idx);

	idx_t GetBytesRead() const;

	ClientContext &context;
	idx_t skip_rows = 0;
	bool sniffing = false;
	const bool per_file_single_threaded;

private:
	//! Reads next buffer in reference to cached_buffers.front()
	bool ReadNextAndCacheIt();
	//! The file path this Buffer Manager refers to
	const string file_path;
	//! The cached buffers
	vector<shared_ptr<CSVBuffer>> cached_buffers;
	//! The last buffer it was accessed
	shared_ptr<CSVBuffer> last_buffer;
	idx_t global_csv_pos = 0;
	//! The size of the buffer, if the csv file has a smaller size than this, we will use that instead to malloc less
	idx_t buffer_size;
	//! If this buffer manager is done (i.e., no more buffers to read beyond the ones that were cached
	bool done = false;
	idx_t bytes_read = 0;
	//! Because the buffer manager can be accessed in Parallel we need a mutex.
	mutex main_mutex;
	//! If the file_handle used seek
	bool has_seeked = false;
	unordered_set<idx_t> reset_when_possible;
	bool is_pipe;
};

} // namespace duckdb
