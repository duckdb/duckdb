//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/operator/scan/csv/csv_file_handle.hpp"
#include "duckdb/execution/operator/scan/csv/csv_reader_options.hpp"

namespace duckdb {
class CSVBuffer;
class CSVStateMachine;

//! This class is used to manage the CSV buffers.  Buffers are cached when used for auto detection.
//! When parsing, buffer are not cached and just returned.
//! A CSV Buffer Managers, holds all buffers for all csv files used within a CSV Scanner
class CSVBufferManager {
public:
	CSVBufferManager(ClientContext &context, const CSVReaderOptions &options, const vector<string> &file_path);
	//! Returns a buffer from a buffer id (starting from 0). If it's in the auto-detection then we cache new buffers
	//! Otherwise we remove them from the cache if they are already there, or just return them bypassing the cache.
	unique_ptr<CSVBufferHandle> GetBuffer(const idx_t file_idx, const idx_t pos);
	//! Returns the starting position of the first buffer
	idx_t GetStartPos();
	//! unique_ptr to the file handle, gets stolen after sniffing
	unique_ptr<CSVFileHandle> file_handle;
	//! Initializes the buffer manager, during it's construction/reset
	void Initialize();

	void UnpinBuffer(const idx_t file_idx, const idx_t cache_idx);
	//! Returns the buffer size set for this CSV buffer manager
	idx_t GetBufferSize();
	//! How many files are part of this buffer manager
	idx_t FileCount();
	ClientContext &context;
	idx_t skip_rows = 0;
	bool done = false;

private:
	//! Reads next buffer in reference to cached_buffers.front()
	bool ReadNextAndCacheIt(const idx_t file_idx);
	vector<string> file_path;
	vector<vector<shared_ptr<CSVBuffer>>> cached_buffers;
	shared_ptr<CSVBuffer> last_buffer;
	idx_t global_csv_pos = 0;
	//! The size of the buffer, if the csv file has a smaller size than this, we will use that instead to malloc less
	idx_t buffer_size;
	//! Starting position of first buffer
	idx_t start_pos = 0;
};

} // namespace duckdb
