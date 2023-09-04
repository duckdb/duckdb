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
class CSVBufferManager {
public:
	CSVBufferManager(ClientContext &context, unique_ptr<CSVFileHandle> file_handle, const CSVReaderOptions &options,
	                 idx_t file_idx = 0);
	//! Returns a buffer from a buffer id (starting from 0). If it's in the auto-detection then we cache new buffers
	//! Otherwise we remove them from the cache if they are already there, or just return them bypassing the cache.
	unique_ptr<CSVBufferHandle> GetBuffer(const idx_t pos);
	//! Returns the starting position of the first buffer
	idx_t GetStartPos();
	//! unique_ptr to the file handle, gets stolen after sniffing
	unique_ptr<CSVFileHandle> file_handle;
	//! Initializes the buffer manager, during it's construction/reset
	void Initialize();

	void UnpinBuffer(idx_t cache_idx);

	ClientContext &context;
	idx_t skip_rows = 0;
	idx_t file_idx;
	bool done = false;

private:
	//! Reads next buffer in reference to cached_buffers.front()
	bool ReadNextAndCacheIt();
	vector<shared_ptr<CSVBuffer>> cached_buffers;
	shared_ptr<CSVBuffer> last_buffer;
	idx_t global_csv_pos = 0;
	//! The size of the buffer, if the csv file has a smaller size than this, we will use that instead to malloc less
	idx_t buffer_size;
	//! Starting position of first buffer
	idx_t start_pos = 0;
};

class CSVBufferIterator {
public:
	explicit CSVBufferIterator(shared_ptr<CSVBufferManager> buffer_manager_p)
	    : buffer_manager(std::move(buffer_manager_p)) {
		cur_pos = buffer_manager->GetStartPos();
	};

	//! This functions templates an operation over the CSV File
	template <class OP, class T>
	inline bool Process(CSVStateMachine &machine, T &result) {

		OP::Initialize(machine);
		//! If current buffer is not set we try to get a new one
		if (!cur_buffer_handle) {
			cur_pos = 0;
			if (cur_buffer_idx == 0) {
				cur_pos = buffer_manager->GetStartPos();
			}
			cur_buffer_handle = buffer_manager->GetBuffer(cur_buffer_idx++);
			D_ASSERT(cur_buffer_handle);
		}
		while (cur_buffer_handle) {
			char *buffer_handle_ptr = cur_buffer_handle->Ptr();
			while (cur_pos < cur_buffer_handle->actual_size) {
				if (OP::Process(machine, result, buffer_handle_ptr[cur_pos], cur_pos)) {
					//! Not-Done Processing the File, but the Operator is happy!
					OP::Finalize(machine, result);
					return false;
				}
				cur_pos++;
			}
			cur_buffer_handle = buffer_manager->GetBuffer(cur_buffer_idx++);
			cur_pos = 0;
		}
		//! Done Processing the File
		OP::Finalize(machine, result);
		return true;
	}
	//! Returns true if the iterator is finished
	bool Finished();
	//! Resets the iterator
	void Reset();

private:
	idx_t cur_pos = 0;
	idx_t cur_buffer_idx = 0;
	shared_ptr<CSVBufferManager> buffer_manager;
	unique_ptr<CSVBufferHandle> cur_buffer_handle;
};
} // namespace duckdb
