//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/external_file_cache/async_file_read_task.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/async_result.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"
#include "duckdb/storage/external_file_cache/file_buffer_handle_group.hpp"

namespace duckdb {

//! The read an AsyncFileReadTask needs performed: [nr_bytes] at [location], landing in [destination]
struct AsyncReadRequest {
	AsyncReadRequest(CachingFileHandle &handle, idx_t nr_bytes, idx_t location, FileBufferHandleGroup &destination)
	    : handle(handle), nr_bytes(nr_bytes), location(location), destination(destination) {
	}

	CachingFileHandle &handle;
	idx_t nr_bytes;
	idx_t location;
	//! Where the bytes land, only valid once the read has completed
	FileBufferHandleGroup &destination;
};

//! An async task that performs exactly one read through a CachingFileHandle.
//! Subclasses only describe the read and consume it - whether it blocks the calling thread or is handed off to the
//! file system is decided here, so no task has to carry a fallback of its own.
class AsyncFileReadTask : public AsyncTask {
public:
	void Execute() final {
		auto request = PrepareRead();
		PerformRead(request);
		FinishRead();
	}

	AsyncTaskExecutionResult TryExecuteAsync(AsyncIOCallback on_complete) final {
		auto request = PrepareRead();
		if (request.handle.TryStartRead(request.nr_bytes, request.location, request.destination,
		                                std::move(on_complete))) {
			return AsyncTaskExecutionResult::PENDING;
		}
		// this file system reads synchronously - do the read here, holding the calling thread
		PerformRead(request);
		FinishRead();
		return AsyncTaskExecutionResult::FINISHED;
	}

	void FinishAsync() final {
		FinishRead();
	}

protected:
	//! Describe the read, doing any set-up it needs. Called exactly once per execution, before any I/O.
	virtual AsyncReadRequest PrepareRead() = 0;
	//! Consume the bytes once they have landed
	virtual void FinishRead() = 0;

private:
	static void PerformRead(AsyncReadRequest &request) {
		request.destination = request.handle.Read(request.nr_bytes, request.location);
	}
};

} // namespace duckdb
