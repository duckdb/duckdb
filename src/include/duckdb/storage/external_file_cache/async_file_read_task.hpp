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

//! When an asynchronous read started and landed, stamped by the read itself rather than by whoever
//! picks the result up, so a reschedule is not counted as time on the wire
struct AsyncReadTiming {
	TimePoint started;
	TimePoint finished;
};

//! An async task that performs exactly one read through a CachingFileHandle. Subclasses describe the read and
//! consume it; whether it blocks or is handed to the file system is decided here, not by the task.
class AsyncFileReadTask : public AsyncTask {
public:
	void Execute() final {
		auto request = PrepareRead();
		PerformRead(request);
		FinishRead();
	}

	AsyncTaskExecutionResult TryExecuteAsync(AsyncIOCallback on_complete) final {
		read = make_uniq<AsyncReadRequest>(PrepareRead());
		timing = make_shared_ptr<AsyncReadTiming>();
		timing->started = TimePoint::Tick();
		// the completion stamps its own finish time, so a reschedule is not counted as time on the wire
		auto stamp = timing;
		auto submission = read->handle.TryStartRead(read->nr_bytes, read->location, destination,
		                                            [stamp, on_complete](optional_ptr<ErrorData> error) {
			                                            stamp->finished = TimePoint::Tick();
			                                            on_complete(error);
		                                            });
		switch (submission) {
		case FileReadSubmission::PENDING:
			return AsyncTaskExecutionResult::PENDING;
		case FileReadSubmission::COMPLETED:
			// the bytes are already here, so take delivery now rather than paying for a reschedule
			timing->finished = TimePoint::Tick();
			TakeDelivery();
			return AsyncTaskExecutionResult::FINISHED;
		default:
			// this file system reads synchronously - do the read here, holding the calling thread
			PerformRead(*read);
			FinishRead();
			return AsyncTaskExecutionResult::FINISHED;
		}
	}

	void FinishAsync() final {
		TakeDelivery();
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

	//! Take a read the file system performed off its hands: the bookkeeping the synchronous read does after
	//! the fact, then the bytes themselves
	void TakeDelivery() {
		read->handle.RecordAsyncRead(timing->started, timing->finished, read->nr_bytes);
		read->destination = destination->TakeGroup();
		FinishRead();
	}

private:
	//! The read started by TryExecuteAsync, held so FinishAsync can finish the same one
	unique_ptr<AsyncReadRequest> read;
	shared_ptr<FileBufferReadDestination> destination;
	shared_ptr<AsyncReadTiming> timing;
};

} // namespace duckdb
