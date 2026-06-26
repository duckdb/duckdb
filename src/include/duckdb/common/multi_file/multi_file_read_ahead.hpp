//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/multi_file_read_ahead.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ClientContext;
class AsyncTask;
struct MultiFileScanJob;

//! Drives read-ahead for the multi-file scan, it's purpose is to keep several scan jobs scheduled ahead of decoding
class MultiFileReadAhead {
public:
	MultiFileReadAhead(idx_t read_ahead_depth);

public:
	//! Maximum number of jobs we try keep scheduled.
	idx_t ReadAheadDepth() const {
		return read_ahead_depth;
	}
	//! Jobs currently claimed but not yet finished decoding (i.e., queued or being decoded).
	idx_t ActiveJobs() const;

	//! Set/Check if scan is done, i.e., no more jobs to do
	void SetDone();
	bool IsDone() const;

	//! Schedule and put the job in a queue
	void PushJob(unique_ptr<MultiFileScanJob> job, vector<unique_ptr<AsyncTask>> io_tasks);

	//! Pop the oldest queued job
	unique_ptr<MultiFileScanJob> ClaimJob();

	//! Mark one dequeued job as fully decoded, freeing its slot.
	void FinishJob();

private:
	idx_t read_ahead_depth;

	mutable mutex lock;
	deque<unique_ptr<MultiFileScanJob>> ready_queue;
	atomic<idx_t> active_jobs {0};
	atomic<bool> done {false};
};

} // namespace duckdb
