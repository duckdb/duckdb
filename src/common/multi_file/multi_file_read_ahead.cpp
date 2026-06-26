#include "duckdb/common/multi_file/multi_file_read_ahead.hpp"

#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/parallel/async_result.hpp"

namespace duckdb {

MultiFileReadAhead::MultiFileReadAhead(idx_t read_ahead_depth_p)
    : read_ahead_depth(MaxValue<idx_t>(read_ahead_depth_p, 1)) {
}

idx_t MultiFileReadAhead::ActiveJobs() const {
	return active_jobs.load();
}

void MultiFileReadAhead::SetDone() {
	done = true;
}

bool MultiFileReadAhead::IsDone() const {
	return done.load();
}

void MultiFileReadAhead::PushJob(unique_ptr<MultiFileScanJob> job, vector<unique_ptr<AsyncTask>> io_tasks) {
	// run the job's I/O inline now, so it is ready to decode as soon as it is dequeued
	for (auto &task : io_tasks) {
		task->Execute();
	}
	// bump active_jobs together with the enqueue so a throw above (before any state is published) cannot desync the
	// count
	lock_guard<mutex> guard(lock);
	active_jobs++;
	ready_queue.push_back(std::move(job));
}

unique_ptr<MultiFileScanJob> MultiFileReadAhead::ClaimJob() {
	lock_guard<mutex> guard(lock);
	if (ready_queue.empty()) {
		return nullptr;
	}
	auto job = std::move(ready_queue.front());
	ready_queue.pop_front();
	return job;
}

void MultiFileReadAhead::FinishJob() {
	D_ASSERT(active_jobs.load() > 0);
	active_jobs--;
}

MultiFileGlobalState::~MultiFileGlobalState() = default;

} // namespace duckdb
