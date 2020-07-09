#include "duckdb/parallel/task_scheduler.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include "concurrentqueue.h"
#include "lightweightsemaphore.h"

using namespace std;

namespace duckdb {

typedef moodycamel::LightweightSemaphore lightweight_semaphore_t;

struct Semaphore {
    lightweight_semaphore_t s;
};


TaskScheduler::TaskScheduler() : semaphore(make_unique<Semaphore>()) {

}

TaskScheduler::~TaskScheduler() {
    SetThreads(1);
}

TaskScheduler &TaskScheduler::GetScheduler(ClientContext &context) {
	return *context.db.scheduler;
}

void TaskScheduler::Schedule(Executor *executor) {
	lock_guard<mutex> slock(scheduler_lock);
	tasks.push_back(make_shared<ExecutorTask>(*executor));
	//! signal all threads to start working on this query
	semaphore->s.signal(threads.size());
}

void TaskScheduler::Finish(Executor *executor) {
	lock_guard<mutex> slock(scheduler_lock);
	idx_t i;
	for(i = 0; i < tasks.size(); i++) {
		if (&tasks[i]->executor == executor) {
			break;
		}
	}
	assert(i < tasks.size());
	tasks[i]->executor.finished = true;
	tasks.erase(tasks.begin() + i);
}

void TaskScheduler::ExecuteForever(bool *marker) {
    unique_ptr<Task> task;
	// loop until the marker is set to false
	while(*marker) {
		// wait for a signal with a timeout
		// the timeout allows us to periodically check
        semaphore->s.wait(TASK_TIMEOUT_USECS);
		// try to find a task to execute
		for(idx_t i = 0; i < tasks.size(); i++) {
			shared_ptr<ExecutorTask> task;
			{
				lock_guard<mutex> slock(scheduler_lock);
				if (i >= tasks.size()) {
					break;
				}
				task = tasks[i];
			}
			if (!task->finished) {
				task->executor.Work();
			}
		}
	}
}

static void ThreadExecuteTasks(TaskScheduler *scheduler, bool *marker) {
	scheduler->ExecuteForever(marker);
}

void TaskScheduler::SetThreads(int32_t n) {
	if (n < 1) {
		throw SyntaxException("Must have at least 1 thread!");
	}
	idx_t new_thread_count = n - 1;
	if (threads.size() < new_thread_count) {
		// we are increasing the number of threads: launch them and run tasks on them
		for(idx_t i = 0; i < new_thread_count - threads.size(); i++) {
			// launch a thread and assign it a cancellation marker
            auto marker = unique_ptr<bool>(new bool(true));
			auto worker_thread = make_unique<thread>(ThreadExecuteTasks, this, marker.get());

	        threads.push_back(move(worker_thread));
            markers.push_back(move(marker));
		}
	} else if (threads.size() > new_thread_count) {
		// we are reducing the number of threads: cancel any threads exceeding new_thread_count
		for(idx_t i = new_thread_count; i < threads.size(); i++) {
			*markers[i] = false;
		}
		// now join the threads to ensure they are fully stopped before erasing them
        for(idx_t i = new_thread_count; i < threads.size(); i++) {
            threads[i]->join();
        }
		// erase the threads/markers
		threads.resize(new_thread_count);
		markers.resize(new_thread_count);
	}
}

}
