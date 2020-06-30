#include "duckdb/execution/task_scheduler.hpp"
#include "duckdb/common/exception.hpp"
#include "blockingconcurrentqueue.h"

namespace duckdb {

TaskScheduler::TaskScheduler() {

}

TaskScheduler::~TaskScheduler() {
    SetThreads(1);
}

void TaskScheduler::ExecuteTasks() {

}

void TaskScheduler::ExecuteTasksForever(bool *marker) {
	while(*marker) {

	}
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
			auto worker_thread = make_unique<thread>([&]() {
                ExecuteTasksForever(marker.get());
			});

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
