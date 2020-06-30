//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/task_scheduler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/thread.hpp"

namespace duckdb {

struct Task {
    virtual ~Task() { }

	//! Execute the task
	virtual void Execute() = 0;
};

//! The TaskScheduler is responsible for managing tasks and threads
class TaskScheduler {
public:
    TaskScheduler();
	~TaskScheduler();

	//! Schedule a task to be executed by the task scheduler
	void ScheduleTask(unique_ptr<Task> task);
	//! Execute tasks on this thread until all the tasks in the queue have been exhausted
	void ExecuteTasks();

	//! Sets the amount of active threads executing tasks for the system; n-1 background threads will be launched.
	//! The main thread will also be used for execution
	void SetThreads(int32_t n);
private:
	//! Run tasks forever until "marker" is set to false
    void ExecuteTasksForever(bool *marker);
private:
    vector<shared_ptr<Task>> tasks;
    //! The active background threads of the task scheduler
	vector<unique_ptr<thread>> threads;
	//! Markers used by the various threads, if the markers are set to "false" the thread execution is stopped
	vector<unique_ptr<bool>> markers;
};

} // namespace duckdb
