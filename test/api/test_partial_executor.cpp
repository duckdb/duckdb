#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parallel/task_executor.hpp"

#include <chrono>
#include <condition_variable>
#include <future>
#include <thread>

using namespace duckdb;

struct WeirdTask : BaseExecutorTask {
	using BaseExecutorTask::BaseExecutorTask;

	void ExecuteTask() override {
	}

	TaskExecutionResult Execute(TaskExecutionMode mode) override {
		if (mode == TaskExecutionMode::PROCESS_PARTIAL) {
			std::this_thread::sleep_for(std::chrono::milliseconds(300));
			return TaskExecutionResult::TASK_NOT_FINISHED;
		}
		executor.FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}
};

TEST_CASE("TaskExecutor can execute partial tasks without busy spinning forever") {
	DuckDB db;
	Connection con {db};
	REQUIRE_NO_FAIL(con.Query("SET threads=5"));
	REQUIRE_NO_FAIL(con.Query("SET scheduler_process_partial=true"));
	TaskExecutor executor {*con.context};

	// One task per background worker (threads=5, external=1 -> 4 workers).
	for (auto i = 0; i < 4; i++) {
		executor.ScheduleTask(make_uniq<WeirdTask>(executor));
	}

	// Let each worker grab a task and enter its PROCESS_PARTIAL sleep.
	std::this_thread::sleep_for(std::chrono::milliseconds(100));

	// WorkOnTasks finds the producer queue empty (all tasks in worker hands),
	// exits its first loop, and enters `while (completed_tasks != total_tasks) {}`.
	auto finished = std::async(std::launch::async, [&] { executor.WorkOnTasks(); });

	// Kill background workers. Their in-flight tasks get re-enqueued as
	// TASK_NOT_FINISHED and stranded — WorkOnTasks is already busy-spinning
	// and never re-checks the queue.
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));

	REQUIRE(finished.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready);
}

class ChildSchedulingTaskState {
public:
	bool WaitForParentStarted() {
		unique_lock<mutex> guard(lock);
		return cv.wait_for(guard, std::chrono::seconds(5), [&]() { return parent_started; });
	}

	void AllowScheduleChild() {
		{
			lock_guard<mutex> guard(lock);
			schedule_child = true;
		}
		cv.notify_all();
	}

	void WaitUntilChildAllowed() {
		unique_lock<mutex> guard(lock);
		cv.wait(guard, [&]() { return schedule_child; });
	}

	bool WaitForChildScheduled() {
		unique_lock<mutex> guard(lock);
		return cv.wait_for(guard, std::chrono::seconds(5), [&]() { return child_scheduled; });
	}

	void AllowParentFinish() {
		{
			lock_guard<mutex> guard(lock);
			parent_finish = true;
		}
		cv.notify_all();
	}

	void WaitUntilParentFinishAllowed() {
		unique_lock<mutex> guard(lock);
		cv.wait(guard, [&]() { return parent_finish; });
	}

	void ParentStarted() {
		{
			lock_guard<mutex> guard(lock);
			parent_started = true;
		}
		cv.notify_all();
	}

	void ChildScheduled() {
		{
			lock_guard<mutex> guard(lock);
			child_scheduled = true;
		}
		cv.notify_all();
	}

private:
	mutex lock;
	std::condition_variable cv;
	bool parent_started = false;
	bool schedule_child = false;
	bool child_scheduled = false;
	bool parent_finish = false;
};

struct ScheduledChildTask : BaseExecutorTask {
	ScheduledChildTask(TaskExecutor &executor, ChildSchedulingTaskState &state_p)
	    : BaseExecutorTask(executor), state(state_p) {
	}

	void ExecuteTask() override {
	}

	ChildSchedulingTaskState &state;
};

struct ChildSchedulingTask : BaseExecutorTask {
	ChildSchedulingTask(TaskExecutor &executor, ChildSchedulingTaskState &state_p)
	    : BaseExecutorTask(executor), state(state_p) {
	}

	void ExecuteTask() override {
		state.ParentStarted();
		state.WaitUntilChildAllowed();
		executor.ScheduleTask(make_uniq<ScheduledChildTask>(executor, state));
		state.ChildScheduled();
		state.WaitUntilParentFinishAllowed();
	}

	ChildSchedulingTaskState &state;
};

TEST_CASE("TaskExecutor wakes when a running task schedules follow-up work") {
	DuckDB db;
	Connection con {db};
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	TaskExecutor executor {*con.context};
	ChildSchedulingTaskState state;

	executor.ScheduleTask(make_uniq<ChildSchedulingTask>(executor, state));
	auto worker = std::async(std::launch::async, [&] {
		shared_ptr<Task> task;
		if (!executor.GetTask(task)) {
			return false;
		}
		auto result = task->Execute(TaskExecutionMode::PROCESS_ALL);
		task.reset();
		return result == TaskExecutionResult::TASK_FINISHED;
	});
	REQUIRE(state.WaitForParentStarted());

	auto finished = std::async(std::launch::async, [&] { executor.WorkOnTasks(); });
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	state.AllowScheduleChild();
	REQUIRE(state.WaitForChildScheduled());
	state.AllowParentFinish();
	REQUIRE(worker.get());

	auto status = finished.wait_for(std::chrono::milliseconds(200));
	if (status != std::future_status::ready) {
		shared_ptr<Task> task;
		REQUIRE(executor.GetTask(task));
		auto result = task->Execute(TaskExecutionMode::PROCESS_ALL);
		task.reset();
		REQUIRE(result == TaskExecutionResult::TASK_FINISHED);
		REQUIRE(finished.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
	}
	REQUIRE(status == std::future_status::ready);
}
