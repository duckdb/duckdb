#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parallel/task_executor.hpp"

#include <chrono>
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
	auto finished = std::async(std::launch::async, [&] {
		executor.WorkOnTasks();
	});

	// Kill background workers. Their in-flight tasks get re-enqueued as
	// TASK_NOT_FINISHED and stranded — WorkOnTasks is already busy-spinning
	// and never re-checks the queue.
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));

	REQUIRE(finished.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready);
}
