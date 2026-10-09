#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parallel/task_executor.hpp"

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

using namespace duckdb;

struct SleepingTask : BaseExecutorTask {
	using BaseExecutorTask::BaseExecutorTask;

	void ExecuteTask() override {
		std::this_thread::sleep_for(std::chrono::milliseconds(300));
	}
};

struct CountingTask : BaseExecutorTask {
	CountingTask(TaskExecutor &executor, std::atomic<int> &executed, std::atomic<int> &cancelled)
	    : BaseExecutorTask(executor), executed(executed), cancelled(cancelled) {
	}

	void ExecuteTask() override {
		++executed;
	}

	void Cancel() override {
		++cancelled;
	}

	std::atomic<int> &executed;
	std::atomic<int> &cancelled;
};

TEST_CASE("TaskExecutor drains tasks that are in flight when the workers are removed") {
	DuckDB db;
	Connection con {db};
	REQUIRE_NO_FAIL(con.Query("SET threads=5"));
	REQUIRE_NO_FAIL(con.Query("SET scheduler_process_partial=true"));
	TaskExecutor executor {*con.context};

	// One task per background worker (threads=5, external=1 -> 4 workers).
	for (auto i = 0; i < 4; i++) {
		executor.ScheduleTask(make_uniq<SleepingTask>(executor));
	}

	// Let each worker grab a task and enter its sleep.
	std::this_thread::sleep_for(std::chrono::milliseconds(100));

	// WorkOnTasks finds the producer queue empty, all tasks are in worker hands.
	auto finished = std::async(std::launch::async, [&] { executor.WorkOnTasks(); });

	// Kill the background workers while their tasks are still running.
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));

	REQUIRE(finished.wait_for(std::chrono::seconds(30)) == std::future_status::ready);
}

TEST_CASE("TaskExecutor runs exactly one of ExecuteTask or Cancel for every scheduled task") {
	static constexpr int TASK_COUNT = 64;

	DuckDB db;
	Connection con {db};
	std::atomic<int> executed {0};
	std::atomic<int> cancelled {0};
	{
		TaskExecutor executor {*con.context};
		for (auto i = 0; i < TASK_COUNT; i++) {
			executor.ScheduleTask(make_uniq<CountingTask>(executor, executed, cancelled));
		}
		executor.CancelAndDrain();
	}
	REQUIRE(executed + cancelled == TASK_COUNT);
}

struct SteppingTask : BaseExecutorTask {
	SteppingTask(TaskExecutor &executor, std::atomic<int> &steps, int total)
	    : BaseExecutorTask(executor), steps(steps), total(total) {
	}

	TaskExecutionResult ExecuteTaskStep() override {
		if (++steps >= total) {
			return TaskExecutionResult::TASK_FINISHED;
		}
		return TaskExecutionResult::TASK_NOT_FINISHED;
	}

	std::atomic<int> &steps;
	int total;
};

TEST_CASE("TaskExecutor drains an incremental task to completion") {
	static constexpr int STEPS = 8;

	DuckDB db;
	Connection con {db};
	std::atomic<int> steps {0};
	TaskExecutor executor {*con.context};
	executor.ScheduleTask(make_uniq<SteppingTask>(executor, steps, STEPS));
	// WorkOnTasks drains inline with PROCESS_ALL, which must loop the steps to completion
	executor.WorkOnTasks();
	REQUIRE(steps == STEPS);
}
