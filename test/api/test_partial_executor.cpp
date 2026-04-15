#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parallel/task_executor.hpp"

#include <chrono>
#include <thread>

using namespace duckdb;

struct WeirdTask : BaseExecutorTask {
	WeirdTask(TaskExecutor &executor_p, shared_ptr<std::atomic_bool> complete_now_p,
	          shared_ptr<std::atomic<idx_t>> finished_count_p)
	    : BaseExecutorTask(executor_p), complete_now(std::move(complete_now_p)),
	      finished_count(std::move(finished_count_p)) {
	}

	void ExecuteTask() override {
		throw InternalException("Simple ExecuteTask should never be called!");
	}

	TaskExecutionResult Execute(TaskExecutionMode mode) override {
		if (executor.HasError()) {
			executor.FinishTask();
			return TaskExecutionResult::TASK_FINISHED;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));

		if (!*complete_now && mode == TaskExecutionMode::PROCESS_PARTIAL) {
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
			return TaskExecutionResult::TASK_NOT_FINISHED;
		}
		++*finished_count;
		executor.FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<std::atomic_bool> complete_now;
	shared_ptr<std::atomic<idx_t>> finished_count;
};

TEST_CASE("TaskExecutor can execute partial tasks without busy spinning forever") {
	DuckDB db {};
	Connection con {db};
	REQUIRE_NO_FAIL(con.Query("SET threads=5"));
	REQUIRE_NO_FAIL(con.Query("SET scheduler_process_partial=true"));
	TaskExecutor executor {*con.context};

	auto complete_now = make_shared_ptr<std::atomic_bool>(false);
	auto finished_count = make_shared_ptr<std::atomic<idx_t>>(0);
	constexpr idx_t TASK_COUNT = 20;
	for (idx_t i = 0; i < TASK_COUNT; i++) {
		executor.ScheduleTask(make_uniq<WeirdTask>(executor, complete_now, finished_count));
	}

	// let background workers pick up the tasks and start cycling them in PROCESS_PARTIAL mode
	std::this_thread::sleep_for(std::chrono::milliseconds(100));

	// shrink the pool to zero background workers (external_threads defaults to 1, so threads=1
	// means requested_thread_count=0). Any task currently in flight gets re-enqueued as
	// TASK_NOT_FINISHED before the worker exits, so the queue now holds tasks with nobody to
	// drive them to completion.
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	complete_now->store(true);

	// poll up to 5 seconds for all tasks to complete - without a worker or WorkOnTasks driving
	// progress, the re-enqueued tasks are stranded and the count never reaches TASK_COUNT
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	while (std::chrono::steady_clock::now() < deadline && finished_count->load() != TASK_COUNT) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	REQUIRE(finished_count->load() == TASK_COUNT);
}
