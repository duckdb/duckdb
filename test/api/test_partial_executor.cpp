#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parallel/task_executor.hpp"

#include <chrono>
#include <future>
#include <thread>

using namespace duckdb;

struct WeirdTask : BaseExecutorTask {
	WeirdTask(TaskExecutor &executor_p, shared_ptr<std::atomic_bool> complete_now_p)
	    : BaseExecutorTask(executor_p), complete_now(std::move(complete_now_p)) {
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
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
			return TaskExecutionResult::TASK_NOT_FINISHED;
		}
		executor.FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<std::atomic_bool> complete_now;
};

TEST_CASE("TaskExecutor can execute partial tasks without busy spinning forever") {
	DuckDB db {};
	Connection con {db};
	REQUIRE_NO_FAIL(con.Query("SET threads=40"));
	REQUIRE_NO_FAIL(con.Query("SET scheduler_process_partial=true"));
	TaskExecutor executor {*con.context};

	shared_ptr<std::atomic_bool> complete_now = make_shared_ptr<std::atomic_bool>(false);
	for (auto i = 0; i < 20; i += 1) {
		executor.ScheduleTask(make_uniq<WeirdTask>(executor, complete_now));
	}
	std::this_thread::sleep_for(std::chrono::milliseconds(100));

	auto finished = std::async(std::launch::async, [&] {
		executor.WorkOnTasks();
		return true;
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	complete_now->store(true);
	REQUIRE(finished.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
}
