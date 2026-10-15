#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parallel/task_executor.hpp"

#include <chrono>
#include <thread>

using namespace duckdb;

//! Yields after every step until stopped, like a long-running query under scheduler_process_partial
struct EndlessTask : BaseExecutorTask {
	EndlessTask(TaskExecutor &executor, const atomic<bool> &stop) : BaseExecutorTask(executor), stop(stop) {
	}

	TaskExecutionResult ExecuteTaskStep() override {
		return stop ? TaskExecutionResult::TASK_FINISHED : TaskExecutionResult::TASK_NOT_FINISHED;
	}

	const atomic<bool> &stop;
};

//! Finishes in a single step, like a short query
struct ShortTask : BaseExecutorTask {
	ShortTask(TaskExecutor &executor, atomic<bool> &done) : BaseExecutorTask(executor), done(done) {
	}

	void ExecuteTask() override {
		done = true;
	}

	atomic<bool> &done;
};

TEST_CASE("An older query is not starved by newer queries", "[scheduler]") {
	DuckDB db;
	Connection con(db);
	// a single worker thread, which puts unfinished tasks back in the queue after every step
	REQUIRE_NO_FAIL(con.Query("SET threads=2"));
	REQUIRE_NO_FAIL(con.Query("SET scheduler_process_partial=true"));

	// every TaskExecutor has its own producer, like the Executor of a query
	TaskExecutor old_query(*con.context);
	atomic<bool> stop {false};
	vector<unique_ptr<TaskExecutor>> new_queries;
	for (idx_t i = 0; i < 3; i++) {
		new_queries.push_back(make_uniq<TaskExecutor>(*con.context));
		new_queries.back()->ScheduleTask(make_uniq<EndlessTask>(*new_queries.back(), stop));
	}

	// the newer queries keep the worker busy, but the older query's short task must still get a turn
	atomic<bool> done {false};
	old_query.ScheduleTask(make_uniq<ShortTask>(old_query, done));
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	while (!done && std::chrono::steady_clock::now() < deadline) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	CHECK(done);

	stop = true;
	old_query.WorkOnTasks();
	for (auto &new_query : new_queries) {
		new_query->WorkOnTasks();
	}
}
