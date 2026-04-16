#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include <thread>

using namespace duckdb;

namespace {

struct CountingTask : public Task {
	explicit CountingTask(atomic<idx_t> &executed_p) : executed(executed_p) {
	}

	TaskExecutionResult Execute(TaskExecutionMode) override {
		executed++;
		return TaskExecutionResult::TASK_FINISHED;
	}

	atomic<idx_t> &executed;
};

struct SchedulerCounters {
	atomic<idx_t> factory_calls;
	atomic<idx_t> create_producer_calls;
	atomic<idx_t> schedule_task_calls;
	atomic<idx_t> get_task_calls;
	atomic<idx_t> set_threads_calls;
	atomic<idx_t> relaunch_threads_calls;
};

class ForwardingTaskScheduler : public TaskScheduler {
public:
	ForwardingTaskScheduler(DatabaseInstance &db, SchedulerCounters &counters_p)
	    : TaskScheduler(db), counters(counters_p), builtin(CreateBuiltinTaskScheduler(db)) {
	}

	unique_ptr<ProducerToken> CreateProducer() override {
		counters.create_producer_calls++;
		return builtin->CreateProducer();
	}

	void ScheduleTask(ProducerToken &producer, shared_ptr<Task> task) override {
		counters.schedule_task_calls++;
		builtin->ScheduleTask(producer, std::move(task));
	}

	void ScheduleTasks(ProducerToken &producer, vector<shared_ptr<Task>> &tasks) override {
		builtin->ScheduleTasks(producer, tasks);
	}

	bool GetTaskFromProducer(ProducerToken &token, shared_ptr<Task> &task) override {
		counters.get_task_calls++;
		return builtin->GetTaskFromProducer(token, task);
	}

	void ExecuteForever(atomic<bool> *marker) override {
		builtin->ExecuteForever(marker);
	}

	idx_t ExecuteTasks(atomic<bool> *marker, idx_t max_tasks) override {
		return builtin->ExecuteTasks(marker, max_tasks);
	}

	void ExecuteTasks(idx_t max_tasks) override {
		builtin->ExecuteTasks(max_tasks);
	}

	void SetThreads(idx_t total_threads, idx_t external_threads) override {
		counters.set_threads_calls++;
		builtin->SetThreads(total_threads, external_threads);
	}

	void RelaunchThreads() override {
		counters.relaunch_threads_calls++;
		builtin->RelaunchThreads();
	}

	int32_t NumberOfThreads() override {
		return builtin->NumberOfThreads();
	}

	idx_t GetNumberOfTasks() const override {
		return builtin->GetNumberOfTasks();
	}

	idx_t GetProducerCount() const override {
		return builtin->GetProducerCount();
	}

	idx_t GetTaskCountForProducer(ProducerToken &token) const override {
		return builtin->GetTaskCountForProducer(token);
	}

	void Signal(idx_t n) override {
		builtin->Signal(n);
	}

	void SetAllocatorFlushTreshold(idx_t threshold) override {
		builtin->SetAllocatorFlushTreshold(threshold);
	}

	void SetAllocatorBackgroundThreads(bool enable) override {
		builtin->SetAllocatorBackgroundThreads(enable);
	}

private:
	SchedulerCounters &counters;
	unique_ptr<TaskScheduler> builtin;
};

} // namespace

void run_query_multiple_times(duckdb::unique_ptr<string> query, duckdb::unique_ptr<Connection> con) {
	for (int i = 0; i < 10; ++i) {
		auto result = con->Query(*query);
	}
}

void change_thread_counts(duckdb::DuckDB &db) {
	auto con = Connection(db);
	for (int i = 0; i < 10; ++i) {
		con.Query("SET threads=10");
		con.Query("SET threads=1");
	}
}

// NumberOfThreads acquired the same lock as RelaunchThreads
// NumberOfThreads is waiting for the lock
// RelaunchThreads is waiting on the thread to finish, while holding the lock
TEST_CASE("Test deadlock issue between NumberOfThreads and RelaunchThreads", "[api]") {
	duckdb::DuckDB db(nullptr);

	int thread_count = 10;
	std::vector<std::thread> threads(thread_count);

	// This query will hit NumberOfThreads because it uses the RadixPartitionedHashtable
	for (int i = 0; i < thread_count; ++i) {
		auto query = make_uniq<string>(R"(
			WITH dataset AS (
			  SELECT * FROM (VALUES
				(1, 'Alice'),
				(2, 'Bob'),
				(3, 'Alice'),
				(4, 'Carol')
			  ) AS t(id, name)
			)
			SELECT DISTINCT name FROM dataset;
		)");

		threads[i] = std::thread(run_query_multiple_times, std::move(query), make_uniq<Connection>(db));
	}

	// Fire off queries that change the thread count,
	// causing us to relaunch the worker threads on every subsequent query.
	change_thread_counts(db);

	for (int i = 0; i < thread_count; ++i) {
		threads[i].join();
	}
}

TEST_CASE("Test database maximum_threads argument", "[api]") {
	// default is number of hw threads
	// FIXME: not yet
	{
		DuckDB db(nullptr);
		auto file_system = make_uniq<VirtualFileSystem>();
		REQUIRE(db.NumberOfThreads() == DBConfig().GetSystemMaxThreads(*file_system));
	}
	// but we can set another value
	{
		DBConfig config;
		config.options.maximum_threads = 10;
		DuckDB db(nullptr, &config);
		REQUIRE(db.NumberOfThreads() == 10);
	}
	// zero is not erlaubt
	{
		DBConfig config;
		config.options.maximum_threads = 0;
		DuckDB db;
		REQUIRE_THROWS(db = DuckDB(nullptr, &config));
	}
}

TEST_CASE("Test external threads", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &config = DBConfig::GetConfig(*db.instance);
	auto options = config.GetOptions();

	con.Query("SET threads=13");
	REQUIRE(config.options.maximum_threads == 13);
	REQUIRE(db.NumberOfThreads() == 13);
	con.Query("SET external_threads=13");
	REQUIRE(Settings::Get<ExternalThreadsSetting>(config) == 13);
	REQUIRE(db.NumberOfThreads() == 13);

	con.Query("SET external_threads=0");
	REQUIRE(Settings::Get<ExternalThreadsSetting>(config) == 0);
	REQUIRE(db.NumberOfThreads() == 13);

	auto res = con.Query("SET external_threads=-1");
	REQUIRE(res->HasError());
	REQUIRE(StringUtil::Contains(res->GetError(), "out of range"));

	res = con.Query("SET external_threads=14");
	REQUIRE(res->HasError());
	REQUIRE(StringUtil::Contains(res->GetError(), "smaller"));

	con.Query("SET external_threads=5");
	REQUIRE(Settings::Get<ExternalThreadsSetting>(config) == 5);
	REQUIRE(db.NumberOfThreads() == 13);

	con.Query("RESET external_threads");
	REQUIRE(Settings::Get<ExternalThreadsSetting>(config) == 1);
	REQUIRE(db.NumberOfThreads() == 13);

	con.Query("RESET threads");
	auto file_system = make_uniq<VirtualFileSystem>();
	REQUIRE(config.options.maximum_threads == DBConfig().GetSystemMaxThreads(*file_system));
	REQUIRE(db.NumberOfThreads() == DBConfig().GetSystemMaxThreads(*file_system));
}

TEST_CASE("Test injecting a custom task scheduler", "[api]") {
	SchedulerCounters counters;
	counters.factory_calls = 0;
	counters.create_producer_calls = 0;
	counters.schedule_task_calls = 0;
	counters.get_task_calls = 0;
	counters.set_threads_calls = 0;
	counters.relaunch_threads_calls = 0;

	DBConfig config;
	config.options.maximum_threads = 1;
	config.task_scheduler_create = [&](DatabaseInstance &db_instance) -> unique_ptr<TaskScheduler> {
		counters.factory_calls++;
		return make_uniq<ForwardingTaskScheduler>(db_instance, counters);
	};

	DuckDB db(nullptr, &config);
	REQUIRE(counters.factory_calls.load() == 1);
	REQUIRE(counters.set_threads_calls.load() >= 1);
	REQUIRE(counters.relaunch_threads_calls.load() >= 1);

	auto producer_calls_before = counters.create_producer_calls.load();
	auto schedule_calls_before = counters.schedule_task_calls.load();
	auto get_task_calls_before = counters.get_task_calls.load();

	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	auto token = scheduler.CreateProducer();

	atomic<idx_t> executed;
	executed = 0;
	scheduler.ScheduleTask(*token, make_shared_ptr<CountingTask>(executed));

	REQUIRE(scheduler.GetProducerCount() == 1);
	REQUIRE(scheduler.GetNumberOfTasks() == 1);
	REQUIRE(scheduler.GetTaskCountForProducer(*token) == 1);

	shared_ptr<Task> task;
	REQUIRE(scheduler.GetTaskFromProducer(*token, task));
	REQUIRE(scheduler.GetNumberOfTasks() == 0);
	REQUIRE(scheduler.GetTaskCountForProducer(*token) == 0);
	REQUIRE(task->Execute(TaskExecutionMode::PROCESS_ALL) == TaskExecutionResult::TASK_FINISHED);
	REQUIRE(executed.load() == 1);

	REQUIRE(counters.create_producer_calls.load() == producer_calls_before + 1);
	REQUIRE(counters.schedule_task_calls.load() == schedule_calls_before + 1);
	REQUIRE(counters.get_task_calls.load() == get_task_calls_before + 1);
}

TEST_CASE("Test rejecting null custom task scheduler factory", "[api]") {
	DBConfig config;
	config.task_scheduler_create = [](DatabaseInstance &) -> unique_ptr<TaskScheduler> {
		return nullptr;
	};

	try {
		DuckDB db(nullptr, &config);
		FAIL("Expected null custom task scheduler to be rejected");
	} catch (const InternalException &ex) {
		REQUIRE(StringUtil::Contains(ex.what(), "Custom task scheduler factory returned null"));
	}
}

#ifdef DUCKDB_NO_THREADS
TEST_CASE("Test scheduling with no threads", "[api]") {
	DuckDB db(nullptr);
	Connection con1(db);
	Connection con2(db);

	const auto query_1 = con1.PendingQuery("SELECT 42");
	const auto query_2 = con2.PendingQuery("SELECT 42");
	// Get the completed pipelines. Because "executeTask" was never called, there should be no completed pipelines.
	auto query_1_pipelines = con1.context->GetExecutor().GetCompletedPipelines();
	REQUIRE((query_1_pipelines == 0));

	// Execute the second query
	REQUIRE_NO_FAIL(query_2->Execute());

	// And even after that, there should still be no completed pipelines for the first query.
	query_1_pipelines = con1.context->GetExecutor().GetCompletedPipelines();
	REQUIRE((query_1_pipelines == 0));
	REQUIRE_NO_FAIL(query_1->Execute());
}
#endif
