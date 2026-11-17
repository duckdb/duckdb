#include "catch.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parallel/scan_affinity_queue.hpp"
#include "duckdb/parallel/scan_read_ahead.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "test_helpers.hpp"

#ifndef DUCKDB_NO_THREADS
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <thread>
#endif

using namespace duckdb;

namespace {
struct ReadyTestJob : public ScanReadAheadJob {
	explicit ReadyTestJob(idx_t id) {
		batch_index = id;
	}
	~ReadyTestJob() override {
		SettleIO();
		if (on_destroy) {
			on_destroy();
		}
	}
	optional_idx ResolveAffinity() override {
		return on_ready ? on_ready() : optional_idx();
	}
	idx_t GetMemoryUsage() const override {
		return memory_usage;
	}
	idx_t memory_usage = 0;
	std::function<optional_idx()> on_ready;
	std::function<void()> on_destroy;
};
} // namespace

namespace duckdb {
class ScanAffinityQueueTestHelper {
public:
	static void ClearHint(ScanAffinityQueue &queue, idx_t shard) {
		queue.shards[shard]->affinity.ready.store(0, std::memory_order_relaxed);
	}
};
} // namespace duckdb

TEST_CASE("Scan affinity prefers local then nearby cached assignments before unknown placement", "[api][storage]") {
	ScanAffinityQueue queue(3);
	queue.Push(0, optional_idx(), make_uniq<ReadyTestJob>(50));
	queue.Push(0, 2, make_uniq<ReadyTestJob>(30));
	queue.Push(0, 1, make_uniq<ReadyTestJob>(20));
	queue.Push(0, 0, make_uniq<ReadyTestJob>(10));
	queue.Push(0, 0, make_uniq<ReadyTestJob>(11));
	duckdb::unique_ptr<ScanReadAheadJob> next;
	for (const auto expected : {10, 11, 20, 30, 50}) {
		REQUIRE(queue.TryPop(0, next));
		REQUIRE(next->batch_index == idx_t(expected));
	}
	REQUIRE_FALSE(queue.TryPop(0, next));
}

TEST_CASE("Scan affinity keeps balanced shard ranges together", "[api][storage]") {
	const idx_t shard_count = GENERATE(idx_t(4), idx_t(6), idx_t(24), idx_t(48));
	const auto group_size = shard_count / 2;
	for (idx_t local = 0; local < shard_count; local++) {
		ScanAffinityQueue queue(shard_count);
		for (idx_t shard = 0; shard < shard_count; shard++) {
			queue.Push(0, shard, make_uniq<ReadyTestJob>(shard));
		}
		duckdb::vector<bool> seen(shard_count, false);
		duckdb::unique_ptr<ScanReadAheadJob> next;
		for (idx_t position = 0; position < shard_count; position++) {
			REQUIRE(queue.TryPop(local, next));
			REQUIRE(next->batch_index < shard_count);
			REQUIRE_FALSE(seen[next->batch_index]);
			seen[next->batch_index] = true;
			if (position == 0) {
				REQUIRE(next->batch_index == local);
			}
			REQUIRE((next->batch_index / group_size == local / group_size) == (position < group_size));
		}
		REQUIRE_FALSE(queue.TryPop(local, next));
	}
}

TEST_CASE("Scan affinity drains assignments despite stale empty hints", "[api][storage]") {
	ScanAffinityQueue queue(3);
	queue.Push(2, 2, make_uniq<ReadyTestJob>(42));
	ScanAffinityQueueTestHelper::ClearHint(queue, 2);
	duckdb::unique_ptr<ScanReadAheadJob> result;
	REQUIRE(queue.TryPop(1, result));
	REQUIRE(result->batch_index == 42);
	REQUIRE_FALSE(queue.TryPop(1, result));
}

namespace {

class TestScanRead : public AsyncTask {
public:
	explicit TestScanRead(std::function<void()> read) : read(std::move(read)) {
	}
	void Execute() override {
		read();
	}
	idx_t GetIOSize() const override {
		return 4096;
	}
	std::function<void()> read;
};

#ifndef DUCKDB_NO_THREADS
class ScanReadySignal {
public:
	void Notify() {
		std::lock_guard<std::mutex> guard(lock);
		++count;
		cv.notify_all();
	}
	bool Wait(idx_t expected) {
		std::unique_lock<std::mutex> guard(lock);
		return cv.wait_for(guard, std::chrono::seconds(10), [&]() { return count >= expected; });
	}
	idx_t Count() {
		std::lock_guard<std::mutex> guard(lock);
		return count;
	}

private:
	std::mutex lock;
	std::condition_variable cv;
	idx_t count = 0;
};

class ScanReadyReadGate {
public:
	void Read() {
		std::unique_lock<std::mutex> guard(lock);
		entered = true;
		cv.notify_all();
		if (!cv.wait_for(guard, std::chrono::seconds(10), [&]() { return released; })) {
			throw IOException("Test did not release the cooperative read");
		}
	}
	bool WaitForRead() {
		std::unique_lock<std::mutex> guard(lock);
		return cv.wait_for(guard, std::chrono::seconds(10), [&]() { return entered; });
	}
	void Release() {
		std::lock_guard<std::mutex> guard(lock);
		released = true;
		cv.notify_all();
	}

private:
	std::mutex lock;
	std::condition_variable cv;
	bool entered = false;
	bool released = false;
};

class ScanReadyCallbackTask : public Task {
public:
	explicit ScanReadyCallbackTask(ScanReadySignal &signal, bool throw_on_reschedule = false)
	    : signal(signal), throw_on_reschedule(throw_on_reschedule) {
	}
	TaskExecutionResult Execute(TaskExecutionMode) override {
		return TaskExecutionResult::TASK_FINISHED;
	}
	void Reschedule() override {
		++reschedules;
		signal.Notify();
		if (throw_on_reschedule) {
			throw InvalidInputException("injected cooperative reschedule failure");
		}
	}
	std::atomic<idx_t> reschedules {0};

private:
	ScanReadySignal &signal;
	bool throw_on_reschedule;
};

//! Declare after the scan: unwinding releases its read before the scan destructor drains I/O.
//! The optional support thread is interrupted and joined before captured state can be destroyed.
class ScanReadyReadGuard {
public:
	ScanReadyReadGuard(Connection &con, ScanReadyReadGate &gate) : con(con), gate(gate) {
	}
	~ScanReadyReadGuard() {
		gate.Release();
		if (worker.joinable()) {
			con.Interrupt();
			worker.join();
		}
	}
	void Start(std::function<void()> work) {
		worker = std::thread([this, work = std::move(work)]() {
			try {
				work();
			} catch (...) {
				error = std::current_exception();
			}
			finished.Notify();
		});
	}
	bool Join() {
		gate.Release();
		const auto completed = finished.Wait(1);
		if (!completed) {
			con.Interrupt();
		}
		worker.join();
		return completed;
	}
	std::thread::id ThreadId() const {
		return worker.get_id();
	}
	std::exception_ptr error;

private:
	Connection &con;
	ScanReadyReadGate &gate;
	ScanReadySignal finished;
	std::thread worker;
};
#endif
} // namespace

TEST_CASE("Scan affinity schedules bounded cached work without async workers", "[api][storage]") {
	DuckDB db(nullptr);
	Connection con(db);
#ifndef DUCKDB_NO_THREADS
	REQUIRE_NO_FAIL(con.Query("SET async_threads=0"));
#endif
	auto scan = ScanReadAhead::Create(*con.context, ScanReadAheadPolicy::AFFINITY, false, 3);
	idx_t produced = 0;
	const idx_t count = 513;
	auto produce = [&](duckdb::vector<duckdb::unique_ptr<AsyncTask>> &) -> duckdb::unique_ptr<ScanReadAheadJob> {
		if (produced == count) {
			return nullptr;
		}
		return make_uniq<ReadyTestJob>(produced++);
	};
	TableFunctionInput input(nullptr, nullptr, nullptr);
	duckdb::vector<bool> seen(count, false);
	for (idx_t i = 0; i < count; i++) {
		duckdb::unique_ptr<ScanReadAheadJob> job;
		REQUIRE(scan->AcquireReadyJob(*con.context, input, produce, job, i % 3) == ScanReadAheadAcquire::ACQUIRED);
		if (i == 0) {
			REQUIRE(produced > 1);
			REQUIRE(produced <= 8);
		}
		REQUIRE(job);
		REQUIRE(produced - i <= 256);
		REQUIRE_FALSE(job->io_completion);
		REQUIRE(job->batch_index < count);
		REQUIRE_FALSE(seen[job->batch_index]);
		seen[job->batch_index] = true;
	}
	duckdb::unique_ptr<ScanReadAheadJob> job;
	REQUIRE(scan->AcquireReadyJob(*con.context, input, produce, job, 0) == ScanReadAheadAcquire::EXHAUSTED);
}

TEST_CASE("Scan affinity executes pending reads when no worker is available", "[api][storage]") {
	DuckDB db(nullptr);
	Connection con(db);
#ifndef DUCKDB_NO_THREADS
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET async_threads=0"));
#endif
	bool produced = false;
	bool read = false;
	bool discovered = false;
	auto scan = ScanReadAhead::Create(*con.context, ScanReadAheadPolicy::AFFINITY, false, 2);
	auto produce = [&](duckdb::vector<duckdb::unique_ptr<AsyncTask>> &tasks) -> duckdb::unique_ptr<ScanReadAheadJob> {
		if (produced) {
			return nullptr;
		}
		produced = true;
		auto job = make_uniq<ReadyTestJob>(0);
		job->on_ready = [&]() {
			discovered = read;
			return optional_idx(1);
		};
		tasks.push_back(make_uniq<TestScanRead>([&]() { read = true; }));
		return std::move(job);
	};
	TableFunctionInput input(nullptr, nullptr, nullptr);
	duckdb::unique_ptr<ScanReadAheadJob> job;
	REQUIRE(scan->AcquireReadyJob(*con.context, input, produce, job, 0) == ScanReadAheadAcquire::ACQUIRED);
	REQUIRE(read);
	REQUIRE(discovered);
}

TEST_CASE("Scan affinity propagates readiness errors and releases reservations", "[api][storage]") {
	DuckDB db(nullptr);
	Connection con(db);
	idx_t destroyed = 0;
	auto scan = ScanReadAhead::Create(*con.context, ScanReadAheadPolicy::AFFINITY, false, 2);
	auto produce = [&](duckdb::vector<duckdb::unique_ptr<AsyncTask>> &) -> duckdb::unique_ptr<ScanReadAheadJob> {
		auto job = make_uniq<ReadyTestJob>(0);
		job->on_ready = []() -> optional_idx {
			throw InvalidInputException("readiness failed");
		};
		job->on_destroy = [&]() {
			destroyed++;
		};
		return std::move(job);
	};
	TableFunctionInput input(nullptr, nullptr, nullptr);
	duckdb::unique_ptr<ScanReadAheadJob> job;
	REQUIRE_THROWS_WITH(scan->AcquireReadyJob(*con.context, input, produce, job, 0),
	                    Catch::Contains("readiness failed"));
	scan->CancelAndDrain();
	REQUIRE(destroyed == 1);
}

TEST_CASE("Scan affinity retires pending reads after cancellation or an I/O error", "[api][storage]") {
	DuckDB db(nullptr);
	Connection con(db);
#ifndef DUCKDB_NO_THREADS
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET async_threads=0"));
#endif
	idx_t produced = 0;
	idx_t destroyed = 0;
	idx_t executed = 0;
	auto scan = ScanReadAhead::Create(*con.context, ScanReadAheadPolicy::AFFINITY, false, 2);
	auto produce = [&](duckdb::vector<duckdb::unique_ptr<AsyncTask>> &tasks) -> duckdb::unique_ptr<ScanReadAheadJob> {
		if (produced == 3) {
			return nullptr;
		}
		auto job = make_uniq<ReadyTestJob>(produced++);
		job->on_destroy = [&]() {
			destroyed++;
		};
		if (job->batch_index != 0) {
			tasks.push_back(make_uniq<TestScanRead>([&]() {
				executed++;
				throw IOException("injected pending read failure");
			}));
		}
		return std::move(job);
	};
	TableFunctionInput input(nullptr, nullptr, nullptr);
	duckdb::unique_ptr<ScanReadAheadJob> cached;
	REQUIRE(scan->AcquireReadyJob(*con.context, input, produce, cached, 0) == ScanReadAheadAcquire::ACQUIRED);
	REQUIRE(produced == 3);
	REQUIRE(cached->batch_index == 0);
	cached.reset();
	SECTION("Cancel before reads execute") {
		scan->CancelAndDrain();
		REQUIRE(executed == 0);
	}
	SECTION("An I/O error cancels the remaining read") {
		duckdb::unique_ptr<ScanReadAheadJob> job;
		REQUIRE_THROWS_WITH(scan->AcquireReadyJob(*con.context, input, produce, job, 0),
		                    Catch::Contains("injected pending read failure"));
		scan->CancelAndDrain();
		REQUIRE(executed == 1);
	}
	REQUIRE(destroyed == produced);
}

#ifndef DUCKDB_NO_THREADS
TEST_CASE("Scan affinity scheduling does not enable prefetch", "[api][storage]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=2"));
	REQUIRE_NO_FAIL(con.Query("SET async_threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET read_ahead_depth=4"));
	bool prefetch = true;
	SECTION("Reader declines prefetch") {
		prefetch = false;
	}
	SECTION("Read-ahead is disabled") {
		REQUIRE_NO_FAIL(con.Query("SET read_ahead_depth=0"));
	}
	SECTION("Async workers are disabled") {
		REQUIRE_NO_FAIL(con.Query("SET async_threads=0"));
	}
	auto scan = ScanReadAhead::Create(*con.context, ScanReadAheadPolicy::AFFINITY, prefetch, 2, 2);
	REQUIRE(scan);
	REQUIRE_FALSE(scan->PrefetchEnabled());
	REQUIRE_FALSE(scan->CanScheduleOpen(0));
	REQUIRE_FALSE(ScanReadAhead::Create(*con.context, ScanReadAheadPolicy::ORDERED, prefetch));
	idx_t produced = 0;
	auto produce = [&](duckdb::vector<duckdb::unique_ptr<AsyncTask>> &) -> duckdb::unique_ptr<ScanReadAheadJob> {
		return produced++ == 0 ? make_uniq<ReadyTestJob>(42) : nullptr;
	};
	TableFunctionInput input(nullptr, nullptr, nullptr);
	duckdb::unique_ptr<ScanReadAheadJob> job;
	REQUIRE(scan->AcquireReadyJob(*con.context, input, produce, job, 0) == ScanReadAheadAcquire::ACQUIRED);
	REQUIRE(job->batch_index == 42);
	job.reset();
	REQUIRE(scan->AcquireReadyJob(*con.context, input, produce, job, 0) == ScanReadAheadAcquire::EXHAUSTED);
}

TEST_CASE("Scan affinity preserves worker demand with shallow read-ahead", "[api][storage]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=8"));
	REQUIRE_NO_FAIL(con.Query("SET external_threads=8"));
	REQUIRE_NO_FAIL(con.Query("SET async_threads=0"));
	const auto depth = GENERATE(idx_t(1), idx_t(2));
	idx_t produced = 0;
	idx_t destroyed = 0;
	idx_t executed = 0;
	// This pipeline has only three useful workers, independent of the database's thread count.
	ScanReadAhead scan(*con.context, ScanReadAheadPolicy::AFFINITY, depth, nullptr, true, 2, 3);
	auto produce = [&](duckdb::vector<duckdb::unique_ptr<AsyncTask>> &tasks) -> duckdb::unique_ptr<ScanReadAheadJob> {
		auto job = make_uniq<ReadyTestJob>(produced++);
		job->on_destroy = [&]() {
			destroyed++;
		};
		if (job->batch_index != 0) {
			tasks.push_back(make_uniq<TestScanRead>([&]() { executed++; }));
		}
		return std::move(job);
	};
	TableFunctionInput input(nullptr, nullptr, nullptr);
	duckdb::unique_ptr<ScanReadAheadJob> cached;
	const auto acquired = scan.AcquireReadyJob(*con.context, input, produce, cached, 0);
	scan.CancelAndDrain();
	cached.reset();
	REQUIRE(acquired == ScanReadAheadAcquire::ACQUIRED);
	REQUIRE(produced == depth + 3);
	REQUIRE(executed == 0);
	REQUIRE(destroyed == produced);
}

TEST_CASE("Scan affinity releases its memory charge before producing more work", "[api][storage]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=2"));
	REQUIRE_NO_FAIL(con.Query("SET async_threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET memory_limit='256MB'"));
	REQUIRE_NO_FAIL(con.Query("SET read_ahead_depth=-1"));
	REQUIRE_NO_FAIL(con.Query("CALL enable_logging('ReadAheadBudget')"));
	auto scan = ScanReadAhead::Create(*con.context, ScanReadAheadPolicy::AFFINITY, true, 2, 2);
	REQUIRE(scan);
	idx_t produced = 0;
	auto produce = [&](duckdb::vector<duckdb::unique_ptr<AsyncTask>> &) -> duckdb::unique_ptr<ScanReadAheadJob> {
		if (produced == 17) {
			return nullptr;
		}
		auto job = make_uniq<ReadyTestJob>(produced++);
		// An estimate above the memory budget must stop production until the first job is claimed.
		job->memory_usage = job->batch_index == 0 ? 1ULL << 30 : 512;
		return std::move(job);
	};
	TableFunctionInput input(nullptr, nullptr, nullptr);
	for (idx_t i = 0; i < 17; i++) {
		duckdb::unique_ptr<ScanReadAheadJob> job;
		REQUIRE(scan->AcquireReadyJob(*con.context, input, produce, job, 0) == ScanReadAheadAcquire::ACQUIRED);
		REQUIRE(job->batch_index == i);
		if (i == 0) {
			REQUIRE(produced == 1);
		} else if (i == 1) {
			REQUIRE(produced > 2);
		}
	}
	duckdb::unique_ptr<ScanReadAheadJob> job;
	REQUIRE(scan->AcquireReadyJob(*con.context, input, produce, job, 0) == ScanReadAheadAcquire::EXHAUSTED);
	scan->CancelAndDrain();
	auto logs = con.Query("SELECT count(*) FROM duckdb_logs_parsed('ReadAheadBudget')");
	REQUIRE_NO_FAIL(*logs);
	REQUIRE(CHECK_COLUMN(logs, 0, {17}));
}
#endif

TEST_CASE("Read-ahead keeps ordered jobs in batch order", "[api][storage]") {
	DuckDB db(nullptr);
	Connection con(db);
	ScanReadAhead scan(*con.context, ScanReadAheadPolicy::ORDERED, 4, nullptr);
	idx_t produced = 0;
	auto produce = [&](duckdb::vector<duckdb::unique_ptr<AsyncTask>> &) -> duckdb::unique_ptr<ScanReadAheadJob> {
		if (produced == 19) {
			return nullptr;
		}
		auto job = make_uniq<ReadyTestJob>(produced++);
		job->on_ready = []() -> optional_idx {
			throw InternalException("Ordered jobs must not discover affinity");
		};
		return std::move(job);
	};
	TableFunctionInput input(nullptr, nullptr, nullptr);
	for (idx_t i = 0; i < 19; i++) {
		duckdb::unique_ptr<ScanReadAheadJob> job;
		REQUIRE(scan.AcquireJob(*con.context, input, produce, job) == ScanReadAheadAcquire::ACQUIRED);
		REQUIRE(job->batch_index == i);
	}
}

#ifndef DUCKDB_NO_THREADS
TEST_CASE("Scan affinity wakes every parked consumer after the final ready job", "[api][storage]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET async_threads=1"));
	ScanReadyReadGate gate;
	ScanReadySignal rescheduled;
	std::array<duckdb::shared_ptr<ScanReadyCallbackTask>, 4> tasks;
	for (auto &task : tasks) {
		task = make_shared_ptr<ScanReadyCallbackTask>(rescheduled);
	}
	idx_t produced = 0;
	auto scan = ScanReadAhead::Create(*con.context, ScanReadAheadPolicy::AFFINITY, false, 2);
	ScanReadyReadGuard release_on_exit(con, gate);
	auto produce = [&](duckdb::vector<duckdb::unique_ptr<AsyncTask>> &reads) -> duckdb::unique_ptr<ScanReadAheadJob> {
		if (produced == 2) {
			return nullptr;
		}
		auto job = make_uniq<ReadyTestJob>(produced++);
		if (job->batch_index == 0) {
			reads.push_back(make_uniq<TestScanRead>([&]() { gate.Read(); }));
		}
		return std::move(job);
	};
	TableFunctionInput synchronous(nullptr, nullptr, nullptr);
	duckdb::unique_ptr<ScanReadAheadJob> cached;
	const auto first = scan->AcquireReadyJob(*con.context, synchronous, produce, cached, 0);
	const bool cache_first = first == ScanReadAheadAcquire::ACQUIRED && cached && cached->batch_index == 1;
	const bool entered = gate.WaitForRead();
	bool parked_all = entered;
	if (entered) {
		for (auto &task : tasks) {
			InterruptState interrupt(task);
			TableFunctionInput input(nullptr, nullptr, nullptr);
			input.results_execution_mode = AsyncResultsExecutionMode::TASK_EXECUTOR;
			input.interrupt_state = &interrupt;
			duckdb::unique_ptr<ScanReadAheadJob> job;
			const auto result = scan->AcquireReadyJob(*con.context, input, produce, job, 0);
			parked_all &= result == ScanReadAheadAcquire::PARKED && !job &&
			              input.async_result.GetResultType() == AsyncResultType::BLOCKED &&
			              !input.async_result.HasTasks();
		}
	}
	gate.Release();
	const bool publication_wakes = parked_all && rescheduled.Wait(2);
	const auto before_final_claim = rescheduled.Count();
	duckdb::unique_ptr<ScanReadAheadJob> cold;
	const auto last = scan->AcquireReadyJob(*con.context, synchronous, produce, cold, 0);
	const bool cold_last = last == ScanReadAheadAcquire::ACQUIRED && cold && cold->batch_index == 0;
	duckdb::unique_ptr<ScanReadAheadJob> exhausted;
	const auto eof = scan->AcquireReadyJob(*con.context, synchronous, produce, exhausted, 0);
	const bool woke_all = rescheduled.Wait(tasks.size());
	scan->CancelAndDrain();
	cached.reset();
	cold.reset();
	scan.reset();

	REQUIRE(cache_first);
	REQUIRE(entered);
	REQUIRE(parked_all);
	REQUIRE(publication_wakes);
	// Publication and the last read's exit wake one each. The final claim/EOF must wake the other two.
	REQUIRE(before_final_claim == 2);
	REQUIRE(cold_last);
	REQUIRE(eof == ScanReadAheadAcquire::EXHAUSTED);
	REQUIRE_FALSE(exhausted);
	REQUIRE(woke_all);
	for (auto &task : tasks) {
		REQUIRE(task->reschedules.load() == 1);
	}
}

TEST_CASE("Scan affinity resumes queued reads inline after the last running read exits", "[api][storage]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET async_threads=0"));
	ScanReadyReadGate gate;
	ScanReadySignal rescheduled;
	auto task = make_shared_ptr<ScanReadyCallbackTask>(rescheduled);
	std::atomic<idx_t> first_reads {0};
	std::atomic<idx_t> second_reads {0};
	std::thread::id first_reader, second_reader;
	idx_t produced = 0;
	auto scan = ScanReadAhead::Create(*con.context, ScanReadAheadPolicy::AFFINITY, false, 2);
	auto produce = [&](duckdb::vector<duckdb::unique_ptr<AsyncTask>> &reads) -> duckdb::unique_ptr<ScanReadAheadJob> {
		if (produced == 2) {
			return nullptr;
		}
		auto job = make_uniq<ReadyTestJob>(produced++);
		if (job->batch_index == 0) {
			reads.push_back(make_uniq<TestScanRead>([&]() {
				first_reader = std::this_thread::get_id();
				++first_reads;
				gate.Read();
			}));
		} else {
			reads.push_back(make_uniq<TestScanRead>([&]() {
				second_reader = std::this_thread::get_id();
				++second_reads;
			}));
		}
		return std::move(job);
	};
	duckdb::unique_ptr<ScanReadAheadJob> first_job;
	auto first_result = ScanReadAheadAcquire::EXHAUSTED;
	ScanReadyReadGuard support(con, gate);
	support.Start([&]() {
		TableFunctionInput input(nullptr, nullptr, nullptr);
		first_result = scan->AcquireReadyJob(*con.context, input, produce, first_job, 0);
	});
	const auto support_id = support.ThreadId();
	const bool entered = gate.WaitForRead();
	InterruptState interrupt(task);
	TableFunctionInput parked_input(nullptr, nullptr, nullptr);
	parked_input.results_execution_mode = AsyncResultsExecutionMode::TASK_EXECUTOR;
	parked_input.interrupt_state = &interrupt;
	duckdb::unique_ptr<ScanReadAheadJob> parked_job;
	auto parked = ScanReadAheadAcquire::EXHAUSTED;
	if (entered) {
		parked = scan->AcquireReadyJob(*con.context, parked_input, produce, parked_job, 0);
	}
	const bool joined = support.Join();
	const bool queued_second = second_reads.load() == 0;
	const bool resumed = rescheduled.Wait(1);
	const bool first_id = first_result == ScanReadAheadAcquire::ACQUIRED && first_job && first_job->batch_index == 0;
	first_job.reset();
	TableFunctionInput resumed_input(nullptr, nullptr, nullptr);
	resumed_input.results_execution_mode = AsyncResultsExecutionMode::TASK_EXECUTOR;
	resumed_input.interrupt_state = &interrupt;
	duckdb::unique_ptr<ScanReadAheadJob> second_job;
	const auto second_result = scan->AcquireReadyJob(*con.context, resumed_input, produce, second_job, 0);
	const bool second_id =
	    second_result == ScanReadAheadAcquire::ACQUIRED && second_job && second_job->batch_index == 1;
	duckdb::unique_ptr<ScanReadAheadJob> exhausted;
	const auto eof = scan->AcquireReadyJob(*con.context, resumed_input, produce, exhausted, 0);
	scan->CancelAndDrain();
	second_job.reset();
	scan.reset();

	REQUIRE(entered);
	REQUIRE(parked == ScanReadAheadAcquire::PARKED);
	REQUIRE_FALSE(parked_job);
	REQUIRE(parked_input.async_result.GetResultType() == AsyncResultType::BLOCKED);
	REQUIRE_FALSE(parked_input.async_result.HasTasks());
	REQUIRE(joined);
	REQUIRE_FALSE(support.error);
	REQUIRE(resumed);
	REQUIRE(task->reschedules.load() == 1);
	REQUIRE(first_id);
	REQUIRE(queued_second);
	REQUIRE(second_id);
	REQUIRE(first_reads.load() == 1);
	REQUIRE(second_reads.load() == 1);
	REQUIRE(first_reader == support_id);
	REQUIRE(second_reader == std::this_thread::get_id());
	REQUIRE(eof == ScanReadAheadAcquire::EXHAUSTED);
	REQUIRE_FALSE(exhausted);
}

TEST_CASE("Scan affinity reschedule errors reach the active submitted query", "[api][storage]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET async_threads=1"));
	// Keep a real query executor alive while the fake consumer's Reschedule reports its error.
	auto pending = con.Submit("SELECT 42");
	REQUIRE_FALSE(pending->HasError());
	REQUIRE(pending->IsOpen());
	ScanReadyReadGate gate;
	ScanReadySignal rescheduled;
	auto task = make_shared_ptr<ScanReadyCallbackTask>(rescheduled, true);
	std::atomic<idx_t> destroyed {0};
	idx_t produced = 0;
	auto scan = ScanReadAhead::Create(*con.context, ScanReadAheadPolicy::AFFINITY, false, 2);
	ScanReadyReadGuard release_on_exit(con, gate);
	auto produce = [&](duckdb::vector<duckdb::unique_ptr<AsyncTask>> &reads) -> duckdb::unique_ptr<ScanReadAheadJob> {
		if (produced == 2) {
			return nullptr;
		}
		auto job = make_uniq<ReadyTestJob>(produced++);
		job->on_destroy = [&]() {
			++destroyed;
		};
		if (job->batch_index == 0) {
			reads.push_back(make_uniq<TestScanRead>([&]() { gate.Read(); }));
		}
		return std::move(job);
	};
	TableFunctionInput synchronous(nullptr, nullptr, nullptr);
	duckdb::unique_ptr<ScanReadAheadJob> cached;
	const auto first = scan->AcquireReadyJob(*con.context, synchronous, produce, cached, 0);
	const bool cache_first = first == ScanReadAheadAcquire::ACQUIRED && cached && cached->batch_index == 1;
	cached.reset();
	const bool entered = gate.WaitForRead();
	InterruptState interrupt(task);
	TableFunctionInput input(nullptr, nullptr, nullptr);
	input.results_execution_mode = AsyncResultsExecutionMode::TASK_EXECUTOR;
	input.interrupt_state = &interrupt;
	duckdb::unique_ptr<ScanReadAheadJob> job;
	auto parked = ScanReadAheadAcquire::EXHAUSTED;
	if (entered) {
		parked = scan->AcquireReadyJob(*con.context, input, produce, job, 0);
	}
	const bool parked_without_job = !job;
	gate.Release();
	const bool callback_ran = rescheduled.Wait(1);
	scan->CancelAndDrain();
	const bool query_has_error = Executor::Get(*con.context).HasError();
	string scan_error;
	try {
		scan->ThrowIfError();
	} catch (const std::exception &error) {
		scan_error = error.what();
	}
	// An already-published job is owned by the ready queue until driver teardown.
	job.reset();
	scan.reset();
	auto state = QueryResultState::NOT_READY;
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
	do {
		state = pending->ExecuteTask();
	} while (!IsObservable(state) && std::chrono::steady_clock::now() < deadline);
	const auto query_error = pending->HasError() ? pending->GetError() : string();
	pending->Close();

	REQUIRE(cache_first);
	REQUIRE(entered);
	REQUIRE(parked == ScanReadAheadAcquire::PARKED);
	REQUIRE(parked_without_job);
	REQUIRE(input.async_result.GetResultType() == AsyncResultType::BLOCKED);
	REQUIRE_FALSE(input.async_result.HasTasks());
	REQUIRE(callback_ran);
	REQUIRE(task->reschedules.load() == 1);
	REQUIRE(query_has_error);
	REQUIRE_THAT(scan_error, Catch::Contains("injected cooperative reschedule failure"));
	REQUIRE(state == QueryResultState::EXECUTION_ERROR);
	REQUIRE_THAT(query_error, Catch::Contains("injected cooperative reschedule failure"));
	REQUIRE(destroyed.load() == 2);
	REQUIRE_NO_FAIL(con.Query("SELECT 42"));
}

TEST_CASE("Scan affinity consumes cached work while a read is pending", "[api][storage]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET async_threads=1"));
	std::mutex lock;
	std::condition_variable cv;
	bool allow_read = false;
	bool read_entered = false;
	bool removal_requested = false;
	std::atomic<bool> read_done {false};
	idx_t produced = 0;
	auto scan = ScanReadAhead::Create(*con.context, ScanReadAheadPolicy::AFFINITY, false, 2);
	auto produce = [&](duckdb::vector<duckdb::unique_ptr<AsyncTask>> &tasks) -> duckdb::unique_ptr<ScanReadAheadJob> {
		if (produced == 2) {
			return nullptr;
		}
		auto job = make_uniq<ReadyTestJob>(produced++);
		if (job->batch_index == 0) {
			tasks.push_back(make_uniq<TestScanRead>([&]() {
				std::unique_lock<std::mutex> guard(lock);
				read_entered = true;
				cv.notify_all();
				if (!cv.wait_for(guard, std::chrono::seconds(10), [&]() { return allow_read; })) {
					throw IOException("Test did not release the read");
				}
				read_done = true;
			}));
			job->on_ready = [&]() {
				return read_done ? optional_idx(1) : optional_idx();
			};
		}
		return std::move(job);
	};
	TableFunctionInput input(nullptr, nullptr, nullptr);
	duckdb::unique_ptr<ScanReadAheadJob> cached;
	auto result = scan->AcquireReadyJob(*con.context, input, produce, cached, 0);
	const bool cache_first = result == ScanReadAheadAcquire::ACQUIRED && cached && cached->batch_index == 1;
	const bool pending = !read_done;
	bool remove_workers = false;
	SECTION("Async workers remain available") {
	}
	SECTION("Async worker removal requested with a read outstanding") {
		remove_workers = true;
	}
	bool entered;
	{
		std::unique_lock<std::mutex> guard(lock);
		entered = cv.wait_for(guard, std::chrono::seconds(10), [&]() { return read_entered; });
	}
	std::thread remove;
	std::exception_ptr removal_error;
	bool requested = false;
	if (remove_workers) {
		remove = std::thread([&]() {
			try {
				auto &scheduler = TaskScheduler::GetScheduler(*con.context);
				scheduler.SetAsyncThreads(0);
				{
					std::lock_guard<std::mutex> guard(lock);
					removal_requested = true;
					cv.notify_all();
				}
				scheduler.RelaunchThreads();
			} catch (...) {
				removal_error = std::current_exception();
			}
		});
		std::unique_lock<std::mutex> guard(lock);
		requested = cv.wait_for(guard, std::chrono::seconds(10), [&]() { return removal_requested; });
	}
	{
		std::lock_guard<std::mutex> guard(lock);
		allow_read = true;
		cv.notify_all();
	}
	if (remove_workers) {
		remove.join();
		REQUIRE_FALSE(removal_error);
		REQUIRE(requested);
	}
	REQUIRE(entered);
	duckdb::unique_ptr<ScanReadAheadJob> cold;
	REQUIRE(scan->AcquireReadyJob(*con.context, input, produce, cold, 1) == ScanReadAheadAcquire::ACQUIRED);
	REQUIRE(cache_first);
	REQUIRE(pending);
	REQUIRE(cold->batch_index == 0);
	REQUIRE(read_done);
}

TEST_CASE("Scan affinity teardown joins readiness publication", "[api][storage]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=2"));
	REQUIRE_NO_FAIL(con.Query("SET async_threads=1"));
	std::mutex lock;
	std::condition_variable cv;
	bool allow_read = false;
	bool publishing = false;
	bool release = false;
	std::atomic<bool> drained {false};
	idx_t produced = 0;
	auto scan = ScanReadAhead::Create(*con.context, ScanReadAheadPolicy::AFFINITY, false, 2);
	auto produce = [&](duckdb::vector<duckdb::unique_ptr<AsyncTask>> &tasks) -> duckdb::unique_ptr<ScanReadAheadJob> {
		if (produced == 2) {
			return nullptr;
		}
		auto job = make_uniq<ReadyTestJob>(produced++);
		if (job->batch_index == 0) {
			tasks.push_back(make_uniq<TestScanRead>([&]() {
				std::unique_lock<std::mutex> guard(lock);
				if (!cv.wait_for(guard, std::chrono::seconds(10), [&]() { return allow_read; })) {
					throw IOException("Test did not release the read");
				}
			}));
			job->on_ready = [&]() {
				std::unique_lock<std::mutex> guard(lock);
				publishing = true;
				cv.notify_all();
				if (!cv.wait_for(guard, std::chrono::seconds(10), [&]() { return release; })) {
					throw IOException("Test did not release publication");
				}
				return optional_idx(1);
			};
		}
		return std::move(job);
	};
	TableFunctionInput input(nullptr, nullptr, nullptr);
	duckdb::unique_ptr<ScanReadAheadJob> job;
	const auto acquired = scan->AcquireReadyJob(*con.context, input, produce, job, 0);
	bool entered;
	{
		std::unique_lock<std::mutex> guard(lock);
		// The producer sentinel has dropped; the worker must perform the last completion.
		allow_read = true;
		cv.notify_all();
		entered = cv.wait_for(guard, std::chrono::seconds(10), [&]() { return publishing; });
	}
	std::thread drain([&]() {
		scan->CancelAndDrain();
		drained = true;
		cv.notify_all();
	});
	bool returned_early;
	{
		std::unique_lock<std::mutex> guard(lock);
		returned_early = cv.wait_for(guard, std::chrono::milliseconds(50), [&]() { return drained.load(); });
		release = true;
		cv.notify_all();
	}
	drain.join();
	REQUIRE(acquired == ScanReadAheadAcquire::ACQUIRED);
	REQUIRE(job->batch_index == 1);
	REQUIRE(entered);
	REQUIRE_FALSE(returned_early);
	REQUIRE(drained);
}

TEST_CASE("Scan affinity delivers every assignment from concurrent producers", "[api][storage]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET async_threads=2"));
	bool reads = false;
	SECTION("Cached assignments") {
	}
	SECTION("Immediate asynchronous completion") {
		reads = true;
	}
	auto scan = ScanReadAhead::Create(*con.context, ScanReadAheadPolicy::AFFINITY, false, 3);
	std::atomic<idx_t> next {0};
	std::array<duckdb::vector<idx_t>, 4> received;
	std::array<std::thread, 4> workers;
	for (idx_t worker = 0; worker < workers.size(); worker++) {
		workers[worker] = std::thread([&, worker]() {
			TableFunctionInput input(nullptr, nullptr, nullptr);
			auto produce =
			    [&](duckdb::vector<duckdb::unique_ptr<AsyncTask>> &tasks) -> duckdb::unique_ptr<ScanReadAheadJob> {
				auto id = next.fetch_add(1);
				if (id >= 1001) {
					return nullptr;
				}
				auto job = make_uniq<ReadyTestJob>(id);
				auto ready = make_shared_ptr<std::atomic<bool>>(!reads);
				job->on_ready = [id, ready]() {
					if (!ready->load()) {
						throw InternalException("Assignment published before its read finished");
					}
					return optional_idx(id % 3);
				};
				if (reads) {
					tasks.push_back(make_uniq<TestScanRead>([ready]() { *ready = true; }));
				}
				return std::move(job);
			};
			while (true) {
				duckdb::unique_ptr<ScanReadAheadJob> job;
				if (scan->AcquireReadyJob(*con.context, input, produce, job, worker % 3) ==
				    ScanReadAheadAcquire::EXHAUSTED) {
					break;
				}
				received[worker].push_back(job->batch_index);
			}
		});
	}
	for (auto &worker : workers) {
		worker.join();
	}
	duckdb::vector<bool> seen(1001, false);
	idx_t count = 0;
	for (auto &ids : received) {
		for (auto id : ids) {
			REQUIRE(id < seen.size());
			REQUIRE_FALSE(seen[id]);
			seen[id] = true;
			count++;
		}
	}
	REQUIRE(count == seen.size());
}
#endif
