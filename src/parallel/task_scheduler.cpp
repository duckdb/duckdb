#include "duckdb/parallel/task_scheduler.hpp"

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/block_allocator.hpp"
#ifndef DUCKDB_NO_THREADS
#include "concurrentqueue.h"
#include "duckdb/common/thread.hpp"
#include "lightweightsemaphore.h"

#include <thread>
#else
#include <queue>
#endif

#if defined(_WIN32)
#include <windows.h>
#elif defined(__GNUC__)
#include <sched.h>
#include <unistd.h>
#if defined(__GLIBC__)
#include <pthread.h>
#endif
#endif

namespace duckdb {

class BuiltinProducerToken;
class BuiltinTaskScheduler;

struct SchedulerThread {
#ifndef DUCKDB_NO_THREADS
	explicit SchedulerThread(unique_ptr<thread> thread_p) : internal_thread(std::move(thread_p)) {
	}

	unique_ptr<thread> internal_thread;
#endif
};

#ifndef DUCKDB_NO_THREADS
typedef duckdb_moodycamel::ConcurrentQueue<shared_ptr<Task>> concurrent_queue_t;
typedef duckdb_moodycamel::LightweightSemaphore lightweight_semaphore_t;

struct ConcurrentQueue {
	ConcurrentQueue() : tasks_in_queue(0) {
	}

	lightweight_semaphore_t semaphore;

	void Enqueue(BuiltinProducerToken &token, shared_ptr<Task> task);
	void EnqueueBulk(BuiltinProducerToken &token, vector<shared_ptr<Task>> &tasks);
	bool DequeueFromProducer(BuiltinProducerToken &token, shared_ptr<Task> &task);
	bool Dequeue(shared_ptr<Task> &task);
	idx_t GetTasksInQueue() const;
	idx_t GetApproxSize() const;
	idx_t GetProducerCount() const;
	idx_t GetTaskCountForProducer(BuiltinProducerToken &token) const;
	concurrent_queue_t &GetQueue() {
		return q;
	}

private:
	concurrent_queue_t q;
	atomic<idx_t> tasks_in_queue;
};

struct QueueProducerToken {
	explicit QueueProducerToken(ConcurrentQueue &queue) : queue_token(queue.GetQueue()) {
	}

	duckdb_moodycamel::ProducerToken queue_token;
};

#else
struct ConcurrentQueue {
	reference_map_t<QueueProducerToken, std::queue<shared_ptr<Task>>> q;
	mutable mutex qlock;

	void Enqueue(BuiltinProducerToken &token, shared_ptr<Task> task);
	void EnqueueBulk(BuiltinProducerToken &token, vector<shared_ptr<Task>> &tasks);
	bool DequeueFromProducer(BuiltinProducerToken &token, shared_ptr<Task> &task);
	bool Dequeue(shared_ptr<Task> &task);
	idx_t GetTasksInQueue() const;
	idx_t GetApproxSize() const;
	idx_t GetProducerCount() const;
	idx_t GetTaskCountForProducer(BuiltinProducerToken &token) const;
};

struct QueueProducerToken {
	explicit QueueProducerToken(ConcurrentQueue &queue) : queue(&queue) {
	}

	~QueueProducerToken() {
		lock_guard<mutex> lock(queue->qlock);
		queue->q.erase(*this);
	}

private:
	ConcurrentQueue *queue;
};
#endif

class BuiltinProducerToken : public ProducerToken {
public:
	explicit BuiltinProducerToken(unique_ptr<QueueProducerToken> token_p) : token(std::move(token_p)) {
	}

	~BuiltinProducerToken() override;

	mutex producer_lock;
	unique_ptr<QueueProducerToken> token;
};

static BuiltinProducerToken &CastProducerToken(ProducerToken &token) {
	auto *builtin_token = dynamic_cast<BuiltinProducerToken *>(&token);
	if (!builtin_token) {
		throw InternalException("Mismatching producer token for builtin task scheduler");
	}
	return *builtin_token;
}

class BuiltinTaskScheduler : public TaskScheduler {
	// timeout for semaphore wait, default 5ms
	constexpr static int64_t TASK_TIMEOUT_USECS = 5000;

public:
	explicit BuiltinTaskScheduler(DatabaseInstance &db);
	~BuiltinTaskScheduler() override;

	unique_ptr<ProducerToken> CreateProducer() override;
	void ScheduleTask(ProducerToken &token, shared_ptr<Task> task) override;
	void ScheduleTasks(ProducerToken &producer, vector<shared_ptr<Task>> &tasks) override;
	bool GetTaskFromProducer(ProducerToken &token, shared_ptr<Task> &task) override;
	void ExecuteForever(atomic<bool> *marker) override;
	idx_t ExecuteTasks(atomic<bool> *marker, idx_t max_tasks) override;
	void ExecuteTasks(idx_t max_tasks) override;
	int32_t NumberOfThreads() override;
	idx_t GetNumberOfTasks() const override;
	idx_t GetProducerCount() const override;
	idx_t GetTaskCountForProducer(ProducerToken &token) const override;
	void SetThreads(idx_t total_threads, idx_t external_threads) override;
	void SetAllocatorFlushTreshold(idx_t threshold) override;
	void SetAllocatorBackgroundThreads(bool enable) override;
	void Signal(idx_t n) override;
	void RelaunchThreads() override;

private:
	void RelaunchThreadsInternal(int32_t n, bool destroy);

private:
	unique_ptr<ConcurrentQueue> queue;
	mutex thread_lock;
	vector<unique_ptr<SchedulerThread>> threads;
	vector<unique_ptr<atomic<bool>>> markers;
	atomic<idx_t> allocator_flush_threshold;
	atomic<bool> allocator_background_threads;
	atomic<int32_t> requested_thread_count;
	atomic<int32_t> current_thread_count;
};

BuiltinProducerToken::~BuiltinProducerToken() = default;

#ifndef DUCKDB_NO_THREADS
void ConcurrentQueue::Enqueue(BuiltinProducerToken &token, shared_ptr<Task> task) {
	lock_guard<mutex> producer_lock(token.producer_lock);
	task->token = token;
	if (q.enqueue(token.token->queue_token, std::move(task))) {
		++tasks_in_queue;
		semaphore.signal();
	} else {
		throw InternalException("Could not schedule task!");
	}
}

void ConcurrentQueue::EnqueueBulk(BuiltinProducerToken &token, vector<shared_ptr<Task>> &tasks) {
	typedef std::make_signed<std::size_t>::type ssize_t;
	lock_guard<mutex> producer_lock(token.producer_lock);
	for (auto &task : tasks) {
		task->token = token;
	}
	if (q.enqueue_bulk(token.token->queue_token, std::make_move_iterator(tasks.begin()), tasks.size())) {
		tasks_in_queue += tasks.size();
		semaphore.signal(NumericCast<ssize_t>(tasks.size()));
	} else {
		throw InternalException("Could not schedule tasks!");
	}
}

bool ConcurrentQueue::DequeueFromProducer(BuiltinProducerToken &token, shared_ptr<Task> &task) {
	lock_guard<mutex> producer_lock(token.producer_lock);
	if (!q.try_dequeue_from_producer(token.token->queue_token, task)) {
		return false;
	}
	--tasks_in_queue;
	return true;
}

bool ConcurrentQueue::Dequeue(shared_ptr<Task> &task) {
	if (!q.try_dequeue(task)) {
		return false;
	}
	--tasks_in_queue;
	return true;
}

idx_t ConcurrentQueue::GetTasksInQueue() const {
	return tasks_in_queue;
}

idx_t ConcurrentQueue::GetApproxSize() const {
	return q.size_approx();
}

idx_t ConcurrentQueue::GetProducerCount() const {
	return q.size_producers_approx();
}

idx_t ConcurrentQueue::GetTaskCountForProducer(BuiltinProducerToken &token) const {
	lock_guard<mutex> producer_lock(token.producer_lock);
	return q.size_producer_approx(token.token->queue_token);
}

#else
void ConcurrentQueue::Enqueue(BuiltinProducerToken &token, shared_ptr<Task> task) {
	lock_guard<mutex> lock(qlock);
	task->token = token;
	q[std::ref(*token.token)].push(std::move(task));
}

void ConcurrentQueue::EnqueueBulk(BuiltinProducerToken &token, vector<shared_ptr<Task>> &tasks) {
	lock_guard<mutex> lock(qlock);
	for (auto &task : tasks) {
		task->token = token;
		q[std::ref(*token.token)].push(std::move(task));
	}
}

bool ConcurrentQueue::DequeueFromProducer(BuiltinProducerToken &token, shared_ptr<Task> &task) {
	lock_guard<mutex> lock(qlock);
	D_ASSERT(!q.empty());

	const auto it = q.find(std::ref(*token.token));
	if (it == q.end() || it->second.empty()) {
		return false;
	}

	task = std::move(it->second.front());
	it->second.pop();

	return true;
}

bool ConcurrentQueue::Dequeue(shared_ptr<Task> &task) {
	throw InternalException("Global dequeue not supported for no threads queue");
}

idx_t ConcurrentQueue::GetTasksInQueue() const {
	lock_guard<mutex> lock(qlock);
	idx_t task_count = 0;
	for (auto &producer : q) {
		task_count += producer.second.size();
	}
	return task_count;
}

idx_t ConcurrentQueue::GetApproxSize() const {
	return GetTasksInQueue();
}

idx_t ConcurrentQueue::GetProducerCount() const {
	lock_guard<mutex> lock(qlock);
	return q.size();
}

idx_t ConcurrentQueue::GetTaskCountForProducer(BuiltinProducerToken &token) const {
	lock_guard<mutex> lock(qlock);
	const auto it = q.find(std::ref(*token.token));
	if (it == q.end()) {
		return 0;
	}
	return it->second.size();
}
#endif

ProducerToken::~ProducerToken() {
}

TaskScheduler::TaskScheduler(DatabaseInstance &db_p) : db(db_p) {
}

TaskScheduler::~TaskScheduler() {
}

TaskScheduler &TaskScheduler::GetScheduler(ClientContext &context) {
	return TaskScheduler::GetScheduler(DatabaseInstance::GetDatabase(context));
}

TaskScheduler &TaskScheduler::GetScheduler(DatabaseInstance &db) {
	return db.GetScheduler();
}

DatabaseInstance &TaskScheduler::GetDatabase() const {
	return db;
}

TaskExecutionMode TaskScheduler::GetWorkerExecutionMode() const {
	auto &config = DBConfig::GetConfig(GetDatabase());
	return Settings::Get<SchedulerProcessPartialSetting>(config) ? TaskExecutionMode::PROCESS_PARTIAL
	                                                             : TaskExecutionMode::PROCESS_ALL;
}

BuiltinTaskScheduler::BuiltinTaskScheduler(DatabaseInstance &db)
    : TaskScheduler(db), queue(make_uniq<ConcurrentQueue>()),
      allocator_flush_threshold(db.config.options.allocator_flush_threshold),
      allocator_background_threads(Settings::Get<AllocatorBackgroundThreadsSetting>(db)), requested_thread_count(0),
      current_thread_count(1) {
	SetAllocatorBackgroundThreads(allocator_background_threads);
}

BuiltinTaskScheduler::~BuiltinTaskScheduler() {
#ifndef DUCKDB_NO_THREADS
	try {
		RelaunchThreadsInternal(0, true);
	} catch (...) {
		// nothing we can do in the destructor if this fails
	}
#endif
}

unique_ptr<ProducerToken> BuiltinTaskScheduler::CreateProducer() {
	auto token = make_uniq<QueueProducerToken>(*queue);
	return make_uniq<BuiltinProducerToken>(std::move(token));
}

void BuiltinTaskScheduler::ScheduleTask(ProducerToken &token, shared_ptr<Task> task) {
	// Enqueue a task for the given producer token and signal any sleeping threads
	queue->Enqueue(CastProducerToken(token), std::move(task));
}

void BuiltinTaskScheduler::ScheduleTasks(ProducerToken &producer, vector<shared_ptr<Task>> &tasks) {
	queue->EnqueueBulk(CastProducerToken(producer), tasks);
}

bool BuiltinTaskScheduler::GetTaskFromProducer(ProducerToken &token, shared_ptr<Task> &task) {
	return queue->DequeueFromProducer(CastProducerToken(token), task);
}

void BuiltinTaskScheduler::ExecuteForever(atomic<bool> *marker) {
#ifndef DUCKDB_NO_THREADS
	static constexpr const int64_t INITIAL_FLUSH_WAIT = 500000; // initial wait time of 0.5s (in mus) before flushing

	const auto &block_allocator = BlockAllocator::Get(GetDatabase());

	shared_ptr<Task> task;
	// loop until the marker is set to false
	while (*marker) {
		if (!block_allocator.SupportsFlush()) {
			// allocator can't flush, just start an untimed wait
			queue->semaphore.wait();
		} else if (!queue->semaphore.wait(INITIAL_FLUSH_WAIT)) {
			// allocator can flush, we flush this threads outstanding allocations after it was idle for 0.5s
			block_allocator.ThreadFlush(allocator_background_threads, allocator_flush_threshold,
			                            NumericCast<idx_t>(requested_thread_count.load()));
			auto decay_delay = Allocator::DecayDelay();
			if (!decay_delay.IsValid()) {
				// no decay delay specified - just wait
				queue->semaphore.wait();
			} else {
				if (!queue->semaphore.wait(UnsafeNumericCast<int64_t>(decay_delay.GetIndex()) * 1000000 -
				                           INITIAL_FLUSH_WAIT)) {
					// in total, the thread was idle for the entire decay delay (note: seconds converted to mus)
					// mark it as idle and start an untimed wait
					Allocator::ThreadIdle();
					queue->semaphore.wait();
				}
			}
		}
		if (queue->Dequeue(task)) {
			auto execute_result = task->Execute(GetWorkerExecutionMode());

			switch (execute_result) {
			case TaskExecutionResult::TASK_FINISHED:
			case TaskExecutionResult::TASK_ERROR:
				task.reset();
				break;
			case TaskExecutionResult::TASK_NOT_FINISHED: {
				// task is not finished - reschedule immediately
				auto &token = *task->token;
				queue->Enqueue(CastProducerToken(token), std::move(task));
				break;
			}
			case TaskExecutionResult::TASK_BLOCKED:
				task->Deschedule();
				task.reset();
				break;
			}
		} else if (queue->GetTasksInQueue() > 0) {
			// failed to dequeue but there are still tasks remaining - signal again to retry
			queue->semaphore.signal(1);
		}
	}
	// this thread will exit, flush all of its outstanding allocations
	if (block_allocator.SupportsFlush()) {
		block_allocator.ThreadFlush(allocator_background_threads, 0, NumericCast<idx_t>(requested_thread_count.load()));
		Allocator::ThreadIdle();
	}
#else
	throw NotImplementedException("DuckDB was compiled without threads! Background thread loop is not allowed.");
#endif
}

idx_t BuiltinTaskScheduler::ExecuteTasks(atomic<bool> *marker, idx_t max_tasks) {
#ifndef DUCKDB_NO_THREADS
	idx_t completed_tasks = 0;
	// loop until the marker is set to false
	while (*marker && completed_tasks < max_tasks) {
		shared_ptr<Task> task;
		if (!queue->Dequeue(task)) {
			return completed_tasks;
		}
		auto execute_result = task->Execute(TaskExecutionMode::PROCESS_ALL);

		switch (execute_result) {
		case TaskExecutionResult::TASK_FINISHED:
		case TaskExecutionResult::TASK_ERROR:
			task.reset();
			completed_tasks++;
			break;
		case TaskExecutionResult::TASK_NOT_FINISHED:
			throw InternalException("Task should not return TASK_NOT_FINISHED in PROCESS_ALL mode");
		case TaskExecutionResult::TASK_BLOCKED:
			task->Deschedule();
			task.reset();
			break;
		}
	}
	return completed_tasks;
#else
	throw NotImplementedException("DuckDB was compiled without threads! Background thread loop is not allowed.");
#endif
}

void BuiltinTaskScheduler::ExecuteTasks(idx_t max_tasks) {
#ifndef DUCKDB_NO_THREADS
	shared_ptr<Task> task;
	for (idx_t i = 0; i < max_tasks; i++) {
		queue->semaphore.wait(TASK_TIMEOUT_USECS);
		if (!queue->Dequeue(task)) {
			return;
		}
		try {
			auto execute_result = task->Execute(TaskExecutionMode::PROCESS_ALL);
			switch (execute_result) {
			case TaskExecutionResult::TASK_FINISHED:
			case TaskExecutionResult::TASK_ERROR:
				task.reset();
				break;
			case TaskExecutionResult::TASK_NOT_FINISHED:
				throw InternalException("Task should not return TASK_NOT_FINISHED in PROCESS_ALL mode");
			case TaskExecutionResult::TASK_BLOCKED:
				task->Deschedule();
				task.reset();
				break;
			}
		} catch (...) {
			return;
		}
	}
#else
	throw NotImplementedException("DuckDB was compiled without threads! Background thread loop is not allowed.");
#endif
}

#ifndef DUCKDB_NO_THREADS
static void ThreadExecuteTasks(BuiltinTaskScheduler *scheduler, atomic<bool> *marker) {
	scheduler->ExecuteForever(marker);
}
#endif

int32_t BuiltinTaskScheduler::NumberOfThreads() {
	return current_thread_count.load();
}

idx_t BuiltinTaskScheduler::GetNumberOfTasks() const {
	return queue->GetTasksInQueue();
}

idx_t BuiltinTaskScheduler::GetProducerCount() const {
	return queue->GetProducerCount();
}

idx_t BuiltinTaskScheduler::GetTaskCountForProducer(ProducerToken &token) const {
	return queue->GetTaskCountForProducer(CastProducerToken(token));
}

void BuiltinTaskScheduler::SetThreads(idx_t total_threads, idx_t external_threads) {
	if (total_threads == 0) {
		throw SyntaxException("Number of threads must be positive!");
	}
#ifndef DUCKDB_NO_THREADS
	if (total_threads < external_threads) {
		throw SyntaxException("Number of threads can't be smaller than number of external threads!");
	}
#else
	if (total_threads != external_threads) {
		throw NotImplementedException(
		    "DuckDB was compiled without threads! Setting total_threads != external_threads is not allowed.");
	}
#endif
	requested_thread_count = NumericCast<int32_t>(total_threads - external_threads);
}

void BuiltinTaskScheduler::SetAllocatorFlushTreshold(idx_t threshold) {
	allocator_flush_threshold = threshold;
}

void BuiltinTaskScheduler::SetAllocatorBackgroundThreads(bool enable) {
	allocator_background_threads = enable;
	Allocator::SetBackgroundThreads(enable);
}

void BuiltinTaskScheduler::Signal(idx_t n) {
#ifndef DUCKDB_NO_THREADS
	typedef std::make_signed<std::size_t>::type ssize_t;
	queue->semaphore.signal(NumericCast<ssize_t>(n));
#endif
}

void TaskScheduler::YieldThread() {
#ifndef DUCKDB_NO_THREADS
	std::this_thread::yield();
#endif
}

idx_t TaskScheduler::GetEstimatedCPUId() {
#if defined(__EMSCRIPTEN__)
	// FIXME: Wasm + multithreads can likely be implemented as
	//   return return (idx_t)std::hash<std::thread::id>()(std::this_thread::get_id());
	return 0;
#else
	// this code comes from jemalloc
#if defined(_WIN32)
	return (idx_t)GetCurrentProcessorNumber();
#elif defined(_GNU_SOURCE)
	auto cpu = sched_getcpu();
	if (cpu < 0) {
#ifndef DUCKDB_NO_THREADS
		// fallback to thread id
		return (idx_t)std::hash<std::thread::id>()(std::this_thread::get_id());
#else

		return 0;
#endif
	}
	return (idx_t)cpu;
#elif defined(__aarch64__) && defined(__APPLE__)
	/* Other oses most likely use tpidr_el0 instead */
	uintptr_t c;
	asm volatile("mrs %x0, tpidrro_el0" : "=r"(c)::"memory");
	return (idx_t)(c & ((1 << 3) - 1));
#else
#ifndef DUCKDB_NO_THREADS
	// fallback to thread id
	return (idx_t)std::hash<std::thread::id>()(std::this_thread::get_id());
#else
	return 0;
#endif
#endif
#endif
}

void BuiltinTaskScheduler::RelaunchThreads() {
	lock_guard<mutex> t(thread_lock);
	auto n = requested_thread_count.load();
	RelaunchThreadsInternal(n, false);
}

#ifndef DUCKDB_NO_THREADS
static vector<int> GetProcessCPUMask() {
#if defined(__GLIBC__)
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	if (sched_getaffinity(0, sizeof(cpu_set_t), &cpuset) != 0) {
		return {};
	}
	vector<int> available_cpus;
	for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
		if (CPU_ISSET(cpu, &cpuset)) {
			available_cpus.push_back(cpu);
		}
	}
	return available_cpus;
#else
	return {};
#endif
}

static void SetThreadAffinity(thread &thread, const vector<int> &available_cpus, idx_t thread_idx) {
#if defined(__GLIBC__)
	if (thread_idx < available_cpus.size()) {
		const auto cpu_id = available_cpus[thread_idx];
		cpu_set_t cpuset;
		CPU_ZERO(&cpuset);
		CPU_SET(cpu_id, &cpuset);

		// note that we don't care about the return value here
		// if we did not manage to set affinity, the thread just does not have affinity, which is OK
		pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t), &cpuset);
	}
#endif
}
#endif

void BuiltinTaskScheduler::RelaunchThreadsInternal(int32_t n, bool destroy) {
#ifndef DUCKDB_NO_THREADS
	auto &config = DBConfig::GetConfig(GetDatabase());
	auto new_thread_count = NumericCast<idx_t>(n);

	idx_t external_threads = 0;
	ThreadPinMode pin_thread_mode = ThreadPinMode::AUTO;
	if (!destroy) {
		// If we are destroying, i.e., calling ~TaskScheduler, we don't want to read the settings
		external_threads = Settings::Get<ExternalThreadsSetting>(config);
		pin_thread_mode = Settings::Get<PinThreadsSetting>(GetDatabase());
	}

	if (threads.size() == new_thread_count) {
		current_thread_count = NumericCast<int32_t>(threads.size() + external_threads);
		return;
	}
	if (threads.size() != new_thread_count) {
		// we are changing the number of threads: clear all threads first
		// we do this even when increasing the number of threads to make sure that all threads follow the current
		// affinity mask
		for (idx_t i = 0; i < threads.size(); i++) {
			*markers[i] = false;
		}
		Signal(threads.size());
		// now join the threads to ensure they are fully stopped before erasing them
		for (idx_t i = 0; i < threads.size(); i++) {
			threads[i]->internal_thread->join();
		}
		// erase the threads/markers
		threads.clear();
		markers.clear();
	}
	if (threads.size() < new_thread_count) {
		// we are increasing the number of threads: launch them and run tasks on them
		idx_t create_new_threads = new_thread_count - threads.size();

		// Whether to pin threads to cores
		static constexpr idx_t THREAD_PIN_THRESHOLD = 64;
		const auto pin_threads =
		    pin_thread_mode == ThreadPinMode::ON ||
		    (pin_thread_mode == ThreadPinMode::AUTO && std::thread::hardware_concurrency() > THREAD_PIN_THRESHOLD);
		const auto available_cpus = pin_threads ? GetProcessCPUMask() : vector<int>();
		// If we have fewer available cores than threads, do not pin and let OS scheduler handle it
		const auto can_pin = pin_threads && new_thread_count <= available_cpus.size();
		for (idx_t i = 0; i < create_new_threads; i++) {
			// launch a thread and assign it a cancellation marker
			auto marker = unique_ptr<atomic<bool>>(new atomic<bool>(true));
			unique_ptr<thread> worker_thread;
			try {
				worker_thread = make_uniq<thread>(ThreadExecuteTasks, this, marker.get());
				if (can_pin) {
					SetThreadAffinity(*worker_thread, available_cpus, threads.size());
				}
			} catch (std::exception &ex) {
				// thread constructor failed - this can happen when the system has too many threads allocated
				// in this case we cannot allocate more threads - stop launching them
				break;
			}
			auto thread_wrapper = make_uniq<SchedulerThread>(std::move(worker_thread));

			threads.push_back(std::move(thread_wrapper));
			markers.push_back(std::move(marker));
		}
	}
	current_thread_count = NumericCast<int32_t>(threads.size() + external_threads);
	BlockAllocator::Get(GetDatabase()).FlushAll();
#endif
}

unique_ptr<TaskScheduler> CreateBuiltinTaskScheduler(DatabaseInstance &db) {
	return make_uniq<BuiltinTaskScheduler>(db);
}

} // namespace duckdb
