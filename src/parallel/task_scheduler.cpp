#include "duckdb/parallel/task_scheduler.hpp"

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
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

	void Enqueue(ProducerToken &token, shared_ptr<Task> task);
	void EnqueueBulk(ProducerToken &token, vector<shared_ptr<Task>> &tasks);
	bool DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task);
	bool Dequeue(shared_ptr<Task> &task);
	idx_t GetTasksInQueue() const;
	idx_t GetApproxSize() const;
	idx_t GetProducerCount() const;
	idx_t GetTaskCountForProducer(ProducerToken &token) const;
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

void ConcurrentQueue::Enqueue(ProducerToken &token, shared_ptr<Task> task) {
	lock_guard<mutex> producer_lock(token.producer_lock);
	task->token = token;
	if (q.enqueue(token.token->queue_token, std::move(task))) {
		++tasks_in_queue;
		semaphore.signal();
	} else {
		throw InternalException("Could not schedule task!");
	}
}

void ConcurrentQueue::EnqueueBulk(ProducerToken &token, vector<shared_ptr<Task>> &tasks) {
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

bool ConcurrentQueue::DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task) {
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

idx_t ConcurrentQueue::GetTaskCountForProducer(ProducerToken &token) const {
	lock_guard<mutex> producer_lock(token.producer_lock);
	return q.size_producer_approx(token.token->queue_token);
}

#else
struct ConcurrentQueue {
	reference_map_t<QueueProducerToken, std::queue<shared_ptr<Task>>> q;
	mutable mutex qlock;

	void Enqueue(ProducerToken &token, shared_ptr<Task> task);
	void EnqueueBulk(ProducerToken &token, vector<shared_ptr<Task>> &tasks);
	bool DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task);
	bool Dequeue(shared_ptr<Task> &task);
	idx_t GetTasksInQueue() const;
	idx_t GetApproxSize() const;
	idx_t GetProducerCount() const;
	idx_t GetTaskCountForProducer(ProducerToken &token) const;
};

void ConcurrentQueue::Enqueue(ProducerToken &token, shared_ptr<Task> task) {
	lock_guard<mutex> lock(qlock);
	task->token = token;
	q[std::ref(*token.token)].push(std::move(task));
}

void ConcurrentQueue::EnqueueBulk(ProducerToken &token, vector<shared_ptr<Task>> &tasks) {
	lock_guard<mutex> lock(qlock);
	for (auto &task : tasks) {
		task->token = token;
		q[std::ref(*token.token)].push(std::move(task));
	}
}

bool ConcurrentQueue::DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task) {
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

idx_t ConcurrentQueue::GetTaskCountForProducer(ProducerToken &token) const {
	lock_guard<mutex> lock(qlock);
	const auto it = q.find(std::ref(*token.token));
	if (it == q.end()) {
		return 0;
	}
	return it->second.size();
}

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

ProducerToken::ProducerToken(TaskScheduler &scheduler, unique_ptr<QueueProducerToken> token)
    : scheduler(scheduler), token(std::move(token)) {
}

ProducerToken::~ProducerToken() {
}

TaskScheduler::TaskScheduler(DatabaseInstance &db)
    : db(db), queue(make_uniq<ConcurrentQueue>()),
      allocator_flush_threshold(db.config.options.allocator_flush_threshold),
      allocator_background_threads(db.config.options.allocator_background_threads), requested_thread_count(0),
      current_thread_count(1) {
	SetAllocatorBackgroundThreads(db.config.options.allocator_background_threads);
}

TaskScheduler::~TaskScheduler() {
#ifndef DUCKDB_NO_THREADS
	try {
		RelaunchThreadsInternal(0);
	} catch (...) {
		// nothing we can do in the destructor if this fails
	}
#endif
}

TaskScheduler &TaskScheduler::GetScheduler(ClientContext &context) {
	return TaskScheduler::GetScheduler(DatabaseInstance::GetDatabase(context));
}

TaskScheduler &TaskScheduler::GetScheduler(DatabaseInstance &db) {
	return db.GetScheduler();
}

unique_ptr<ProducerToken> TaskScheduler::CreateProducer() {
	auto token = make_uniq<QueueProducerToken>(*queue);
	return make_uniq<ProducerToken>(*this, std::move(token));
}

void TaskScheduler::ScheduleTask(ProducerToken &token, shared_ptr<Task> task) {
	// Enqueue a task for the given producer token and signal any sleeping threads
	queue->Enqueue(token, std::move(task));
}

void TaskScheduler::ScheduleTasks(ProducerToken &producer, vector<shared_ptr<Task>> &tasks) {
	queue->EnqueueBulk(producer, tasks);
}

bool TaskScheduler::GetTaskFromProducer(ProducerToken &token, shared_ptr<Task> &task) {
	return queue->DequeueFromProducer(token, task);
}

void TaskScheduler::ExecuteForever(atomic<bool> *marker) {
#ifndef DUCKDB_NO_THREADS
	static constexpr const int64_t INITIAL_FLUSH_WAIT = 500000; // initial wait time of 0.5s (in mus) before flushing

	const auto &block_allocator = BlockAllocator::Get(db);
	const auto &config = DBConfig::GetConfig(db);

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
			auto process_mode = config.options.scheduler_process_partial ? TaskExecutionMode::PROCESS_PARTIAL
			                                                             : TaskExecutionMode::PROCESS_ALL;
			auto execute_result = task->Execute(process_mode);

			switch (execute_result) {
			case TaskExecutionResult::TASK_FINISHED:
			case TaskExecutionResult::TASK_ERROR:
				task.reset();
				break;
			case TaskExecutionResult::TASK_NOT_FINISHED: {
				// task is not finished - reschedule immediately
				auto &token = *task->token;
				queue->Enqueue(token, std::move(task));
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

idx_t TaskScheduler::ExecuteTasks(atomic<bool> *marker, idx_t max_tasks) {
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

void TaskScheduler::ExecuteTasks(idx_t max_tasks) {
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
static void ThreadExecuteTasks(TaskScheduler *scheduler, atomic<bool> *marker) {
	scheduler->ExecuteForever(marker);
}
#endif

int32_t TaskScheduler::NumberOfThreads() {
	return current_thread_count.load();
}

idx_t TaskScheduler::GetNumberOfTasks() const {
	return queue->GetTasksInQueue();
}

idx_t TaskScheduler::GetProducerCount() const {
	return queue->GetProducerCount();
}

idx_t TaskScheduler::GetTaskCountForProducer(ProducerToken &token) const {
	return queue->GetTaskCountForProducer(token);
}

void TaskScheduler::SetThreads(idx_t total_threads, idx_t external_threads) {
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

void TaskScheduler::SetAllocatorFlushTreshold(idx_t threshold) {
	allocator_flush_threshold = threshold;
}

void TaskScheduler::SetAllocatorBackgroundThreads(bool enable) {
	allocator_background_threads = enable;
	Allocator::SetBackgroundThreads(enable);
}

void TaskScheduler::Signal(idx_t n) {
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
#if defined(EMSCRIPTEN)
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

void TaskScheduler::RelaunchThreads() {
	lock_guard<mutex> t(thread_lock);
	auto n = requested_thread_count.load();
	RelaunchThreadsInternal(n);
}

#ifndef DUCKDB_NO_THREADS
static void SetThreadAffinity(thread &thread, const int &cpu_id) {
#if defined(__GLIBC__)
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(cpu_id, &cpuset);

	// note that we don't care about the return value here
	// if we did not manage to set affinity, the thread just does not have affinity, which is OK
	pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t), &cpuset);
#endif
}
#endif

void TaskScheduler::RelaunchThreadsInternal(int32_t n) {
#ifndef DUCKDB_NO_THREADS
	auto &config = DBConfig::GetConfig(db);
	auto new_thread_count = NumericCast<idx_t>(n);
	if (threads.size() == new_thread_count) {
		current_thread_count = NumericCast<int32_t>(threads.size() + config.options.external_threads);
		return;
	}
	if (threads.size() > new_thread_count) {
		// we are reducing the number of threads: clear all threads first
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
		const auto pin_threads = db.config.options.pin_threads == ThreadPinMode::ON ||
		                         (db.config.options.pin_threads == ThreadPinMode::AUTO &&
		                          std::thread::hardware_concurrency() > THREAD_PIN_THRESHOLD);
		for (idx_t i = 0; i < create_new_threads; i++) {
			// launch a thread and assign it a cancellation marker
			auto marker = unique_ptr<atomic<bool>>(new atomic<bool>(true));
			unique_ptr<thread> worker_thread;
			try {
				worker_thread = make_uniq<thread>(ThreadExecuteTasks, this, marker.get());
				if (pin_threads) {
					SetThreadAffinity(*worker_thread, NumericCast<int>(threads.size()));
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
	current_thread_count = NumericCast<int32_t>(threads.size() + config.options.external_threads);
	BlockAllocator::Get(db).FlushAll();
#endif
}

} // namespace duckdb
