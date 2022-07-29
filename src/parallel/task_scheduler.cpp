#include "duckdb/parallel/task_scheduler.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#ifndef DUCKDB_NO_THREADS
#include "concurrentqueue.h"
#include "lightweightsemaphore.h"
#include "duckdb/common/thread.hpp"
#else
#include <queue>
#endif

namespace duckdb {

struct SchedulerThread {
#ifndef DUCKDB_NO_THREADS
	explicit SchedulerThread(unique_ptr<thread> thread_p) : internal_thread(move(thread_p)) {
	}

	unique_ptr<thread> internal_thread;
#endif
};

#ifndef DUCKDB_NO_THREADS
typedef duckdb_moodycamel::ConcurrentQueue<unique_ptr<Task>> concurrent_queue_t;
typedef duckdb_moodycamel::LightweightSemaphore lightweight_semaphore_t;

struct ConcurrentQueue {
	concurrent_queue_t q;
	lightweight_semaphore_t semaphore;

	void Enqueue(ProducerToken &token, unique_ptr<Task> task);
	bool DequeueFromProducer(ProducerToken &token, unique_ptr<Task> &task);
};

struct QueueProducerToken {
	explicit QueueProducerToken(ConcurrentQueue &queue) : queue_token(queue.q) {
	}

	duckdb_moodycamel::ProducerToken queue_token;
};

void ConcurrentQueue::Enqueue(ProducerToken &token, unique_ptr<Task> task) {
	lock_guard<mutex> producer_lock(token.producer_lock);
	if (q.enqueue(token.token->queue_token, move(task))) {
		semaphore.signal();
	} else {
		throw InternalException("Could not schedule task!");
	}
}

bool ConcurrentQueue::DequeueFromProducer(ProducerToken &token, unique_ptr<Task> &task) {
	lock_guard<mutex> producer_lock(token.producer_lock);
	return q.try_dequeue_from_producer(token.token->queue_token, task);
}

#else
struct ConcurrentQueue {
	std::queue<std::unique_ptr<Task>> q;
	mutex qlock;

	void Enqueue(ProducerToken &token, unique_ptr<Task> task);
	bool DequeueFromProducer(ProducerToken &token, unique_ptr<Task> &task);
};

void ConcurrentQueue::Enqueue(ProducerToken &token, unique_ptr<Task> task) {
	lock_guard<mutex> lock(qlock);
	q.push(move(task));
}

bool ConcurrentQueue::DequeueFromProducer(ProducerToken &token, unique_ptr<Task> &task) {
	lock_guard<mutex> lock(qlock);
	if (q.empty()) {
		return false;
	}
	task = move(q.front());
	q.pop();
	return true;
}

struct QueueProducerToken {
	QueueProducerToken(ConcurrentQueue &queue) {
	}
};
#endif

ProducerToken::ProducerToken(TaskScheduler &scheduler, unique_ptr<QueueProducerToken> token)
    : scheduler(scheduler), token(move(token)) {
}

ProducerToken::~ProducerToken() {
}

TaskScheduler::TaskScheduler(DatabaseInstance &db) : db(db), queue(make_unique<ConcurrentQueue>()) {
}

TaskScheduler::~TaskScheduler() {
#ifndef DUCKDB_NO_THREADS
	SetThreadsInternal(1);
#endif
}

TaskScheduler &TaskScheduler::GetScheduler(ClientContext &context) {
	return TaskScheduler::GetScheduler(DatabaseInstance::GetDatabase(context));
}

TaskScheduler &TaskScheduler::GetScheduler(DatabaseInstance &db) {
	return db.GetScheduler();
}

unique_ptr<ProducerToken> TaskScheduler::CreateProducer() {
	auto token = make_unique<QueueProducerToken>(*queue);
	return make_unique<ProducerToken>(*this, move(token));
}

void TaskScheduler::ScheduleTask(ProducerToken &token, unique_ptr<Task> task) {
	// Enqueue a task for the given producer token and signal any sleeping threads
	queue->Enqueue(token, move(task));
}

bool TaskScheduler::GetTaskFromProducer(ProducerToken &token, unique_ptr<Task> &task) {
	return queue->DequeueFromProducer(token, task);
}

void TaskScheduler::ExecuteForever(atomic<bool> *marker) {
#ifndef DUCKDB_NO_THREADS
	unique_ptr<Task> task;
	// loop until the marker is set to false
	while (*marker) {
		// wait for a signal with a timeout
		queue->semaphore.wait();
		if (queue->q.try_dequeue(task)) {
			task->Execute(TaskExecutionMode::PROCESS_ALL);
			task.reset();
		}
	}
#else
	throw NotImplementedException("DuckDB was compiled without threads! Background thread loop is not allowed.");
#endif
}

void TaskScheduler::ExecuteTasks(idx_t max_tasks) {
#ifndef DUCKDB_NO_THREADS
	unique_ptr<Task> task;
	for (idx_t i = 0; i < max_tasks; i++) {
		queue->semaphore.wait(TASK_TIMEOUT_USECS);
		if (!queue->q.try_dequeue(task)) {
			return;
		}
		try {
			task->Execute(TaskExecutionMode::PROCESS_ALL);
			task.reset();
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
	auto &config = DBConfig::GetConfig(db);
	return threads.size() + config.options.external_threads + 1;
}

void TaskScheduler::SetThreads(int32_t n) {
#ifndef DUCKDB_NO_THREADS
	if (n < 1) {
		throw SyntaxException("Must have at least 1 thread!");
	}
	SetThreadsInternal(n);
#else
	if (n != 1) {
		throw NotImplementedException("DuckDB was compiled without threads! Setting threads > 1 is not allowed.");
	}
#endif
}

void TaskScheduler::Signal(idx_t n) {
#ifndef DUCKDB_NO_THREADS
	queue->semaphore.signal(n);
#endif
}

void TaskScheduler::SetThreadsInternal(int32_t n) {
#ifndef DUCKDB_NO_THREADS
	if (threads.size() == idx_t(n - 1)) {
		return;
	}
	idx_t new_thread_count = n - 1;
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
		for (idx_t i = 0; i < create_new_threads; i++) {
			// launch a thread and assign it a cancellation marker
			auto marker = unique_ptr<atomic<bool>>(new atomic<bool>(true));
			auto worker_thread = make_unique<thread>(ThreadExecuteTasks, this, marker.get());
			auto thread_wrapper = make_unique<SchedulerThread>(move(worker_thread));

			threads.push_back(move(thread_wrapper));
			markers.push_back(move(marker));
		}
	}
#endif
}

} // namespace duckdb
