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
	explicit SchedulerThread(unique_ptr<thread> thread_p) : internal_thread(std::move(thread_p)) {
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
	if (q.enqueue(token.token->queue_token, std::move(task))) {
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
	std::queue<unique_ptr<Task>> q;
	mutex qlock;

	void Enqueue(ProducerToken &token, unique_ptr<Task> task);
	bool DequeueFromProducer(ProducerToken &token, unique_ptr<Task> &task);
};

void ConcurrentQueue::Enqueue(ProducerToken &token, unique_ptr<Task> task) {
	lock_guard<mutex> lock(qlock);
	q.push(std::move(task));
}

bool ConcurrentQueue::DequeueFromProducer(ProducerToken &token, unique_ptr<Task> &task) {
	lock_guard<mutex> lock(qlock);
	if (q.empty()) {
		return false;
	}
	task = std::move(q.front());
	q.pop();
	return true;
}

struct QueueProducerToken {
	QueueProducerToken(ConcurrentQueue &queue) {
	}
};
#endif

ProducerToken::ProducerToken(TaskScheduler &scheduler, unique_ptr<QueueProducerToken> token)
    : scheduler(scheduler), token(std::move(token)) {
}

ProducerToken::~ProducerToken() {
}

TaskScheduler::TaskScheduler(DatabaseInstance &db) : db(db), queue(make_uniq<ConcurrentQueue>()) {
}

TaskScheduler::~TaskScheduler() {
#ifndef DUCKDB_NO_THREADS
	SetThreadsInternal(1);
#endif
#ifdef DEBUG
	// Ensure we're not missing any tasks we might have missed
	{
		unique_lock<mutex> lck(blocked_task_lock);
		D_ASSERT(buffered_callbacks.size() == 0);
		D_ASSERT(blocked_tasks.size() == 0);
	}
	{
		unique_lock<mutex> lck(sleeping_task_lock);
		D_ASSERT(sleeping_tasks.size() == 0);
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

void TaskScheduler::ScheduleTask(ProducerToken &token, unique_ptr<Task> task) {
	// Enqueue a task for the given producer token and signal any sleeping threads
	task->current_token = &token;
	queue->Enqueue(token, std::move(task));
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
		RescheduleSleepingTasks();
		queue->semaphore.wait();
		if (queue->q.try_dequeue(task)) {
			auto execute_result = task->Execute(TaskExecutionMode::PROCESS_ALL);

			switch(execute_result) {
			case TaskExecutionResult::TASK_FINISHED:
			case TaskExecutionResult::TASK_ERROR:
				task.reset();
				break;
			case TaskExecutionResult::TASK_NOT_FINISHED:
				throw InternalException("Task should not return TASK_NOT_FINISHED in PROCESS_ALL mode");
			case TaskExecutionResult::TASK_BLOCKED:
				DescheduleTask(std::move(task));
				break;
			}
		}
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
		unique_ptr<Task> task;
		RescheduleSleepingTasks();
		if (!queue->q.try_dequeue(task)) {
			return completed_tasks;
		}
		auto execute_result = task->Execute(TaskExecutionMode::PROCESS_ALL);

		switch(execute_result) {
		case TaskExecutionResult::TASK_FINISHED:
		case TaskExecutionResult::TASK_ERROR:
			task.reset();
			completed_tasks++;
			break;
		case TaskExecutionResult::TASK_NOT_FINISHED:
			throw InternalException("Task should not return TASK_NOT_FINISHED in PROCESS_ALL mode");
		case TaskExecutionResult::TASK_BLOCKED:
			DescheduleTask(std::move(task));
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
	unique_ptr<Task> task;
	for (idx_t i = 0; i < max_tasks; i++) {
		RescheduleSleepingTasks();
		queue->semaphore.wait(TASK_TIMEOUT_USECS);
		if (!queue->q.try_dequeue(task)) {
			return;
		}
		try {
			auto execute_result = task->Execute(TaskExecutionMode::PROCESS_ALL);
			switch(execute_result) {
			case TaskExecutionResult::TASK_FINISHED:
			case TaskExecutionResult::TASK_ERROR:
				task.reset();
				break;
			case TaskExecutionResult::TASK_NOT_FINISHED:
				throw InternalException("Task should not return TASK_NOT_FINISHED in PROCESS_ALL mode");
			case TaskExecutionResult::TASK_BLOCKED:
				DescheduleTask(std::move(task));
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
	lock_guard<mutex> t(thread_lock);
	auto &config = DBConfig::GetConfig(db);
	return threads.size() + config.options.external_threads + 1;
}

void TaskScheduler::SetThreads(int32_t n) {
#ifndef DUCKDB_NO_THREADS
	lock_guard<mutex> t(thread_lock);
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
			auto worker_thread = make_uniq<thread>(ThreadExecuteTasks, this, marker.get());
			auto thread_wrapper = make_uniq<SchedulerThread>(std::move(worker_thread));

			threads.push_back(std::move(thread_wrapper));
			markers.push_back(std::move(marker));
		}
	}
#endif
}

void TaskScheduler::RescheduleSleepingTasks() {
	if (!have_sleeping_tasks) {
		return;
	}

	uint64_t current_time = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();
	unique_lock<mutex> lck(sleeping_task_lock);

	// Reschedule any tasks who have exceeded their sleep timer
	for (auto it = sleeping_tasks.begin(); it != sleeping_tasks.end(); ) {
		if (it->first < current_time) {
//			Printer::Print("Rescheduled task with sleeping time " + to_string(it->first));
			ScheduleTask(*it->second->current_token, std::move(it->second));
			it = sleeping_tasks.erase(it);
		} else {
//			Printer::Print("Did not reschedule: " + to_string(it->first));
			++it;
		}
	}

//	// DEBUG
//	{
//		unique_lock<mutex> lck(sleeping_task_lock);
//		unique_lock<mutex> lck2(blocked_task_lock);
//		Printer::Print("	> currently have: " + to_string(sleeping_tasks.size()) + " sleeping");
//		Printer::Print("	> currently have: " + to_string(blocked_tasks.size()) + " Blocked");
//		Printer::Print("\n");
//	}

	have_sleeping_tasks = !sleeping_tasks.empty();
}

void TaskScheduler::DescheduleTaskCallback(unique_ptr<Task> task, hugeint_t callback_uuid) {
	unique_lock<mutex> lck(blocked_task_lock);

	// First check if callback was made already
	auto buffered_cb_lookup = buffered_callbacks.find(callback_uuid);
	if (buffered_cb_lookup != buffered_callbacks.end()) {
//		Printer::Print(" > Insta reschedule uuid " + to_string(callback_uuid.lower) + to_string(callback_uuid.upper));
		// this callback already happened, reschedule straight away
		ScheduleTask(*task->current_token, std::move(task));
		buffered_callbacks.erase(buffered_cb_lookup);
		return;
	}

//	Printer::Print(" > added to blocked");

	// Callback was not made yet, this task will need to block
	D_ASSERT(blocked_tasks.find(callback_uuid) == blocked_tasks.end());
	blocked_tasks[callback_uuid] = std::move(task);
}

void TaskScheduler::DescheduleTaskSleeping(unique_ptr<Task> task, uint64_t end_time) {
	unique_lock<mutex> lck(sleeping_task_lock);
	sleeping_tasks.insert({end_time, std::move(task)});
	have_sleeping_tasks = true;
}

void TaskScheduler::DescheduleTask(unique_ptr<Task> task) {
	hugeint_t uuid;
	int64_t sleep;

	switch(task->interrupt_state.result) {
	case InterruptResultType::CALLBACK_UUID:
		uuid = task->interrupt_state.callback_uuid;
//		Printer::Print("Descheduled Task with callback id " + to_string(uuid.lower) + to_string(uuid.upper));
		DescheduleTaskCallback(std::move(task), uuid);
		break;
	case InterruptResultType::SLEEP:
		sleep = task->interrupt_state.sleep_until_ns_from_epoch;
//		Printer::Print("Descheduled Task with end time " + to_string(sleep));
		DescheduleTaskSleeping(std::move(task), sleep);
		break;
	default:
		throw InternalException("Unexpected interrupt result type found: (" + to_string((int)task->interrupt_state.result)+ ")");
	}

//	// DEBUG
//	{
//		unique_lock<mutex> lck(sleeping_task_lock);
//		unique_lock<mutex> lck2(blocked_task_lock);
//		Printer::Print("	> currently have: " + to_string(sleeping_tasks.size()) + " sleeping");
//		Printer::Print("	> currently have: " + to_string(blocked_tasks.size()) + " Blocked");
//		Printer::Print("\n");
//	}
}

void TaskScheduler::RescheduleCallback(shared_ptr<DatabaseInstance> db, hugeint_t callback_uuid) {
//	Printer::Print("Callback received for uuid " + to_string(callback_uuid.lower) + to_string(callback_uuid.upper));
	auto& scheduler = GetScheduler(*db);
	unique_lock<mutex> lck (scheduler.blocked_task_lock);

	auto res = scheduler.blocked_tasks.find(callback_uuid);
	if (res == scheduler.blocked_tasks.end()) {
//		Printer::Print(" > adding to buffered");
		scheduler.buffered_callbacks.insert(callback_uuid);
	} else {
//		Printer::Print(" > Reschedule task");
		scheduler.ScheduleTask(*res->second->current_token, std::move(res->second));
		scheduler.blocked_tasks.erase(res);
	}
//	Printer::Print("\n");

//	// DEBUG
//	{
//		unique_lock<mutex> lck(scheduler.sleeping_task_lock);
//		unique_lock<mutex> lck2(scheduler.blocked_task_lock);
//		Printer::Print("	> currently have: " + to_string(scheduler.sleeping_tasks.size()) + " sleeping");
//		Printer::Print("	> currently have: " + to_string(scheduler.blocked_tasks.size()) + " Blocked");
//		Printer::Print("\n");
//	}
}

} // namespace duckdb
