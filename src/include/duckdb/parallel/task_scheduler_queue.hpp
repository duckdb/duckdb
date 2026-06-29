//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/task_scheduler_queue.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/enums/task_scheduler_type.hpp"

#ifdef DUCKDB_NO_THREADS
#include <queue>
#endif

namespace duckdb {

class Task;
class TaskSchedulerQueue;
struct ConcurrentQueueWrapper;
struct ProducerToken;
struct QueueProducerToken;

class TaskSchedulerQueue {
public:
	explicit TaskSchedulerQueue(TaskSchedulerType pool_type_p);
	~TaskSchedulerQueue();

public:
	TaskSchedulerType GetPoolType();
	void Enqueue(ProducerToken &token, shared_ptr<Task> task);
	void EnqueueBulk(ProducerToken &token, vector<shared_ptr<Task>> &tasks);
	bool DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task);
	bool Dequeue(shared_ptr<Task> &task);
	idx_t GetTasksInQueue() const;
	idx_t GetApproxSize() const;
	idx_t GetProducerCount() const;
	idx_t GetTaskCountForProducer(ProducerToken &token) const;

#ifndef DUCKDB_NO_THREADS
	ConcurrentQueueWrapper &GetQueue();
#else
	void RemoveToken(QueueProducerToken &token);
#endif

private:
	const TaskSchedulerType pool_type;
#ifndef DUCKDB_NO_THREADS
	unique_ptr<ConcurrentQueueWrapper> queue;
	atomic<idx_t> tasks_in_queue {0};
#else
	reference_map_t<QueueProducerToken, std::queue<shared_ptr<Task>>> q;
	mutable mutex qlock;
#endif
};

} // namespace duckdb
