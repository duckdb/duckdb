//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/task_scheduler_token.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/array.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/task_scheduler_type.hpp"
#include "duckdb/common/mutex.hpp"

#include <condition_variable>

namespace duckdb {

// Forward declarations.
struct QueueProducerToken;
class TaskSchedulerQueue;

struct ProducerToken {
public:
	explicit ProducerToken(array<unique_ptr<TaskSchedulerQueue>, TASK_SCHEDULER_TYPE_COUNT> &queues);
	~ProducerToken();

public:
	QueueProducerToken &GetQueueProducerToken(TaskSchedulerType pool_type);

public:
	annotated_mutex producer_lock;
	std::condition_variable producer_cv;

private:
	array<unique_ptr<QueueProducerToken>, TASK_SCHEDULER_TYPE_COUNT> tokens;
};

} // namespace duckdb
