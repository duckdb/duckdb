#include "duckdb/storage/concurrent_operator_memory_manager.hpp"

#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"

namespace duckdb {

OperatorMemoryState::OperatorMemoryState(ConcurrentOperatorMemoryManager &concurrent_operator_memory_manager_p,
                                         PhysicalOperator &op_p, idx_t operator_soft_limit_p)
    : concurrent_operator_memory_manager(concurrent_operator_memory_manager_p), op(op_p), operator_memory_usage(0),
      operator_soft_limit(operator_soft_limit_p) {
}

OperatorMemoryState::~OperatorMemoryState() {
	concurrent_operator_memory_manager.UnregisterOperator(op);
}

ConcurrentOperatorMemoryManager::ConcurrentOperatorMemoryManager(BufferPool &buffer_pool_p)
    : buffer_pool(buffer_pool_p) {
}

unique_ptr<OperatorMemoryState> ConcurrentOperatorMemoryManager::RegisterOperator(ClientContext &context,
                                                                                  PhysicalOperator &op) {
	// Create an OperatorMemoryState with a soft limit
	const idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	auto result = make_uniq<OperatorMemoryState>(*this, op, num_threads * SOFT_LIMIT_PER_OPERATOR_PER_THREAD);

	// Grab the lock and add
	lock_guard<mutex> guard(lock);
	active_operators.emplace(op, *result);
	return result;
}

void ConcurrentOperatorMemoryManager::UnregisterOperator(PhysicalOperator &op) {
	lock_guard<mutex> guard(lock);
	active_operators.erase(op);
}

} // namespace duckdb
