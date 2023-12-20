//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/concurrent_operator_memory_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/reference_map.hpp"

class BufferPool;
class PhysicalOperator;

namespace duckdb {

//! The memory state of an active operator. As long as this is within scope, the operator is assumed to be active
class OperatorMemoryState {
public:
	OperatorMemoryState(ConcurrentOperatorMemoryManager &concurrent_operator_memory_manager, PhysicalOperator &op,
	                    idx_t operator_soft_limit);
	~OperatorMemoryState();

private:
	//! The ConcurrentOperatorMemoryManager that owns this state
	ConcurrentOperatorMemoryManager &concurrent_operator_memory_manager;
	//! The operator that this state is for
	PhysicalOperator &op;

private:
	//! The current memory usage as indicated by the operator
	idx_t operator_memory_usage;
	//! The soft limit for the operator's memory usage as set by the ConcurrentOperatorMemoryManager
	//! The operator may always increase its memory usage up to this limit
	const idx_t operator_soft_limit;
};

//! ConcurrentOperatorMemoryManager is a one-of class owned by the buffer pool that tries to dynamically assign memory
//! to concurrent operators, such that their combined memory usage does not exceed the limit
class ConcurrentOperatorMemoryManager {
	friend class OperatorMemoryState;

public:
	explicit ConcurrentOperatorMemoryManager(BufferPool &buffer_pool);

private:
	//! At least 512 blocks per operator per thread. This is 128MB for Storage::BLOCK_SIZE = 262144
	static constexpr const idx_t SOFT_LIMIT_PER_OPERATOR_PER_THREAD = 512 * Storage::BLOCK_ALLOC_SIZE;

public:
	unique_ptr<OperatorMemoryState> RegisterOperator(ClientContext &context, PhysicalOperator &op);

private:
	void UnregisterOperator(PhysicalOperator &op);

private:
	//! The buffer pool that manages the memory
	BufferPool &buffer_pool;

	//! Lock because ConcurrentOperatorMemoryManager is accessed concurrently
	mutex lock;
	//! Currently active operators
	reference_map_t<PhysicalOperator, reference<OperatorMemoryState>> active_operators;
	//! Current memory usage of all act
	idx_t current_memory;
};

} // namespace duckdb
