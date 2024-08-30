//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/temporary_memory_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

class ClientContext;
class TemporaryMemoryManager;

//! State of the temporary memory to be managed concurrently with other states
//! As long as this is within scope, it is active
class TemporaryMemoryState {
	friend class TemporaryMemoryManager;

private:
	TemporaryMemoryState(TemporaryMemoryManager &temporary_memory_manager, idx_t minimum_reservation);

public:
	~TemporaryMemoryState();

public:
	//! Set the remaining size needed for this state (NOTE: does not update the reservation!)
	void SetRemainingSize(idx_t new_remaining_size);
	//! Set the remaining size needed for this state and update the reservation
	void SetRemainingSizeAndUpdateReservation(ClientContext &context, idx_t new_remaining_size);
	//! Set the remaining size to 0 (NOTE: updates the reservation to 0 as well)
	void SetZero();
	//! Get the remaining size that was set for this state
	idx_t GetRemainingSize() const;
	//! Set the minimum reservation for this state
	void SetMinimumReservation(idx_t new_minimum_reservation);
	//! Get the minimum reservation for this state
	idx_t GetMinimumReservation() const;
	//! Updates the reservation based on current remaining size
	void UpdateReservation(ClientContext &context);
	//! Get the reservation of this state
	idx_t GetReservation() const;
	//! Set the materialization penalty for this state
	void SetMaterializationPenalty(idx_t new_materialization_penalty);
	//! Get the materialization penalty for this state
	idx_t GetMaterializationPenalty() const;

private:
	//! The TemporaryMemoryManager that owns this state
	TemporaryMemoryManager &temporary_memory_manager;

	//! The remaining size needed if it could fit fully in memory
	atomic<idx_t> remaining_size;
	//! The minimum reservation for this state
	atomic<idx_t> minimum_reservation;
	//! How much memory this operator has reserved
	atomic<idx_t> reservation;
	//! The weight used for determining the reservation for this state
	atomic<idx_t> materialization_penalty;
};

//! TemporaryMemoryManager is a one-of class owned by the buffer pool that tries to dynamically assign memory
//! to concurrent states, such that their combined memory usage does not exceed the limit
class TemporaryMemoryManager {
	//! TemporaryMemoryState is a friend class so it can access the private methods of this class,
	//! but it should not access the private fields!
	friend class TemporaryMemoryState;

public:
	TemporaryMemoryManager();

private:
	//! TemporaryMemoryState is initialized with a minimum reservation guarantee, which is the minimum of
	//! MINIMUM_RESERVATION_PER_STATE_PER_THREAD and MINIMUM_RESERVATION_MEMORY_LIMIT_DIVISOR.

	//! 512 blocks per state per thread, which is 0.125GB per thread for DEFAULT_BLOCK_ALLOC_SIZE.
	static constexpr idx_t MINIMUM_RESERVATION_PER_STATE_PER_THREAD = 512ULL * DEFAULT_BLOCK_ALLOC_SIZE;
	//! 1/16th of the available main memory.
	static constexpr idx_t MINIMUM_RESERVATION_MEMORY_LIMIT_DIVISOR = 16ULL;

	//! The maximum ratio of the memory limit that we reserve using the TemporaryMemoryManager
	static constexpr double MAXIMUM_MEMORY_LIMIT_RATIO = 0.8;
	//! The maximum ratio of the remaining memory that we reserve per TemporaryMemoryState
	static constexpr double MAXIMUM_FREE_MEMORY_RATIO = 2.0 / 3.0;

public:
	//! Get the TemporaryMemoryManager
	static TemporaryMemoryManager &Get(ClientContext &context);
	//! Register a TemporaryMemoryState
	unique_ptr<TemporaryMemoryState> Register(ClientContext &context);

private:
	//! Locks the TemporaryMemoryManager
	unique_lock<mutex> Lock();
	//! Unregister a TemporaryMemoryState (called by the destructor of TemporaryMemoryState)
	void Unregister(TemporaryMemoryState &temporary_memory_state);
	//! Update memory_limit, has_temporary_directory, and num_threads (must hold the lock)
	void UpdateConfiguration(ClientContext &context);
	//! Update the TemporaryMemoryState to the new remaining size, and updates the reservation (must hold the lock)
	void UpdateState(ClientContext &context, TemporaryMemoryState &temporary_memory_state);
	//! Set the remaining size of a TemporaryMemoryState (must hold the lock)
	void SetRemainingSize(TemporaryMemoryState &temporary_memory_state, idx_t new_remaining_size);
	//! Set the reservation of a TemporaryMemoryState (must hold the lock)
	void SetReservation(TemporaryMemoryState &temporary_memory_state, idx_t new_reservation);
	//! Computes optimal reservation of a TemporaryMemoryState based on a cost function
	idx_t ComputeReservation(const TemporaryMemoryState &temporary_memory_state) const;
	//! Verify internal counts (must hold the lock)
	void Verify() const;

private:
	//! Lock because TemporaryMemoryManager is used concurrently
	mutex lock;

	//! Memory limit of the buffer pool
	idx_t memory_limit = DConstants::INVALID_INDEX;
	//! Whether there is a temporary directory that we can offload blocks to
	bool has_temporary_directory = false;
	//! Number of threads
	idx_t num_threads = DConstants::INVALID_INDEX;
	//! Max memory per query
	idx_t query_max_memory = DConstants::INVALID_INDEX;

	//! Currently active states
	reference_set_t<TemporaryMemoryState> active_states;
	//! The sum of reservations of all active states
	idx_t reservation;
	//! The sum of the remaining size of all active states
	idx_t remaining_size;
};

} // namespace duckdb
