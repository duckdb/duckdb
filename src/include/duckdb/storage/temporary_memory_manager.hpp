//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/concurrent_operator_memory_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {

class ClientContext;
class TemporaryMemoryManager;

//! State of the temporary memory of to be managed concurrently with other states
//! As long as this is within scope, it is active
class TemporaryMemoryState {
	friend class TemporaryMemoryManager;

public:
	TemporaryMemoryState(TemporaryMemoryManager &concurrent_operator_memory_manager, idx_t initial_memory);
	~TemporaryMemoryState();

public:
	//! Set the remaining size needed for this state, and updates the reservation
	void SetRemainingSize(ClientContext &context, idx_t new_remaining_size);
	//! Get the reservation of this state
	idx_t GetReservation() const;

private:
	//! The TemporaryMemoryManager that owns this state
	TemporaryMemoryManager &temporary_memory_manager;

	//! The memory this state was initialized to
	const idx_t initial_memory;
	//! How much memory this operator has reserved (initialized to 'initial_memory')
	idx_t reservation;
	//! The remaining size needed if it could fit fully in memory (initialized to 'initial_memory')
	idx_t remaining_size;
};

//! TemporaryMemoryManager is a one-of class owned by the buffer pool that tries to dynamically assign memory
//! to concurrent states, such that their combined memory usage does not exceed the limit
class TemporaryMemoryManager {
	friend class TemporaryMemoryState;

public:
	TemporaryMemoryManager();

private:
	//! Initialize to at least 512 blocks per operator per thread. This is ~128MB for Storage::BLOCK_SIZE = 262144
	static constexpr const idx_t INITIAL_MEMORY_PER_STATE_PER_THREAD = 512 * Storage::BLOCK_ALLOC_SIZE;
	//! The maximum ratio of the memory limit that we reserve using the TemporaryMemoryManager
	static constexpr const double MEMORY_LIMIT_RATIO = 0.9;

public:
	//! Register a TemporaryMemoryState
	unique_ptr<TemporaryMemoryState> Register(ClientContext &context);

private:
	//! Update memory_limit, has_temporary_directory, and num_threads (must hold the lock)
	void UpdateConfiguration(ClientContext &context);
	//! Update the TemporaryMemoryState to the new remaining size, and updates the reservation (must hold the lock)
	void UpdateState(ClientContext &context, TemporaryMemoryState &temporary_memory_state);
	//! Set the reservation of a TemporaryMemoryState (must hold the lock)
	void SetReservation(TemporaryMemoryState &temporary_memory_state, idx_t new_reservation);
	//! Set the remaining size of a TemporaryMemoryState (must hold the lock)
	void SetRemainingSize(TemporaryMemoryState &temporary_memory_state, idx_t new_remaining_size);
	//! Unregister a TemporaryMemoryState (called by the destructor of TemporaryMemoryState)
	void Unregister(TemporaryMemoryState &temporary_memory_state);
	//! Verify internal counts (must hold the lock)
	void Verify() const;

private:
	//! Lock because TemporaryMemoryManager is used concurrently
	mutex lock;

	//! Memory limit of the buffer pool
	idx_t memory_limit;
	//! Whether there is a temporary directory that we can offload blocks to
	bool has_temporary_directory;
	//! Number of threads
	idx_t num_threads;

	//! Currently active states
	reference_set_t<TemporaryMemoryState> active_states;
	//! The sum of reservations of all active states
	idx_t reservation;
	//! The sum of the remaining size of all active states
	idx_t remaining_size;
};

} // namespace duckdb
