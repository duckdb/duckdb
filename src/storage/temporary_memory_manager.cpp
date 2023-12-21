#include "duckdb/storage/temporary_memory_manager.hpp"

#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

TemporaryMemoryState::TemporaryMemoryState(TemporaryMemoryManager &temporary_memory_manager_p)
    : temporary_memory_manager(temporary_memory_manager_p), reservation(0), remaining_size(0) {
}

TemporaryMemoryState::~TemporaryMemoryState() {
	temporary_memory_manager.Unregister(*this);
}

void TemporaryMemoryState::SetRemainingSize(ClientContext &context, idx_t new_remaining_size) {
	lock_guard<mutex> guard(temporary_memory_manager.lock);
	temporary_memory_manager.SetRemainingSize(*this, new_remaining_size);
	temporary_memory_manager.UpdateState(context, *this);
}

idx_t TemporaryMemoryState::GetReservation() const {
	return reservation;
}

TemporaryMemoryManager::TemporaryMemoryManager() : reservation(0), remaining_size(0) {
}

void TemporaryMemoryManager::UpdateConfiguration(ClientContext &context) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto &task_scheduler = TaskScheduler::GetScheduler(context);

	memory_limit = MAXIMUM_MEMORY_LIMIT_RATIO * double(buffer_manager.GetMaxMemory());
	has_temporary_directory = buffer_manager.HasTemporaryDirectory();
	num_threads = task_scheduler.NumberOfThreads();
}

idx_t TemporaryMemoryManager::MinimumStateMemory() const {
	// TODO this should be tweaked, maybe use the size of 'active_states', or the memory limit to lower this
	return num_threads * MINIMUM_MEMORY_PER_THREAD;
}

unique_ptr<TemporaryMemoryState> TemporaryMemoryManager::Register(ClientContext &context) {
	lock_guard<mutex> guard(lock);
	UpdateConfiguration(context);

	auto result = make_uniq<TemporaryMemoryState>(*this);
	SetReservation(*result, MinimumStateMemory());
	SetRemainingSize(*result, MinimumStateMemory());
	active_states.insert(*result);

	Verify();
	return result;
}

void TemporaryMemoryManager::UpdateState(ClientContext &context, TemporaryMemoryState &temporary_memory_state) {
	UpdateConfiguration(context);

	if (!has_temporary_directory || temporary_memory_state.remaining_size <= MinimumStateMemory()) {
		// We cannot offload / less than initial_memory remaining. Set reservation equal to the remaining size
		SetReservation(temporary_memory_state, temporary_memory_state.remaining_size);
	} else if (reservation - temporary_memory_state.reservation >= memory_limit) {
		// We overshot. Set reservation equal to the minimum
		SetReservation(temporary_memory_state, MinimumStateMemory());
	} else {
		// Non-trivial case, compute how much of the total remaining data belongs to this state
		auto ratio_of_remaining = double(temporary_memory_state.remaining_size) / double(remaining_size);
		// Limit the ratio by taking the minimum of this ratio and the maximum ratio
		auto ratio_limited = MinValue(ratio_of_remaining, MAXIMUM_MEMORY_REMAINING_RATIO);
		// Compute how much memory is left (excluding this state's current reservation)
		auto memory_remaining = memory_limit - (reservation - temporary_memory_state.reservation);
		// Set the reservation equal to the limited ratio of the remaining memory, or the minimum state memory
		SetReservation(temporary_memory_state, MaxValue<idx_t>(ratio_limited * memory_remaining, MinimumStateMemory()));
	}

	Verify();
}

void TemporaryMemoryManager::SetReservation(TemporaryMemoryState &temporary_memory_state, idx_t new_reservation) {
	this->reservation -= temporary_memory_state.reservation;
	temporary_memory_state.reservation = new_reservation;
	this->reservation += temporary_memory_state.reservation;
}

void TemporaryMemoryManager::SetRemainingSize(TemporaryMemoryState &temporary_memory_state, idx_t new_remaining_size) {
	this->remaining_size -= temporary_memory_state.remaining_size;
	temporary_memory_state.remaining_size = new_remaining_size;
	this->remaining_size += temporary_memory_state.remaining_size;
}

void TemporaryMemoryManager::Unregister(TemporaryMemoryState &temporary_memory_state) {
	lock_guard<mutex> guard(lock);

	SetReservation(temporary_memory_state, 0);
	SetRemainingSize(temporary_memory_state, 0);
	active_states.erase(temporary_memory_state);

	Verify();
}

void TemporaryMemoryManager::Verify() const {
#ifdef DEBUG
	idx_t total_reservation = 0;
	idx_t total_remaining_size = 0;
	for (auto &temporary_memory_state : active_states) {
		total_reservation += temporary_memory_state.get().reservation;
		total_remaining_size += temporary_memory_state.get().remaining_size;
	}
	D_ASSERT(total_reservation == this->reservation);
	D_ASSERT(total_remaining_size == this->remaining_size);
#endif
}

} // namespace duckdb
