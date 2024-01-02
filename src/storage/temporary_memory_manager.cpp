#include "duckdb/storage/temporary_memory_manager.hpp"

#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

TemporaryMemoryState::TemporaryMemoryState(TemporaryMemoryManager &temporary_memory_manager_p)
    : temporary_memory_manager(temporary_memory_manager_p), minimum_reservation(INITIAL_MEMORY) {
}

TemporaryMemoryState::~TemporaryMemoryState() {
	temporary_memory_manager.Unregister(*this);
}

void TemporaryMemoryState::SetRemainingSize(ClientContext &context, idx_t new_remaining_size) {
	lock_guard<mutex> guard(temporary_memory_manager.lock);
	temporary_memory_manager.SetRemainingSize(*this, new_remaining_size);
	temporary_memory_manager.UpdateState(context, *this);
}

void TemporaryMemoryState::SetMinimumReservation(idx_t new_minimum_reservation) {
	if (new_minimum_reservation > reservation) {
		throw InternalException(
		    "TemporaryMemoryState::SetMinimumReservation cannot be higher than current reservation");
	}
	minimum_reservation = new_minimum_reservation;
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

TemporaryMemoryManager &TemporaryMemoryManager::Get(ClientContext &context) {
	return BufferManager::GetBufferManager(context).GetTemporaryMemoryManager();
}

unique_ptr<TemporaryMemoryState> TemporaryMemoryManager::Register(ClientContext &context) {
	lock_guard<mutex> guard(lock);
	UpdateConfiguration(context);

	auto result = unique_ptr<TemporaryMemoryState>(new TemporaryMemoryState(*this));
	SetRemainingSize(*result, result->minimum_reservation);
	SetReservation(*result, result->minimum_reservation);
	active_states.insert(*result);

	Verify();
	return result;
}

void TemporaryMemoryManager::UpdateState(ClientContext &context, TemporaryMemoryState &temporary_memory_state) {
	UpdateConfiguration(context);

	if (!has_temporary_directory) {
		// We cannot offload, so we cannot limit memory usage. Set reservation equal to the remaining size
		SetReservation(temporary_memory_state, temporary_memory_state.remaining_size);
	} else if (reservation - temporary_memory_state.reservation >= memory_limit) {
		// We overshot. Set reservation equal to the minimum
		SetReservation(temporary_memory_state, temporary_memory_state.minimum_reservation);
	} else {
		// The lower bound for the reservation of this state is its minimum reservation
		auto &lower_bound = temporary_memory_state.minimum_reservation;

		// The upper bound for the reservation of this state is the minimum of:
		// 1. Remaining size of the state
		// 2. MAXIMUM_FREE_MEMORY_RATIO * free memory
		auto free_memory = memory_limit - (reservation - temporary_memory_state.reservation);
		auto upper_bound =
		    MinValue<idx_t>(temporary_memory_state.remaining_size, MAXIMUM_FREE_MEMORY_RATIO * free_memory);

		if (remaining_size > memory_limit) {
			// We're processing more data than fits in memory, so we must further limit memory usage.
			// The upper bound for the reservation of this state is now also the minimum of:
			// 3. The ratio of the remaining size of this state and the total remaining size * memory limit
			auto ratio_of_remaining = double(temporary_memory_state.remaining_size) / double(remaining_size);
			upper_bound = MinValue<idx_t>(upper_bound, ratio_of_remaining * memory_limit);
		}

		SetReservation(temporary_memory_state, MaxValue<idx_t>(lower_bound, upper_bound));
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
