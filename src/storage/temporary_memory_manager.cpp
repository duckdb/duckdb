#include "duckdb/storage/temporary_memory_manager.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <math.h>

namespace duckdb {

TemporaryMemoryState::TemporaryMemoryState(TemporaryMemoryManager &temporary_memory_manager_p,
                                           idx_t minimum_reservation_p)
    : temporary_memory_manager(temporary_memory_manager_p), remaining_size(0),
      minimum_reservation(minimum_reservation_p), reservation(0), materialization_penalty(1) {
}

TemporaryMemoryState::~TemporaryMemoryState() {
	temporary_memory_manager.Unregister(*this);
}

void TemporaryMemoryState::SetRemainingSize(idx_t new_remaining_size) {
	auto guard = temporary_memory_manager.Lock();
	temporary_memory_manager.SetRemainingSize(*this, new_remaining_size);
}

idx_t TemporaryMemoryState::GetRemainingSize() const {
	return remaining_size;
}

void TemporaryMemoryState::SetMinimumReservation(idx_t new_minimum_reservation) {
	minimum_reservation = new_minimum_reservation;
}

idx_t TemporaryMemoryState::GetMinimumReservation() const {
	return minimum_reservation;
}

void TemporaryMemoryState::UpdateReservation(ClientContext &context) {
	auto guard = temporary_memory_manager.Lock();
	temporary_memory_manager.UpdateState(context, *this);
}

idx_t TemporaryMemoryState::GetReservation() const {
	return reservation;
}

void TemporaryMemoryState::SetMaterializationPenalty(idx_t new_materialization_penalty) {
	materialization_penalty = new_materialization_penalty;
}

TemporaryMemoryManager::TemporaryMemoryManager() : reservation(0), remaining_size(0) {
}

unique_lock<mutex> TemporaryMemoryManager::Lock() {
	return unique_lock<mutex>(lock);
}

void TemporaryMemoryManager::Unregister(TemporaryMemoryState &temporary_memory_state) {
	auto guard = Lock();

	SetReservation(temporary_memory_state, 0);
	SetRemainingSize(temporary_memory_state, 0);
	active_states.erase(temporary_memory_state);

	Verify();
}

void TemporaryMemoryManager::UpdateConfiguration(ClientContext &context) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto &task_scheduler = TaskScheduler::GetScheduler(context);

	memory_limit = NumericCast<idx_t>(MAXIMUM_MEMORY_LIMIT_RATIO * static_cast<double>(buffer_manager.GetMaxMemory()));
	has_temporary_directory = buffer_manager.HasTemporaryDirectory();
	num_threads = NumericCast<idx_t>(task_scheduler.NumberOfThreads());
	query_max_memory = buffer_manager.GetQueryMaxMemory();
}

TemporaryMemoryManager &TemporaryMemoryManager::Get(ClientContext &context) {
	return BufferManager::GetBufferManager(context).GetTemporaryMemoryManager();
}

unique_ptr<TemporaryMemoryState> TemporaryMemoryManager::Register(ClientContext &context) {
	auto guard = Lock();
	UpdateConfiguration(context);

	auto minimum_reservation = MinValue(num_threads * MINIMUM_RESERVATION_PER_STATE_PER_THREAD,
	                                    memory_limit / MINIMUM_RESERVATION_MEMORY_LIMIT_DIVISOR);
	auto result = unique_ptr<TemporaryMemoryState>(new TemporaryMemoryState(*this, minimum_reservation));
	SetRemainingSize(*result, result->GetMinimumReservation());
	SetReservation(*result, result->GetMinimumReservation());
	active_states.insert(*result);

	Verify();
	return result;
}

void TemporaryMemoryManager::UpdateState(ClientContext &context, TemporaryMemoryState &temporary_memory_state) {
	UpdateConfiguration(context);

	// The lower bound for the reservation of this state is either the minimum reservation or the remaining size
	const auto lower_bound =
	    MinValue(temporary_memory_state.GetMinimumReservation(), temporary_memory_state.GetRemainingSize());

	if (context.config.force_external) {
		// We're forcing external processing. Give it the minimum
		SetReservation(temporary_memory_state, lower_bound);
	} else if (!has_temporary_directory) {
		// We cannot offload, so we cannot limit memory usage. Set reservation equal to the remaining size
		SetReservation(temporary_memory_state, temporary_memory_state.GetRemainingSize());
	} else if (temporary_memory_state.GetRemainingSize() == 0) {
		// Sometimes set to 0 to denote end of state (before actually deleting the state)
		SetReservation(temporary_memory_state, 0);
	} else if (reservation - temporary_memory_state.GetReservation() + lower_bound >= memory_limit) {
		// We overshot. Set reservation equal to the minimum
		SetReservation(temporary_memory_state, lower_bound);
	} else {
		// The upper bound for the reservation of this state is the minimum of:
		// 1. Remaining size of the state
		// 2. The max memory per query
		// 3. MAXIMUM_FREE_MEMORY_RATIO * free memory
		auto upper_bound = MinValue<idx_t>(temporary_memory_state.GetRemainingSize(), query_max_memory);
		const auto free_memory = memory_limit - (reservation - temporary_memory_state.GetReservation());
		upper_bound = MinValue<idx_t>(upper_bound,
		                              NumericCast<idx_t>(MAXIMUM_FREE_MEMORY_RATIO * static_cast<double>(free_memory)));
		upper_bound = MinValue<idx_t>(upper_bound, free_memory);

		idx_t new_reservation;
		if (lower_bound >= upper_bound) {
			new_reservation = lower_bound;
		} else {
			new_reservation =
			    remaining_size > memory_limit
			        ? ComputeOptimalReservation(temporary_memory_state, free_memory, lower_bound, upper_bound)
			        : upper_bound;
		}

		SetReservation(temporary_memory_state, new_reservation);
	}

	Verify();
}

void TemporaryMemoryManager::SetRemainingSize(TemporaryMemoryState &temporary_memory_state, idx_t new_remaining_size) {
	D_ASSERT(this->remaining_size >= temporary_memory_state.GetRemainingSize());
	this->remaining_size -= temporary_memory_state.GetRemainingSize();
	temporary_memory_state.remaining_size = new_remaining_size;
	this->remaining_size += temporary_memory_state.GetRemainingSize();
}

void TemporaryMemoryManager::SetReservation(TemporaryMemoryState &temporary_memory_state, idx_t new_reservation) {
	D_ASSERT(this->reservation >= temporary_memory_state.GetReservation());
	this->reservation -= temporary_memory_state.GetReservation();
	temporary_memory_state.reservation = new_reservation;
	this->reservation += temporary_memory_state.GetReservation();
}

idx_t TemporaryMemoryManager::ComputeOptimalReservation(const TemporaryMemoryState &temporary_memory_state,
                                                        const idx_t free_memory, const idx_t lower_bound,
                                                        const idx_t upper_bound) const {
	static constexpr idx_t OPTIMIZATION_ITERATIONS_BASE = 10;
	const idx_t n = active_states.size();

	// Collect sizes and reservations in vectors for ease
	optional_idx state_index;
	vector<double> siz(n, 0);
	vector<double> res(n, 0);
	vector<double> pen(n, 0);
	vector<double> der(n, 0);

	idx_t i = 0;
	for (auto &active_state : active_states) {
		D_ASSERT(active_state.get().GetRemainingSize() != 0);
		D_ASSERT(active_state.get().GetReservation() != 0);
		siz[i] = MaxValue<double>(static_cast<double>(active_state.get().GetRemainingSize()), 1);
		res[i] = MaxValue<double>(static_cast<double>(active_state.get().GetReservation()), 1);
		pen[i] = static_cast<double>(active_state.get().materialization_penalty.load());
		if (RefersToSameObject(active_state.get(), temporary_memory_state)) {
			state_index = i;
			// We can't actually reserve memory for all active operators, only for "temporary_memory_state"
			// So, we're essentially computing how much of the remaining memory should go to "temporary_memory_state"
			// We initialize it with its lower bound
			res[i] = static_cast<double>(lower_bound);
		}
		i++;
	}

	// Distribute memory in OPTIMIZATION_ITERATIONS
	idx_t remaining_memory = free_memory - lower_bound;
	const idx_t optimization_iterations = OPTIMIZATION_ITERATIONS_BASE + n;
	for (idx_t opt_idx = 0; opt_idx < optimization_iterations; opt_idx++) {
		// Cost function takes "throughput" (reservation / size) of each operator as its principal input
		double prod_siz = 1;
		double prod_res = 1;
		double mat_cost = 0;
		for (i = 0; i < n; i++) {
			if (res[i] == siz[i]) {
				continue; // We can't increase the reservation of "maxed" states, so we skip these
			}
			prod_siz *= siz[i];
			prod_res *= res[i];
			mat_cost += pen[i] * (1 - res[i] / siz[i]); // Materialization cost: sum of (1 - throughput)
		}
		const double nd = static_cast<double>(n); // n as double for convenience
		const double tp_mult =                    // Throughput multiplier: 1 - geometric mean of throughputs
		    1 - pow(prod_res / prod_siz, 1 / nd);

		// Cost function: materialization cost * (1 - throughput multiplier), but we don't actually need to compute it
		// here. We need to compute the derivative with respect to every reservation, stored in "der"
		// Just use https://www.derivative-calculator.net with this (n = 3) to see what's going on
		// (3 - (a_1/s_1)-(a_2/s_2)-(a_3/s_3))*(1-((a_1/s_1)*(a_2/s_2)*(a_3/s_3))^(1/3))
		const double intermediate = -(pow(prod_res, 1 / nd) * mat_cost) / (nd * pow(prod_siz, 1 / nd));

		double sum_of_nonmaxed = 0;
		idx_t nonmax_count = 0;
		for (i = 0; i < n; i++) {
			if (res[i] == siz[i]) {
				continue; // We can't increase the reservation of "maxed" states, so we skip these
			}
			der[i] = intermediate / res[i] - pen[i] * tp_mult / siz[i];
			D_ASSERT(res[i] <= siz[i]);
			sum_of_nonmaxed += der[i];
			nonmax_count++;
		}
		// We will increase the reservation of every operator with a gradient less than the average gradient
		const auto avg_of_nonmaxed = sum_of_nonmaxed / static_cast<double>(nonmax_count);

		// This is how much memory we will distribute in this round
		const auto iter_memory = NumericCast<idx_t>(static_cast<double>(remaining_memory) /
		                                            static_cast<double>(optimization_iterations - opt_idx));
		for (i = 0; i < n; i++) {
			if (res[i] == siz[i] || der[i] > avg_of_nonmaxed) {
				continue;
			}
			const auto delta = NumericCast<idx_t>(
			    MinValue<double>(siz[i] - res[i], (der[i] / sum_of_nonmaxed) * static_cast<double>(iter_memory)));
			D_ASSERT(delta > 0 && delta <= remaining_memory);
			res[i] += static_cast<double>(delta);
			remaining_memory -= delta;
		}
	}
	D_ASSERT(remaining_memory == 0);

	// Return computed reservation of this state
	return MinValue(NumericCast<idx_t>(res[state_index.GetIndex()]), upper_bound);
}

void TemporaryMemoryManager::Verify() const {
#ifdef DEBUG
	idx_t total_reservation = 0;
	idx_t total_remaining_size = 0;
	for (auto &active_state : active_states) {
		total_reservation += active_state.get().GetReservation();
		total_remaining_size += active_state.get().GetRemainingSize();
	}
	D_ASSERT(total_reservation == this->reservation);
	D_ASSERT(total_remaining_size == this->remaining_size);
#endif
}

} // namespace duckdb
