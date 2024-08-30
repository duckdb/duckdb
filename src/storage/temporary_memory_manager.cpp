#include "duckdb/storage/temporary_memory_manager.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <cmath>

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

void TemporaryMemoryState::SetRemainingSizeAndUpdateReservation(ClientContext &context, idx_t new_remaining_size) {
	D_ASSERT(new_remaining_size != 0); // Use SetZero instead
	auto guard = temporary_memory_manager.Lock();
	temporary_memory_manager.SetRemainingSize(*this, new_remaining_size);
	temporary_memory_manager.UpdateState(context, *this);
}

void TemporaryMemoryState::SetZero() {
	auto guard = temporary_memory_manager.Lock();
	temporary_memory_manager.SetRemainingSize(*this, 0);
	temporary_memory_manager.SetReservation(*this, 0);
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
	auto guard = temporary_memory_manager.Lock();
	materialization_penalty = new_materialization_penalty;
}

idx_t TemporaryMemoryState::GetMaterializationPenalty() const {
	return materialization_penalty;
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

	memory_limit =
	    LossyNumericCast<idx_t>(MAXIMUM_MEMORY_LIMIT_RATIO * static_cast<double>(buffer_manager.GetMaxMemory()));
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

	if (temporary_memory_state.GetRemainingSize() == 0) {
		// Sometimes set to 0 to denote end of state (before actually deleting the state)
		SetReservation(temporary_memory_state, 0);
	} else if (context.config.force_external) {
		// We're forcing external processing. Give it the minimum
		SetReservation(temporary_memory_state, lower_bound);
	} else if (!has_temporary_directory) {
		// We cannot offload, so we cannot limit memory usage. Set reservation equal to the remaining size
		SetReservation(temporary_memory_state, temporary_memory_state.GetRemainingSize());
	} else if (reservation - temporary_memory_state.GetReservation() + lower_bound >= memory_limit) {
		// We overshot. Set reservation equal to the minimum
		SetReservation(temporary_memory_state, lower_bound);
	} else {
		// The upper bound for the reservation of this state is the minimum of:
		// 1. Remaining size of the state
		// 2. The max memory per query
		// 3. MAXIMUM_FREE_MEMORY_RATIO * free memory
		auto upper_bound = MinValue(temporary_memory_state.GetRemainingSize(), query_max_memory);
		const auto free_memory = memory_limit - (reservation - temporary_memory_state.GetReservation());
		upper_bound = MinValue(upper_bound,
		                       LossyNumericCast<idx_t>(MAXIMUM_FREE_MEMORY_RATIO * static_cast<double>(free_memory)));
		upper_bound = MinValue(upper_bound, free_memory);

		idx_t new_reservation;
		if (lower_bound >= upper_bound) {
			new_reservation = lower_bound;
		} else {
			new_reservation = remaining_size > memory_limit ? ComputeReservation(temporary_memory_state) : upper_bound;
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

//! Compute initial reservation for use in ComputeReservation
static idx_t ComputeInitialReservation(const TemporaryMemoryState &temporary_memory_state) {
	// Maximum of minimum reservation and the current reservation
	auto result = MaxValue(temporary_memory_state.GetMinimumReservation(), temporary_memory_state.GetReservation());
	// Bounded by the remaining size
	result = MinValue(result, temporary_memory_state.GetRemainingSize());
	// At least 1
	return MaxValue<idx_t>(result, 1);
}

static void ComputeDerivatives(const vector<reference<const TemporaryMemoryState>> &states, const vector<idx_t> &res,
                               vector<double> &der, const idx_t n) {
	// Cost function takes "throughput" (reservation / size) of each operator as its principal input
	double prod_siz = 1;
	double prod_res = 1;
	double mat_cost = 0;
	for (idx_t i = 0; i < n; i++) {
		auto &state = states[i].get();
		const auto resd = static_cast<double>(res[i]);
		const auto sizd = static_cast<double>(MaxValue<idx_t>(state.GetRemainingSize(), 1));
		const auto pend = static_cast<double>(state.GetMaterializationPenalty());
		prod_res *= resd;
		prod_siz *= sizd;
		mat_cost += pend * (1 - resd / sizd); // Materialization cost: sum of (1 - throughput)
	}
	const double nd = static_cast<double>(n);                    // n as double for convenience
	const double tp_mult = 1 - pow(prod_res / prod_siz, 1 / nd); // Throughput multiplier: 1 - geomean throughputs

	// Cost function: materialization cost * (1 - throughput multiplier), but we don't actually need to compute it
	// here. We need to compute the derivative with respect to every reservation, stored in "der"
	// Just use https://www.derivative-calculator.net with this (n = 3) to see what's going on
	// (3 - (a_1/s_1)-(a_2/s_2)-(a_3/s_3))*(1-((a_1/s_1)*(a_2/s_2)*(a_3/s_3))^(1/3))
	const double intermediate = -(pow(prod_res, 1 / nd) * mat_cost) / (nd * pow(prod_siz, 1 / nd));
	for (idx_t i = 0; i < n; i++) {
		auto &state = states[i].get();
		const auto resd = static_cast<double>(res[i]);
		const auto sizd = static_cast<double>(MaxValue<idx_t>(state.GetRemainingSize(), 1));
		const auto pend = static_cast<double>(state.GetMaterializationPenalty());
		der[i] = intermediate / resd - pend * tp_mult / sizd;
	}
}

idx_t TemporaryMemoryManager::ComputeReservation(const TemporaryMemoryState &temporary_memory_state) const {
	static constexpr idx_t OPTIMIZATION_ITERATIONS_MULTIPLIER = 5;

	// Use vectors for ease
	optional_idx state_index;
	vector<reference<const TemporaryMemoryState>> states;
	vector<idx_t> res;
	vector<double> der;
	idx_t sum_of_initial_res = 0;

	idx_t i = 0;
	for (auto &state : active_states) {
		const auto initial_reservation = ComputeInitialReservation(state);
		sum_of_initial_res += initial_reservation;
		if (RefersToSameObject(state.get(), temporary_memory_state)) {
			state_index = i;
		} else if (state.get().GetReservation() >= state.get().GetRemainingSize()) {
			continue;
		}
		states.emplace_back(state);
		res.push_back(initial_reservation);
		der.push_back(NumericLimits<double>::Maximum());
		i++;
	}
	const idx_t n = i;

	if (sum_of_initial_res >= memory_limit) {
		return res[state_index.GetIndex()];
	}
	const auto free_memory = memory_limit - sum_of_initial_res;

	// Distribute memory in OPTIMIZATION_ITERATIONS
	idx_t remaining_memory = free_memory;
	const idx_t optimization_iterations = OPTIMIZATION_ITERATIONS_MULTIPLIER * n;
	for (idx_t opt_idx = 0; opt_idx < optimization_iterations; opt_idx++) {
		ComputeDerivatives(states, res, der, n);

		// Find the index of the state with the lowest derivative
		idx_t min_idx = 0;
		for (i = 1; i < n; i++) {
			auto &state = states[i].get();
			if (res[i] >= state.GetRemainingSize()) {
				continue; // We can't increase the reservation of "maxed" states, so we skip these
			}
			min_idx = der[i] < der[min_idx] ? i : min_idx;
		}
		auto &min_state = states[min_idx].get();

		// This is how much memory we will distribute in this round
		const auto iter_memory = ExactNumericCast<idx_t>(
		    std::ceil(static_cast<double>(remaining_memory) / static_cast<double>(optimization_iterations - opt_idx)));

		// Compute how much we can add
		const auto state_room = min_state.GetRemainingSize() - res[min_idx];
		const auto delta = MinValue(iter_memory, state_room);

		// Update counts
		res[min_idx] += delta;
		remaining_memory -= delta;
	}
	D_ASSERT(remaining_memory == 0);

	// We computed how the memory should be assigned to the states,
	// but we did not yet take into account the upper bound of MAXIMUM_FREE_MEMORY_RATIO * free_memory.
	// We don't simply bound this state's reservation because order matters,
	// and this function is called in arbitrary order for all active states.
	// So, we order the states by derivative (as if they got the max reservation),
	// and assign the states with the lowest derivate first.
	// This way, order does not matter _as much_, and the "best" states will _likely_ get more memory
	vector<idx_t> idxs(n, 0);
	vector<idx_t> max_res(n, 0);
	for (i = 0; i < n; i++) {
		idxs[i] = i;
		max_res[i] = states[i].get().GetRemainingSize() - 1;
	}
	ComputeDerivatives(states, max_res, der, n);
	std::sort(idxs.begin(), idxs.end(), [&](const idx_t &lhs, const idx_t &rhs) { return der[lhs] < der[rhs]; });

	// Loop through sorted indices until we encounter the state index
	remaining_memory = free_memory;
	for (const auto idx : idxs) {
		auto &state = states[idx].get();
		D_ASSERT(state.GetReservation() < state.GetRemainingSize());
		const auto initial_state_reservation = ComputeInitialReservation(state);
		const auto upper_bound = LossyNumericCast<idx_t>(
		    MAXIMUM_FREE_MEMORY_RATIO * static_cast<double>(initial_state_reservation + remaining_memory));
		auto state_reservation = MinValue(res[idx], upper_bound);
		// But make sure it's never less than the initial reservation
		state_reservation = MaxValue(state_reservation, initial_state_reservation);
		// If this is the current state, we can just return
		if (idx == state_index.GetIndex()) {
			return state_reservation;
		}
		// Decrement the remaining memory
		const auto delta = state_reservation - initial_state_reservation;
		remaining_memory -= delta;
	}

	// We cannot have gotten through the loop above without finding the current state
	throw InternalException("Did not find state_index in ComputeOptimalReservation");
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
