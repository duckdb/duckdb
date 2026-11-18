#include "duckdb/execution/radix_ht_adaptivity.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/execution/radix_ht_sink_state.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

RadixHTAdaptivity::SinkResult RadixHTAdaptivity::HandleSinkBoundary(RadixHTGlobalSinkState &gstate,
                                                                    RadixHTLocalSinkState &lstate,
                                                                    const bool memory_pressure) {
	auto &ht = *lstate.ht;
	if (!memory_pressure) {
		if (TryGrow(gstate, lstate)) {
			return SinkResult::GROWN;
		}
		MaybeSkipLookups(gstate, ht);
	}

	if (gstate.number_of_threads > RadixHTConfig::GROW_STRATEGY_THREAD_THRESHOLD || gstate.external) {
		// Keep the materialized rows and clear only the local pointer table.
		gstate.any_abandoned = true;
		ht.Abandon();
		MaybeResumeLookups(ht);
		return SinkResult::ABANDONED;
	}
	return SinkResult::UNCHANGED;
}

bool RadixHTAdaptivity::TryGrow(RadixHTGlobalSinkState &gstate, RadixHTLocalSinkState &lstate) {
	if (gstate.number_of_threads <= RadixHTConfig::GROW_STRATEGY_THREAD_THRESHOLD || gstate.external) {
		return false;
	}
	auto &ht = *lstate.ht;
	const auto &adaptivity = ht.GetAdaptivityState();
	const auto materialized_count = ht.GetMaterializedCount();
	if (!adaptivity.HLLEnabled() || ht.Count() == materialized_count ||
	    (gstate.spill_plan && gstate.StatePressureExceeded(ht))) {
		return false;
	}

	const auto hll_count = MinValue(adaptivity.GetHLLUpperBound(), materialized_count);
	if (hll_count == 0) {
		return false;
	}

	if (ht.Capacity() > NumericLimits<idx_t>::Maximum() / (3 * sizeof(ht_entry_t))) {
		return false;
	}
	const auto minimum_capacity = ht.Capacity() * 2;
	auto next_capacity = MaxValue(GroupedAggregateHashTable::GetCapacityForCount(hll_count), minimum_capacity);
	if (next_capacity > NumericLimits<idx_t>::Maximum() / sizeof(ht_entry_t)) {
		return false;
	}
	// Without a spill plan the arena stays in memory either way, so it does not count against the growth.
	const auto current_size = gstate.SpillableSize(ht);
	const auto table_size = next_capacity * sizeof(ht_entry_t);
	// Grow only when repeated tuple materialization outweighs the larger pointer table.
	// Without a spill plan, a duplicate row also re-creates its states in the arena, which cannot spill,
	// so count those bytes too.
	const auto arena_bytes_per_row = gstate.spill_plan
	                                     ? 0.0
	                                     : static_cast<double>(ht.GetAggregateAllocator()->AllocationSize()) /
	                                           static_cast<double>(materialized_count);
	const auto duplicate_bytes_per_row = static_cast<double>(ht.GetLayout().GetRowWidth()) + arena_bytes_per_row;
	if (static_cast<double>(materialized_count - hll_count) * duplicate_bytes_per_row <
	    GROWTH_DUPLICATE_BYTES_FACTOR * static_cast<double>(table_size)) {
		return false;
	}

	if (table_size > NumericLimits<idx_t>::Maximum() - current_size) {
		return false;
	}
	// The old pointer table remains allocated until the new allocation succeeds.
	const auto desired_size = current_size + table_size;
	if (desired_size > gstate.GetThreadLimit() && desired_size <= gstate.memory_limit / gstate.number_of_threads) {
		const annotated_lock_guard<annotated_mutex> guard {gstate.lock};
		auto &memory_state = *gstate.temporary_memory_state;
		const auto request = desired_size * gstate.number_of_threads;
		const auto doubled_request = request <= NumericLimits<idx_t>::Maximum() / 2 ? request * 2 : request;
		memory_state.SetRemainingSizeAndUpdateReservation(gstate.context.GetContext(),
		                                                  MaxValue(memory_state.GetRemainingSize(), doubled_request));
	}
	const auto thread_limit = gstate.GetThreadLimit();
	while (next_capacity >= minimum_capacity) {
		if (current_size <= thread_limit && next_capacity <= (thread_limit - current_size) / sizeof(ht_entry_t)) {
			break;
		}
		next_capacity /= 2;
	}
	if (next_capacity < minimum_capacity) {
		return false;
	}

	gstate.any_abandoned = true;
	ht.Abandon();
	try {
		ht.Resize(next_capacity);
	} catch (const OutOfMemoryException &) {
		// Other operators may consume the reservation before the allocation succeeds.
		ht.EnableHLL(false);
		return false;
	}
	ht.ResumeLookups();
	lstate.local_sink_capacity = next_capacity;
	return true;
}

void RadixHTAdaptivity::MaybeSkipLookups(const RadixHTGlobalSinkState &gstate, GroupedAggregateHashTable &ht) {
	const auto &state = ht.GetAdaptivityState();
	if (state.LookupsSkipped() || gstate.number_of_threads <= RadixHTConfig::GROW_STRATEGY_THREAD_THRESHOLD ||
	    ht.Capacity() != gstate.config.sink_capacity) {
		return;
	}

	const auto cycle_input = state.GetCycleInputCount();
	D_ASSERT(cycle_input >= ht.Count());
	if (cycle_input == 0) {
		// Failed growth already abandoned this cycle.
		return;
	}
	if (recent_input_rows >= LOOKUP_HISTORY_DECAY_THRESHOLD_ROWS) {
		recent_input_rows /= 2;
		recent_reduced_rows /= 2;
	}
	recent_input_rows += cycle_input;
	recent_reduced_rows += cycle_input - ht.Count();
	if (state.GetSinkCount() < MIN_INPUT_ROWS_BEFORE_LOOKUP_SKIP) {
		return;
	}
	const auto recent_reduction = static_cast<double>(recent_reduced_rows) / static_cast<double>(recent_input_rows);
	const auto reduction = static_cast<double>(cycle_input - ht.Count()) / static_cast<double>(cycle_input);
	if (reduction < MINIMUM_SMALL_TABLE_REDUCTION && recent_reduction < MINIMUM_SMALL_TABLE_REDUCTION) {
		retry_interval_rows = ClampValue(ht.GetMaterializedCount() / LOOKUP_RETRY_MATERIALIZED_ROWS_DIVISOR,
		                                 MIN_LOOKUP_RETRY_ROWS, MAX_LOOKUP_RETRY_ROWS);
		ht.SkipLookups();
	}
}

void RadixHTAdaptivity::MaybeResumeLookups(GroupedAggregateHashTable &ht) const {
	const auto &state = ht.GetAdaptivityState();
	if (state.LookupsSkipped() && state.GetSkippedInputCount() >= retry_interval_rows) {
		ht.ResumeLookups();
	}
}

void RadixHTAdaptivity::Reset() {
	*this = RadixHTAdaptivity();
}

} // namespace duckdb
