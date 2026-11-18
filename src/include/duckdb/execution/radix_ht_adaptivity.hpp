//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/radix_ht_adaptivity.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class GroupedAggregateHashTable;
class RadixHTGlobalSinkState;
class RadixHTLocalSinkState;

//! Policy for growth, abandonment and lookups of one local sink HT.
//! Lookup decisions are independent of HLL and external aggregation.
class RadixHTAdaptivity {
public:
	//! Handle a full local table or memory pressure, returning whether the table grew.
	bool HandleSinkBoundary(RadixHTGlobalSinkState &gstate, RadixHTLocalSinkState &lstate, bool memory_pressure);
	void Reset();

	//! Minimum cumulative input before lookups may first be skipped
	static constexpr idx_t MIN_INPUT_ROWS_BEFORE_LOOKUP_SKIP = 1024 * 1024;
	//! Minimum input processed while skipping before retrying lookups
	static constexpr idx_t MIN_LOOKUP_RETRY_ROWS = 128 * 1024;

private:
	//! Grow a profitable table within the memory budget, resuming lookups on success.
	static bool TryGrow(RadixHTGlobalSinkState &gstate, RadixHTLocalSinkState &lstate);
	//! Consider skipping lookups using the completed cycle, before abandonment.
	void MaybeSkipLookups(const RadixHTGlobalSinkState &gstate, GroupedAggregateHashTable &ht);
	//! Periodically retry lookups at the existing capacity, after abandonment.
	void MaybeResumeLookups(GroupedAggregateHashTable &ht) const;

private:
	static constexpr idx_t LOOKUP_HISTORY_DECAY_THRESHOLD_ROWS = 128 * 1024;
	static constexpr idx_t MAX_LOOKUP_RETRY_ROWS = 1024 * 1024;
	static constexpr idx_t LOOKUP_RETRY_MATERIALIZED_ROWS_DIVISOR = 8;
	static constexpr double GROWTH_DUPLICATE_BYTES_FACTOR = 4.0;
	static constexpr double MINIMUM_SMALL_TABLE_REDUCTION = 0.05;

private:
	//! Decayed observations from completed lookup-enabled cycles
	idx_t recent_input_rows = 0;
	idx_t recent_reduced_rows = 0;
	//! Input rows before retrying, chosen when skipping starts
	idx_t retry_interval_rows = MIN_LOOKUP_RETRY_ROWS;
};

} // namespace duckdb
