//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/partition_fold.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/partition_stats.hpp"

namespace duckdb {

//! Classification of one partition's statistics for one aggregate.
enum class FoldPartitionState : uint8_t {
	//! The statistics are exact and produced a usable value - the only foldable outcome
	EXACT_VALUE,
	//! The statistics bound every surviving row of the partition, but are not exact: good enough to
	//! vote with, never good enough to fold
	BOUND,
	//! The statistics are exact, but the partition contributes no value for this aggregate (e.g. a
	//! partition holding only NULL values, ignored by MIN/MAX)
	NEUTRAL,
	//! No reliable state at all: the statistics do not describe the rows that will be read
	NO_INFO
};

//! One partition over which aggregates may be folded: its statistics plus where it came from.
struct FoldPartition {
	FoldPartition() = default;
	FoldPartition(PartitionStatistics stats_p, idx_t original_index_p, FilterPropagateResult filter_result_p)
	    : stats(std::move(stats_p)), original_index(original_index_p), filter_result(filter_result_p) {
	}

	PartitionStatistics stats;
	//! The index of this partition in the row-group list, shared by filter classification and folding
	idx_t original_index;
	//! Verdict of the table filters over the whole partition
	FilterPropagateResult filter_result;
};

//! Fold one aggregate over the partitions with the compile-time client policy `Client`. A policy
//! provides ClassifyPartition, CombineCandidate and FallbackValue; using a template keeps the
//! per-aggregate families free of virtual dispatch and per-aggregate allocations. Returns false
//! when the statistics cannot answer the aggregate - the caller then keeps the original plan.
template <typename Client>
bool PartitionFold(const vector<FoldPartition> &partitions, const Client &client, Value &result) {
	Value candidate;
	bool found_candidate = false;
	for (auto &partition : partitions) {
		Value value;
		switch (client.ClassifyPartition(partition, value)) {
		case FoldPartitionState::EXACT_VALUE:
			if (!found_candidate) {
				candidate = std::move(value);
				found_candidate = true;
			} else {
				client.CombineCandidate(candidate, value);
			}
			break;
		case FoldPartitionState::NEUTRAL:
			// the partition contributes no value, so it cannot affect the extremum
			break;
		case FoldPartitionState::BOUND:
			// a bound always carries the value that bounds the partition
			D_ASSERT(!value.IsNull());
			// a bound is never exact: only an exact value can become a folded constant
			return false;
		case FoldPartitionState::NO_INFO:
			// the statistics do not describe the rows that will be read
			return false;
		}
	}
	result = found_candidate ? std::move(candidate) : client.FallbackValue();
	return true;
}

} // namespace duckdb
