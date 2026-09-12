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

#include <type_traits>

#if defined(__cpp_concepts) && __cpp_concepts >= 201907L
#include <concepts>
#endif

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

#if defined(__cpp_concepts) && __cpp_concepts >= 201907L
// clang-format off
template <typename Client>
concept PartitionFoldClient = requires(const Client &client, const FoldPartition &partition, Value &value) {
	// Classify one partition for this aggregate. On EXACT_VALUE `value` holds the exact value; on
	// BOUND it holds the bound covering every surviving row.
	{ client.ClassifyPartition(partition, value) } -> std::same_as<FoldPartitionState>;
	// Merge an exact value into the running candidate.
	client.CombineCandidate(value, value);
	// Whether a BOUND partition cannot contribute a value strictly better than the candidate. The
	// comparison must be sound for the kind of bound the client returned - statistics that are not
	// safe for a plain comparison (e.g. truncated string prefixes) must not be excluded.
	{ client.ExcludesCandidate(value, value) } -> std::same_as<bool>;
	// The result when every partition is NEUTRAL.
	{ client.FallbackValue() } -> std::same_as<Value>;
};
// clang-format on

template <typename T>
using IsPartitionFoldClient = std::bool_constant<PartitionFoldClient<T>>;
#else
//! C++17 stand-in for the PartitionFoldClient concept above.
template <typename T, typename = void>
struct IsPartitionFoldClient : std::false_type {};

template <typename T>
struct IsPartitionFoldClient<T, std::void_t<decltype(std::declval<const T &>().ClassifyPartition(
                                                std::declval<const FoldPartition &>(), std::declval<Value &>())),
                                            decltype(std::declval<const T &>().CombineCandidate(
                                                std::declval<Value &>(), std::declval<Value &>())),
                                            decltype(std::declval<const T &>().ExcludesCandidate(
                                                std::declval<const Value &>(), std::declval<const Value &>())),
                                            decltype(std::declval<const T &>().FallbackValue())>> : std::true_type {};
#endif

//! Fold one aggregate over the partitions with the compile-time client policy `Client`, keeping the
//! aggregate families free of virtual dispatch and per-aggregate allocations. Bounds only vote: a
//! fold requires an exact source. Returns false when the statistics cannot answer the aggregate.
template <typename Client>
bool PartitionFold(const vector<FoldPartition> &partitions, const Client &client, Value &result) {
	static_assert(IsPartitionFoldClient<Client>::value,
	              "Client must provide ClassifyPartition, CombineCandidate, ExcludesCandidate and FallbackValue");
	Value candidate;
	bool found_candidate = false;
	vector<Value> bounds;
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
			// the partition contributes no value, so it cannot affect the result
			break;
		case FoldPartitionState::BOUND:
			// the bound covers every surviving row but is not attained by any of them: it can never
			// become the candidate, it can only be excluded by one
			D_ASSERT(!value.IsNull());
			bounds.push_back(std::move(value));
			break;
		case FoldPartitionState::NO_INFO:
			// the statistics do not describe the rows that will be read
			return false;
		}
	}
	if (!found_candidate) {
		if (!bounds.empty()) {
			// only an exact source can produce the candidate - a bound alone never folds
			return false;
		}
		// every partition is neutral
		result = client.FallbackValue();
		return true;
	}
	for (auto &bound : bounds) {
		if (!client.ExcludesCandidate(bound, candidate)) {
			// the partition may hold a surviving row that beats the candidate
			return false;
		}
	}
	result = std::move(candidate);
	return true;
}

} // namespace duckdb
