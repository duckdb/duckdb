#include "catch.hpp"
#include "duckdb/optimizer/partition_fold.hpp"

#include <utility>
#include <vector>

using namespace duckdb;

namespace {

//! A client scripted with the state to return per partition, in order. Reduction follows MIN
//! semantics over integer values; whether bounds are excluded is controlled by `excludes`.
struct ScriptedFoldClient {
	explicit ScriptedFoldClient(Value fallback_p, bool excludes_p = true)
	    : fallback(std::move(fallback_p)), excludes(excludes_p) {
	}

	ScriptedFoldClient &Emit(FoldPartitionState state_p, Value value_p = Value()) {
		script.emplace_back(state_p, std::move(value_p));
		return *this;
	}

	FoldPartitionState ClassifyPartition(const FoldPartition &, Value &value) const {
		D_ASSERT(classify_idx < script.size());
		auto &entry = script[classify_idx++];
		value = entry.second;
		return entry.first;
	}

	void CombineCandidate(Value &candidate, Value &value) const {
		combine_calls++;
		if (value < candidate) {
			candidate = std::move(value);
		}
	}

	bool ExcludesCandidate(const Value &, const Value &) const {
		exclude_calls++;
		return excludes;
	}

	Value FallbackValue() const {
		return fallback;
	}

	mutable idx_t combine_calls = 0;
	mutable idx_t exclude_calls = 0;

private:
	vector<pair<FoldPartitionState, Value>> script;
	mutable idx_t classify_idx = 0;
	Value fallback;
	bool excludes;
};

//! A client reducing like COUNT(*): exact values are summed, bounds are never excluded.
struct SummingFoldClient {
	SummingFoldClient &Emit(FoldPartitionState state_p, Value value_p = Value()) {
		script.emplace_back(state_p, std::move(value_p));
		return *this;
	}

	FoldPartitionState ClassifyPartition(const FoldPartition &, Value &value) const {
		D_ASSERT(classify_idx < script.size());
		auto &entry = script[classify_idx++];
		value = entry.second;
		return entry.first;
	}

	void CombineCandidate(Value &candidate, Value &value) const {
		candidate = Value::BIGINT(candidate.GetValue<int64_t>() + value.GetValue<int64_t>());
	}

	bool ExcludesCandidate(const Value &, const Value &) const {
		return false;
	}

	Value FallbackValue() const {
		return Value::BIGINT(0);
	}

private:
	vector<pair<FoldPartitionState, Value>> script;
	mutable idx_t classify_idx = 0;
};

vector<FoldPartition> MakePartitions(idx_t count) {
	vector<FoldPartition> partitions;
	for (idx_t i = 0; i < count; i++) {
		partitions.emplace_back(PartitionStatistics(), i, FilterPropagateResult::FILTER_ALWAYS_TRUE);
	}
	return partitions;
}

} // namespace

TEST_CASE("Exact partitions reduce into the extremum", "[optimizer][partition_fold]") {
	auto partitions = MakePartitions(3);
	ScriptedFoldClient client(Value(-1));
	client.Emit(FoldPartitionState::EXACT_VALUE, Value::BIGINT(5));
	client.Emit(FoldPartitionState::EXACT_VALUE, Value::BIGINT(3));
	client.Emit(FoldPartitionState::EXACT_VALUE, Value::BIGINT(9));

	Value result;
	REQUIRE(PartitionFold(partitions, client, result));
	REQUIRE(result == Value::BIGINT(3));
	REQUIRE(client.combine_calls == 2);
	REQUIRE(client.exclude_calls == 0);
}

TEST_CASE("Neutral partitions are skipped", "[optimizer][partition_fold]") {
	auto partitions = MakePartitions(3);
	ScriptedFoldClient client(Value(-1));
	client.Emit(FoldPartitionState::EXACT_VALUE, Value::BIGINT(5));
	client.Emit(FoldPartitionState::NEUTRAL);
	client.Emit(FoldPartitionState::EXACT_VALUE, Value::BIGINT(3));

	Value result;
	REQUIRE(PartitionFold(partitions, client, result));
	REQUIRE(result == Value::BIGINT(3));
}

TEST_CASE("All-neutral partitions fold to the fallback value", "[optimizer][partition_fold]") {
	auto partitions = MakePartitions(2);
	ScriptedFoldClient client(Value(-1));
	client.Emit(FoldPartitionState::NEUTRAL);
	client.Emit(FoldPartitionState::NEUTRAL);

	Value result;
	REQUIRE(PartitionFold(partitions, client, result));
	REQUIRE(result == Value(-1));
	REQUIRE(client.combine_calls == 0);
}

TEST_CASE("A bound strictly dominated by the candidate is excluded", "[optimizer][partition_fold]") {
	auto partitions = MakePartitions(2);
	ScriptedFoldClient client(Value(-1));
	client.Emit(FoldPartitionState::EXACT_VALUE, Value::BIGINT(5));
	client.Emit(FoldPartitionState::BOUND, Value::BIGINT(7));

	Value result;
	REQUIRE(PartitionFold(partitions, client, result));
	REQUIRE(result == Value::BIGINT(5));
	REQUIRE(client.exclude_calls == 1);
}

TEST_CASE("A bound that may beat the candidate vetoes the fold", "[optimizer][partition_fold]") {
	auto partitions = MakePartitions(2);
	ScriptedFoldClient client(Value(-1), /*excludes=*/false);
	client.Emit(FoldPartitionState::EXACT_VALUE, Value::BIGINT(5));
	client.Emit(FoldPartitionState::BOUND, Value::BIGINT(3));

	Value result;
	REQUIRE(!PartitionFold(partitions, client, result));
}

TEST_CASE("Voting happens after the candidate exists, regardless of partition order", "[optimizer][partition_fold]") {
	auto partitions = MakePartitions(2);
	ScriptedFoldClient client(Value(-1));
	client.Emit(FoldPartitionState::BOUND, Value::BIGINT(7));
	client.Emit(FoldPartitionState::EXACT_VALUE, Value::BIGINT(5));

	Value result;
	REQUIRE(PartitionFold(partitions, client, result));
	REQUIRE(result == Value::BIGINT(5));
	REQUIRE(client.exclude_calls == 1);
}

TEST_CASE("Bounds alone never fold, even when they would be excluded", "[optimizer][partition_fold]") {
	auto partitions = MakePartitions(2);
	ScriptedFoldClient client(Value(-1));
	client.Emit(FoldPartitionState::BOUND, Value::BIGINT(7));
	client.Emit(FoldPartitionState::NEUTRAL);

	Value result;
	REQUIRE(!PartitionFold(partitions, client, result));
}

TEST_CASE("NO_INFO vetoes the fold immediately", "[optimizer][partition_fold]") {
	Value result;

	auto partitions = MakePartitions(2);
	ScriptedFoldClient client(Value(-1));
	client.Emit(FoldPartitionState::EXACT_VALUE, Value::BIGINT(5));
	client.Emit(FoldPartitionState::NO_INFO);
	REQUIRE(!PartitionFold(partitions, client, result));

	auto single_partition = MakePartitions(1);
	ScriptedFoldClient fatal_client(Value(-1));
	fatal_client.Emit(FoldPartitionState::NO_INFO);
	REQUIRE(!PartitionFold(single_partition, fatal_client, result));
}

TEST_CASE("Additive reduction sums exact values and refuses bounds", "[optimizer][partition_fold]") {
	Value result;

	auto partitions = MakePartitions(2);
	SummingFoldClient summing;
	summing.Emit(FoldPartitionState::EXACT_VALUE, Value::BIGINT(2));
	summing.Emit(FoldPartitionState::EXACT_VALUE, Value::BIGINT(3));
	REQUIRE(PartitionFold(partitions, summing, result));
	REQUIRE(result == Value::BIGINT(5));

	auto veto_partitions = MakePartitions(2);
	SummingFoldClient vetoing;
	vetoing.Emit(FoldPartitionState::EXACT_VALUE, Value::BIGINT(2));
	vetoing.Emit(FoldPartitionState::BOUND, Value::BIGINT(1));
	REQUIRE(!PartitionFold(veto_partitions, vetoing, result));
}
