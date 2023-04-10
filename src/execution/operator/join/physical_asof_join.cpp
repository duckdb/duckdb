#include "duckdb/execution/operator/join/physical_asof_join.hpp"

#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/sort/comparators.hpp"
#include "duckdb/common/sort/partition_state.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/join/outer_join_marker.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

PhysicalAsOfJoin::PhysicalAsOfJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   idx_t estimated_cardinality)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::ASOF_JOIN, std::move(cond), join_type, estimated_cardinality) {

	// Convert the conditions partitions and sorts
	for (auto &cond : conditions) {
		D_ASSERT(cond.left->return_type == cond.right->return_type);
		join_key_types.push_back(cond.left->return_type);

		auto left = cond.left->Copy();
		auto right = cond.right->Copy();
		switch (cond.comparison) {
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			lhs_orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, std::move(left));
			rhs_orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, std::move(right));
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_DISTINCT_FROM:
			lhs_partitions.emplace_back(std::move(left));
			rhs_partitions.emplace_back(std::move(right));
			break;
		default:
			// COMPARE EQUAL not supported with merge join
			throw NotImplementedException("Unimplemented join condition for ASOF join");
		}
	}
	D_ASSERT(!lhs_orders.empty());
	D_ASSERT(!rhs_orders.empty());

	children.push_back(std::move(left));
	children.push_back(std::move(right));
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class AsOfGlobalSinkState : public GlobalSinkState {
public:
	AsOfGlobalSinkState(ClientContext &context, const PhysicalAsOfJoin &op)
	    : global_partition(context, op.rhs_partitions, op.rhs_orders, op.children[1]->types, {},
	                       op.estimated_cardinality),
	      is_outer(IsRightOuterJoin(op.join_type)), has_null(false) {
	}

	idx_t Count() const {
		return global_partition.count;
	}

	void Finalize();

	PartitionGlobalSinkState global_partition;

	//	One per partition
	const bool is_outer;
	vector<OuterJoinMarker> right_outers;
	bool has_null;
};

void AsOfGlobalSinkState::Finalize() {
	// for FULL/RIGHT OUTER JOIN, initialize found_match to false for every tuple
	right_outers.reserve(global_partition.hash_groups.size());
	for (const auto &hash_group : global_partition.hash_groups) {
		right_outers.emplace_back(OuterJoinMarker(is_outer));
		right_outers.back().Initialize(hash_group->count);
	}
}

class AsOfLocalSinkState : public LocalSinkState {
public:
	explicit AsOfLocalSinkState(ClientContext &context, PartitionGlobalSinkState &gstate_p)
	    : local_partition(context, gstate_p) {
	}

	void Sink(DataChunk &input_chunk) {
		local_partition.Sink(input_chunk);
	}

	void Combine() {
		local_partition.Combine();
	}

	PartitionLocalSinkState local_partition;
};

unique_ptr<GlobalSinkState> PhysicalAsOfJoin::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<AsOfGlobalSinkState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalAsOfJoin::GetLocalSinkState(ExecutionContext &context) const {
	// We only sink the RHS
	return make_uniq<AsOfLocalSinkState>(context.client, *this);
}

SinkResultType PhysicalAsOfJoin::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                      DataChunk &input) const {
	auto &lstate = lstate_p.Cast<AsOfLocalSinkState>();

	lstate.Sink(input);

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalAsOfJoin::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &lstate = lstate_p.Cast<AsOfLocalSinkState>();
	lstate.Combine();
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalAsOfJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<AsOfGlobalSinkState>();

	gstate.Finalize();

	// Find the first group to sort
	auto &groups = gstate.global_partition.grouping_data->GetPartitions();
	if (groups.empty() && EmptyResultIfRHSIsEmpty()) {
		// Empty input!
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Schedule all the sorts for maximum thread utilisation
	auto new_event = make_shared<PartitionMergeEvent>(gstate.global_partition, pipeline);
	event.InsertEvent(std::move(new_event));

	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class AsOfJoinState : public CachingOperatorState {
public:
	using Orders = vector<BoundOrderByNode>;
	using Match = std::pair<hash_t, idx_t>;

	AsOfJoinState(ClientContext &context, const PhysicalAsOfJoin &op, bool force_external);

public:
	void ResolveJoin(DataChunk &input, bool *found_matches, Match *matches = nullptr);

	void ResolveJoinKeys(DataChunk &input);

	ClientContext &context;
	Allocator &allocator;
	const PhysicalAsOfJoin &op;
	BufferManager &buffer_manager;
	const bool force_external;
	Orders lhs_orders;

	//	LHS sorting
	unique_ptr<GlobalSortState> lhs_global_state;
	RowLayout lhs_layout;
	DataChunk lhs_keys;
	DataChunk lhs_payload;
	ExpressionExecutor lhs_executor;

	// LHS binning
	Vector hash_vector;

	//	Output
	idx_t nsel;
	SelectionVector sel;
	DataChunk scan_payload;
	DataChunk rhs_payload;
	OuterJoinMarker left_outer;
	bool fetch_next_left;
};

AsOfJoinState::AsOfJoinState(ClientContext &context, const PhysicalAsOfJoin &op, bool force_external)
    : context(context), allocator(Allocator::Get(context)), op(op),
      buffer_manager(BufferManager::GetBufferManager(context)), force_external(force_external), lhs_executor(context),
      hash_vector(LogicalType::HASH), left_outer(IsLeftOuterJoin(op.join_type)), fetch_next_left(true) {
	vector<unique_ptr<BaseStatistics>> partition_stats;
	Orders partitions; // Not used.
	PartitionGlobalSinkState::GenerateOrderings(partitions, lhs_orders, op.lhs_partitions, op.lhs_orders,
	                                            partition_stats);

	lhs_layout.Initialize(op.children[0]->types);

	lhs_keys.Initialize(allocator, op.join_key_types);
	for (const auto &cond : op.conditions) {
		lhs_executor.AddExpression(*cond.left);
	}

	lhs_payload.Initialize(allocator, op.children[0]->types);
	rhs_payload.Initialize(allocator, op.children[1]->types);
	scan_payload.Initialize(allocator, op.children[1]->types);

	sel.Initialize();
}

void AsOfJoinState::ResolveJoinKeys(DataChunk &input) {
	//	Compute the join keys
	lhs_keys.Reset();
	lhs_executor.Execute(input, lhs_keys);

	// 	Sort on them
	lhs_global_state = make_uniq<GlobalSortState>(buffer_manager, lhs_orders, lhs_layout);
	auto &global_state = *lhs_global_state;
	LocalSortState local_sort;
	local_sort.Initialize(*lhs_global_state, buffer_manager);

	local_sort.SinkChunk(lhs_keys, input);

	// Set external (can be forced with the PRAGMA)
	global_state.external = force_external;
	global_state.AddLocalState(local_sort);
	global_state.PrepareMergePhase();
	while (global_state.sorted_blocks.size() > 1) {
		MergeSorter merge_sorter(*lhs_global_state, buffer_manager);
		merge_sorter.PerformInMergeRound();
		global_state.CompleteMergeRound();
	}

	// Scan the sorted payload
	D_ASSERT(global_state.sorted_blocks.size() == 1);

	auto scanner = make_uniq<PayloadScanner>(*global_state.sorted_blocks[0]->payload_data, global_state, false);
	lhs_payload.Reset();
	scanner->Scan(lhs_payload);

	//	Hash to find the partition
	const auto count = input.size();
	auto &global_partition = op.sink_state->Cast<AsOfGlobalSinkState>().global_partition;
	if (op.lhs_partitions.empty()) {
		// Only one hash group
		hash_vector.Reference(Value::HASH(0));
	} else {
		//	Hash to determine the partitions.
		VectorOperations::Hash(lhs_keys.data[0], hash_vector, count);
		for (size_t prt_idx = 1; prt_idx < op.lhs_partitions.size(); ++prt_idx) {
			VectorOperations::CombineHash(hash_vector, lhs_keys.data[prt_idx], count);
		}

		// Convert hashes to hash groups
		const auto radix_bits = global_partition.grouping_data->GetRadixBits();
		RadixPartitioning::HashesToBins(hash_vector, radix_bits, hash_vector, count);
	}
}

void AsOfJoinState::ResolveJoin(DataChunk &input, bool *found_match, std::pair<hash_t, idx_t> *matches) {
	//	Sort the input into lhs_payload, radix keys in lhs_global_state
	ResolveJoinKeys(input);

	const auto count = input.size();
	auto &gsink = op.sink_state->Cast<AsOfGlobalSinkState>();
	auto &global_partition = gsink.global_partition;

	// The bins are contiguous from sorting, so load them one at a time
	const auto bins = FlatVector::GetData<hash_t>(hash_vector);
	hash_t prev_bin = global_partition.hash_groups.size();
	PartitionGlobalHashGroup *hash_group = nullptr;
	OuterJoinMarker *right_outer = nullptr;
	unique_ptr<SBIterator> right;
	SBIterator left(*lhs_global_state, ExpressionType::COMPARE_LESSTHANOREQUALTO);
	nsel = 0;
	for (idx_t i = 0; i < count; ++i) {
		const auto curr_bin = bins[i];
		if (!hash_group || curr_bin != prev_bin) {
			//	Grab the next group
			hash_group = global_partition.hash_groups[curr_bin].get();
			right_outer = gsink.right_outers.data() + curr_bin;
			prev_bin = curr_bin;
			right = make_uniq<SBIterator>(*(hash_group->global_sort), ExpressionType::COMPARE_LESSTHANOREQUALTO);
		}
		left.SetIndex(i);

		// Exponential search for the first matching value using radix iterators
		// (We use exponential search to avoid thrashing the block manager on large probes)
		idx_t bound = 1;
		idx_t begin = right->GetIndex();
		right->SetIndex(begin + bound);
		while (right->GetIndex() < hash_group->count) {
			//	If left < right
			if (left.Compare(*right)) {
				//	Too low, jump ahead
				bound *= 2;
				right->SetIndex(begin + bound);
			} else {
				break;
			}
		}

		//	Binary search
		auto first = begin + bound / 2;
		auto last = MinValue<idx_t>(begin + bound + 1, hash_group->count);
		while (first < last) {
			const auto it = first + (last - first) / 2;
			right->SetIndex(it);
			if (left.Compare(*right)) {
				last = it;
			} else {
				first = it + 1;
			}
		}
		right->SetIndex(first);
		right_outer->SetMatch(first);

		if (first >= hash_group->count) {
			//	LHS is before everything, so no match
			continue;
		}

		//	Check partitions for strict equality
		if (!op.lhs_partitions.empty() && hash_group->ComparePartitions(left, *right)) {
			continue;
		}

		// Emit match data
		left_outer.SetMatch(i);
		if (found_match) {
			found_match[i] = true;
		}
		if (matches) {
			matches[i] = Match(curr_bin, first);
		}
		sel.set_index(nsel++, i);
	}
}

unique_ptr<OperatorState> PhysicalAsOfJoin::GetOperatorState(ExecutionContext &context) const {
	auto &config = ClientConfig::GetConfig(context.client);
	return make_uniq<AsOfJoinState>(context.client, *this, config.force_external);
}

void PhysicalAsOfJoin::ResolveSimpleJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                         OperatorState &state_p) const {
	auto &state = state_p.Cast<AsOfJoinState>();
	auto &gstate = sink_state->Cast<AsOfGlobalSinkState>();

	// perform the actual join
	bool found_match[STANDARD_VECTOR_SIZE] = {false};
	state.ResolveJoin(input, found_match);

	// use the sorted payload
	auto &payload = state.lhs_payload;

	// now construct the result based on the join result
	switch (join_type) {
	case JoinType::MARK: {
		PhysicalJoin::ConstructMarkJoinResult(state.lhs_keys, payload, chunk, found_match, gstate.has_null);
		break;
	}
	case JoinType::SEMI:
		PhysicalJoin::ConstructSemiJoinResult(payload, chunk, found_match);
		break;
	case JoinType::ANTI:
		PhysicalJoin::ConstructAntiJoinResult(payload, chunk, found_match);
		break;
	default:
		throw NotImplementedException("Unimplemented join type for AsOf join");
	}
}

OperatorResultType PhysicalAsOfJoin::ResolveComplexJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                        OperatorState &state_p) const {
	auto &state = state_p.Cast<AsOfJoinState>();
	auto &gstate = sink_state->Cast<AsOfGlobalSinkState>();

	if (!state.fetch_next_left) {
		state.fetch_next_left = true;
		if (state.left_outer.Enabled()) {
			// left join: before we move to the next chunk, see if we need to output any vectors that didn't
			// have a match found
			state.left_outer.ConstructLeftJoinResult(input, chunk);
			state.left_outer.Reset();
		}
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// perform the actual join
	AsOfJoinState::Match matches[STANDARD_VECTOR_SIZE];
	state.ResolveJoin(input, nullptr, matches);
	state.rhs_payload.Reset();
	state.scan_payload.Reset();

	auto &global_partition = gstate.global_partition;
	hash_t scan_bin = global_partition.hash_groups.size();
	PartitionGlobalHashGroup *hash_group = nullptr;
	unique_ptr<PayloadScanner> scanner;
	for (idx_t i = 0; i < state.nsel; ++i) {
		const auto match_bin = matches[i].first;
		const auto match_pos = matches[i].second;
		if (match_bin != scan_bin) {
			//	Grab the next group
			hash_group = global_partition.hash_groups[match_bin].get();
			scan_bin = match_bin;
			scanner = make_uniq<PayloadScanner>(*hash_group->global_sort, false);
			state.scan_payload.Reset();
		}
		// Skip to the range containing the match
		while (match_pos >= scanner->Scanned() + state.scan_payload.size()) {
			state.scan_payload.Reset();
			scanner->Scan(state.scan_payload);
		}
		// Append the individual values
		// TODO: Batch the copies
		const auto source_offset = match_pos - scanner->Scanned();
		for (column_t col_idx = 0; col_idx < state.lhs_payload.ColumnCount(); ++col_idx) {
			auto &source = state.scan_payload.data[col_idx];
			auto &target = state.rhs_payload.data[input.ColumnCount() + col_idx];
			VectorOperations::Copy(source, target, 1, source_offset, i);
		}
	}

	//	Slice the sorted payload into the left side
	chunk.Slice(state.lhs_payload, state.sel, state.nsel);
	chunk.Slice(state.rhs_payload, state.sel, state.nsel, input.ColumnCount());
	chunk.SetCardinality(state.nsel);

	//	If we are doing a left join, come back for the NULLs
	if (state.left_outer.Enabled()) {
		state.fetch_next_left = false;
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}

	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorResultType PhysicalAsOfJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                     GlobalOperatorState &gstate_p, OperatorState &state) const {
	auto &gstate = sink_state->Cast<AsOfGlobalSinkState>();

	if (gstate.global_partition.count == 0) {
		// empty RHS
		if (!EmptyResultIfRHSIsEmpty()) {
			ConstructEmptyJoinResult(join_type, gstate.has_null, input, chunk);
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			return OperatorResultType::FINISHED;
		}
	}

	input.Verify();
	switch (join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
		// simple joins can have max STANDARD_VECTOR_SIZE matches per chunk
		ResolveSimpleJoin(context, input, chunk, state);
		return OperatorResultType::NEED_MORE_INPUT;
	case JoinType::LEFT:
	case JoinType::INNER:
	case JoinType::RIGHT:
	case JoinType::OUTER:
		return ResolveComplexJoin(context, input, chunk, state);
	default:
		throw NotImplementedException("Unimplemented type for as-of join!");
	}
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class AsOfGlobalSourceState : public GlobalSourceState {
public:
	explicit AsOfGlobalSourceState(PartitionGlobalSinkState &gsink_p) : gsink(gsink_p), next_bin(0) {
	}

	PartitionGlobalSinkState &gsink;
	//! The output read position.
	atomic<idx_t> next_bin;

public:
	idx_t MaxThreads() override {
		// If there is only one partition, we have to process it on one thread.
		if (!gsink.grouping_data) {
			return 1;
		}

		// If there is not a lot of data, process serially.
		if (gsink.count < STANDARD_ROW_GROUPS_SIZE) {
			return 1;
		}

		return gsink.hash_groups.size();
	}
};

class AsOfLocalSourceState {
public:
	using HashGroupPtr = unique_ptr<PartitionGlobalHashGroup>;

	explicit AsOfLocalSourceState(AsOfGlobalSinkState &gstate_p);

	idx_t GeneratePartition(const idx_t hash_bin);

	AsOfGlobalSinkState &gstate;

	//! The read partition
	idx_t hash_bin;
	HashGroupPtr hash_group;

	//! The read cursor
	unique_ptr<PayloadScanner> scanner;
	//! Buffer for the inputs
	DataChunk input_chunk;
	//! Pointer to the matches
	const bool *found_match;
};

AsOfLocalSourceState::AsOfLocalSourceState(AsOfGlobalSinkState &gstate_p) : gstate(gstate_p) {
	input_chunk.Initialize(gstate.global_partition.allocator, gstate.global_partition.payload_types);
}

idx_t AsOfLocalSourceState::GeneratePartition(const idx_t hash_bin_p) {
	//	Get rid of any stale data
	hash_bin = hash_bin_p;

	hash_group = std::move(gstate.global_partition.hash_groups[hash_bin]);
	scanner = make_uniq<PayloadScanner>(*hash_group->global_sort);
	found_match = gstate.right_outers[hash_bin].GetMatches();

	return scanner->Remaining();
}

unique_ptr<GlobalSourceState> PhysicalAsOfJoin::GetGlobalSourceState(ClientContext &context) const {
	auto &gsink = sink_state->Cast<AsOfGlobalSinkState>();
	return make_uniq<AsOfGlobalSourceState>(gsink.global_partition);
}

void PhysicalAsOfJoin::GetData(ExecutionContext &context, DataChunk &result, GlobalSourceState &gstate_p,
                               LocalSourceState &lstate_p) const {
	D_ASSERT(IsRightOuterJoin(join_type));

	auto &gsource = gstate_p.Cast<AsOfGlobalSourceState>();
	auto &lsource = lstate_p.Cast<AsOfLocalSourceState>();
	auto &gsink = gsource.gsink;

	auto &hash_groups = gsink.hash_groups;
	const auto bin_count = hash_groups.size();

	DataChunk rhs_chunk;
	rhs_chunk.Initialize(Allocator::Get(context.client), gsink.payload_types);
	SelectionVector rsel(STANDARD_VECTOR_SIZE);

	while (result.size() == 0) {
		//	Move to the next bin if we are done.
		while (!lsource.scanner || !lsource.scanner->Remaining()) {
			lsource.scanner.reset();
			lsource.hash_group.reset();
			auto hash_bin = gsource.next_bin++;
			if (hash_bin >= bin_count) {
				return;
			}

			for (; hash_bin < hash_groups.size(); hash_bin = gsource.next_bin++) {
				if (hash_groups[hash_bin]) {
					break;
				}
			}
			lsource.GeneratePartition(hash_bin);
		}
		const auto rhs_position = lsource.scanner->Scanned();
		lsource.scanner->Scan(rhs_chunk);

		const auto count = rhs_chunk.size();
		if (count == 0) {
			return;
		}

		// figure out which tuples didn't find a match in the RHS
		auto found_match = lsource.found_match;
		idx_t result_count = 0;
		for (idx_t i = 0; i < count; i++) {
			if (!found_match[rhs_position + i]) {
				rsel.set_index(result_count++, i);
			}
		}

		if (result_count > 0) {
			// if there were any tuples that didn't find a match, output them
			const idx_t left_column_count = children[0]->types.size();
			for (idx_t col_idx = 0; col_idx < left_column_count; ++col_idx) {
				result.data[col_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result.data[col_idx], true);
			}
			const idx_t right_column_count = children[1]->types.size();
			;
			for (idx_t col_idx = 0; col_idx < right_column_count; ++col_idx) {
				result.data[left_column_count + col_idx].Slice(rhs_chunk.data[col_idx], rsel, result_count);
			}
			result.SetCardinality(result_count);
			return;
		}
	}
}

} // namespace duckdb
