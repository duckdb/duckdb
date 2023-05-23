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

PhysicalAsOfJoin::PhysicalAsOfJoin(LogicalComparisonJoin &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::ASOF_JOIN, std::move(op.conditions), op.join_type,
                             op.estimated_cardinality) {

	// Convert the conditions partitions and sorts
	for (auto &cond : conditions) {
		D_ASSERT(cond.left->return_type == cond.right->return_type);
		join_key_types.push_back(cond.left->return_type);

		auto left = cond.left->Copy();
		auto right = cond.right->Copy();
		switch (cond.comparison) {
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			null_sensitive.emplace_back(lhs_orders.size());
			lhs_orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, std::move(left));
			rhs_orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, std::move(right));
			break;
		case ExpressionType::COMPARE_EQUAL:
			null_sensitive.emplace_back(lhs_orders.size());
			// Fall through
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			lhs_partitions.emplace_back(std::move(left));
			rhs_partitions.emplace_back(std::move(right));
			break;
		default:
			throw NotImplementedException("Unsupported join condition for ASOF join");
		}
	}
	D_ASSERT(!lhs_orders.empty());
	D_ASSERT(!rhs_orders.empty());

	children.push_back(std::move(left));
	children.push_back(std::move(right));

	//	Fill out the right projection map.
	right_projection_map = op.right_projection_map;
	if (right_projection_map.empty()) {
		const auto right_count = children[1]->types.size();
		right_projection_map.reserve(right_count);
		for (column_t i = 0; i < right_count; ++i) {
			right_projection_map.emplace_back(i);
		}
	}
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

	PartitionGlobalSinkState global_partition;

	//	One per partition
	const bool is_outer;
	vector<OuterJoinMarker> right_outers;
	bool has_null;
};

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
	auto &gsink = sink_state->Cast<AsOfGlobalSinkState>();
	return make_uniq<AsOfLocalSinkState>(context.client, gsink.global_partition);
}

SinkResultType PhysicalAsOfJoin::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<AsOfLocalSinkState>();

	lstate.Sink(chunk);

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
class AsOfGlobalState : public GlobalOperatorState {
public:
	explicit AsOfGlobalState(AsOfGlobalSinkState &gsink) {
		// for FULL/RIGHT OUTER JOIN, initialize right_outers to false for every tuple
		auto &global_partition = gsink.global_partition;
		auto &right_outers = gsink.right_outers;
		right_outers.reserve(global_partition.hash_groups.size());
		for (const auto &hash_group : global_partition.hash_groups) {
			right_outers.emplace_back(OuterJoinMarker(gsink.is_outer));
			right_outers.back().Initialize(hash_group->count);
		}
	}
};

unique_ptr<GlobalOperatorState> PhysicalAsOfJoin::GetGlobalOperatorState(ClientContext &context) const {
	auto &gsink = sink_state->Cast<AsOfGlobalSinkState>();
	return make_uniq<AsOfGlobalState>(gsink);
}

class AsOfLocalState : public CachingOperatorState {
public:
	using Orders = vector<BoundOrderByNode>;
	using Match = std::pair<hash_t, idx_t>;

	AsOfLocalState(ClientContext &context, const PhysicalAsOfJoin &op, bool force_external);

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
	ExpressionExecutor lhs_executor;
	DataChunk lhs_keys;
	ValidityMask lhs_valid_mask;
	SelectionVector lhs_sel;
	idx_t lhs_valid;
	RowLayout lhs_layout;
	unique_ptr<GlobalSortState> lhs_global_state;
	DataChunk lhs_sorted;

	// LHS binning
	Vector hash_vector;
	Vector bin_vector;

	//	Output
	idx_t lhs_match_count;
	SelectionVector lhs_matched;
	OuterJoinMarker left_outer;
	bool fetch_next_left;
	DataChunk group_payload;
	DataChunk rhs_payload;
};

AsOfLocalState::AsOfLocalState(ClientContext &context, const PhysicalAsOfJoin &op, bool force_external)
    : context(context), allocator(Allocator::Get(context)), op(op),
      buffer_manager(BufferManager::GetBufferManager(context)), force_external(force_external), lhs_executor(context),
      hash_vector(LogicalType::HASH), bin_vector(LogicalType::HASH), left_outer(IsLeftOuterJoin(op.join_type)),
      fetch_next_left(true) {
	vector<unique_ptr<BaseStatistics>> partition_stats;
	Orders partitions; // Not used.
	PartitionGlobalSinkState::GenerateOrderings(partitions, lhs_orders, op.lhs_partitions, op.lhs_orders,
	                                            partition_stats);

	//	We sort the row numbers of the incoming block, not the rows
	lhs_layout.Initialize({LogicalType::UINTEGER});
	lhs_sorted.Initialize(allocator, lhs_layout.GetTypes());

	lhs_keys.Initialize(allocator, op.join_key_types);
	for (const auto &cond : op.conditions) {
		lhs_executor.AddExpression(*cond.left);
	}

	group_payload.Initialize(allocator, op.children[1]->types);
	rhs_payload.Initialize(allocator, op.children[1]->types);

	lhs_matched.Initialize();
	lhs_sel.Initialize();
	left_outer.Initialize(STANDARD_VECTOR_SIZE);
}

void AsOfLocalState::ResolveJoinKeys(DataChunk &input) {
	//	Compute the join keys
	lhs_keys.Reset();
	lhs_executor.Execute(input, lhs_keys);

	//	Extract the NULLs
	const auto count = input.size();
	lhs_valid_mask.Reset();
	for (auto col_idx : op.null_sensitive) {
		auto &col = lhs_keys.data[col_idx];
		UnifiedVectorFormat unified;
		col.ToUnifiedFormat(count, unified);
		lhs_valid_mask.Combine(unified.validity, count);
	}

	//	Convert the mask to a selection vector.
	//	We need this anyway for sorting
	lhs_valid = 0;
	const auto entry_count = lhs_valid_mask.EntryCount(count);
	idx_t base_idx = 0;
	for (idx_t entry_idx = 0; entry_idx < entry_count;) {
		const auto validity_entry = lhs_valid_mask.GetValidityEntry(entry_idx++);
		const auto next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
		if (ValidityMask::AllValid(validity_entry)) {
			for (; base_idx < next; ++base_idx) {
				lhs_sel.set_index(lhs_valid++, base_idx);
			}
		} else if (ValidityMask::NoneValid(validity_entry)) {
			base_idx = next;
		} else {
			const auto start = base_idx;
			for (; base_idx < next; ++base_idx) {
				if (ValidityMask::RowIsValid(validity_entry, base_idx - start)) {
					lhs_sel.set_index(lhs_valid++, base_idx);
				}
			}
		}
	}

	//	Slice the keys to the ones we can match
	if (lhs_valid < count) {
		lhs_keys.Slice(lhs_sel, lhs_valid);
	}

	//	Hash to assign the partitions
	auto &global_partition = op.sink_state->Cast<AsOfGlobalSinkState>().global_partition;
	if (op.lhs_partitions.empty()) {
		// Only one hash group
		bin_vector.Reference(Value::HASH(0));
	} else {
		//	Hash to determine the partitions.
		VectorOperations::Hash(lhs_keys.data[0], hash_vector, lhs_sel, lhs_valid);
		for (size_t prt_idx = 1; prt_idx < op.lhs_partitions.size(); ++prt_idx) {
			VectorOperations::CombineHash(hash_vector, lhs_keys.data[prt_idx], lhs_sel, lhs_valid);
		}

		// Convert hashes to hash groups
		const auto radix_bits = global_partition.grouping_data->GetRadixBits();
		RadixPartitioning::HashesToBins(hash_vector, radix_bits, bin_vector, count);
	}

	// 	Sort the selection vector on the valid keys
	lhs_global_state = make_uniq<GlobalSortState>(buffer_manager, lhs_orders, lhs_layout);
	auto &global_state = *lhs_global_state;
	LocalSortState local_sort;
	local_sort.Initialize(*lhs_global_state, buffer_manager);

	DataChunk payload_chunk;
	payload_chunk.InitializeEmpty({LogicalType::UINTEGER});
	FlatVector::SetData(payload_chunk.data[0], data_ptr_cast(lhs_sel.data()));
	payload_chunk.SetCardinality(lhs_valid);
	local_sort.SinkChunk(lhs_keys, payload_chunk);

	// Set external (can be forced with the PRAGMA)
	global_state.external = force_external;
	global_state.AddLocalState(local_sort);
	global_state.PrepareMergePhase();
	while (global_state.sorted_blocks.size() > 1) {
		MergeSorter merge_sorter(*lhs_global_state, buffer_manager);
		merge_sorter.PerformInMergeRound();
		global_state.CompleteMergeRound();
	}

	// Scan the sorted selection
	D_ASSERT(global_state.sorted_blocks.size() == 1);

	auto scanner = make_uniq<PayloadScanner>(*global_state.sorted_blocks[0]->payload_data, global_state, false);
	lhs_sorted.Reset();
	scanner->Scan(lhs_sorted);
}

void AsOfLocalState::ResolveJoin(DataChunk &input, bool *found_match, std::pair<hash_t, idx_t> *matches) {
	//	Sort the input into lhs_payload, radix keys in lhs_global_state
	ResolveJoinKeys(input);

	auto &gsink = op.sink_state->Cast<AsOfGlobalSinkState>();
	auto &global_partition = gsink.global_partition;

	// The bins are contiguous from sorting, so load them one at a time
	// But they may be constant, so unify.
	UnifiedVectorFormat bin_unified;
	bin_vector.ToUnifiedFormat(lhs_valid, bin_unified);
	const auto bins = UnifiedVectorFormat::GetData<hash_t>(bin_unified);

	hash_t prev_bin = global_partition.bin_groups.size();
	optional_ptr<PartitionGlobalHashGroup> hash_group;
	optional_ptr<OuterJoinMarker> right_outer;
	//	Searching for right <= left
	SBIterator left(*lhs_global_state, ExpressionType::COMPARE_LESSTHANOREQUALTO);
	unique_ptr<SBIterator> right;
	lhs_match_count = 0;
	const auto sorted_sel = FlatVector::GetData<sel_t>(lhs_sorted.data[0]);
	for (idx_t i = 0; i < lhs_valid; ++i) {
		//	idx is the index in the input; i is the index in the sorted keys
		const auto idx = sorted_sel[i];
		const auto curr_bin = bins[bin_unified.sel->get_index(idx)];
		if (!hash_group || curr_bin != prev_bin) {
			//	Grab the next group
			prev_bin = curr_bin;
			const auto group_idx = global_partition.bin_groups[curr_bin];
			if (group_idx >= global_partition.hash_groups.size()) {
				//	No matching partition
				hash_group = nullptr;
				right_outer = nullptr;
				right.reset();
				continue;
			}
			hash_group = global_partition.hash_groups[group_idx].get();
			right_outer = gsink.right_outers.data() + group_idx;
			right = make_uniq<SBIterator>(*(hash_group->global_sort), ExpressionType::COMPARE_LESSTHANOREQUALTO);
		}
		left.SetIndex(i);

		//	If right > left, then there is no match
		if (!right->Compare(left)) {
			continue;
		}

		// Exponential search forward for a non-matching value using radix iterators
		// (We use exponential search to avoid thrashing the block manager on large probes)
		idx_t bound = 1;
		idx_t begin = right->GetIndex();
		right->SetIndex(begin + bound);
		while (right->GetIndex() < hash_group->count) {
			if (right->Compare(left)) {
				//	If right <= left, jump ahead
				bound *= 2;
				right->SetIndex(begin + bound);
			} else {
				break;
			}
		}

		//	Binary search for the first non-matching value using radix iterators
		//	The previous value (which we know exists) is the match
		auto first = begin + bound / 2;
		auto last = MinValue<idx_t>(begin + bound, hash_group->count);
		while (first < last) {
			const auto mid = first + (last - first) / 2;
			right->SetIndex(mid);
			if (right->Compare(left)) {
				//	If right <= left, new lower bound
				first = mid + 1;
			} else {
				last = mid;
			}
		}
		right->SetIndex(--first);

		//	Check partitions for strict equality
		if (!op.lhs_partitions.empty() && hash_group->ComparePartitions(left, *right)) {
			continue;
		}

		// Emit match data
		right_outer->SetMatch(first);
		left_outer.SetMatch(idx);
		if (found_match) {
			found_match[idx] = true;
		}
		if (matches) {
			matches[idx] = Match(curr_bin, first);
		}
		lhs_matched.set_index(lhs_match_count++, idx);
	}
}

unique_ptr<OperatorState> PhysicalAsOfJoin::GetOperatorState(ExecutionContext &context) const {
	auto &config = ClientConfig::GetConfig(context.client);
	return make_uniq<AsOfLocalState>(context.client, *this, config.force_external);
}

void PhysicalAsOfJoin::ResolveSimpleJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                         OperatorState &lstate_p) const {
	auto &lstate = lstate_p.Cast<AsOfLocalState>();
	auto &gsink = sink_state->Cast<AsOfGlobalSinkState>();

	// perform the actual join
	bool found_match[STANDARD_VECTOR_SIZE] = {false};
	lstate.ResolveJoin(input, found_match);

	// now construct the result based on the join result
	switch (join_type) {
	case JoinType::MARK: {
		PhysicalJoin::ConstructMarkJoinResult(lstate.lhs_keys, input, chunk, found_match, gsink.has_null);
		break;
	}
	case JoinType::SEMI:
		PhysicalJoin::ConstructSemiJoinResult(input, chunk, found_match);
		break;
	case JoinType::ANTI:
		PhysicalJoin::ConstructAntiJoinResult(input, chunk, found_match);
		break;
	default:
		throw NotImplementedException("Unimplemented join type for AsOf join");
	}
}

OperatorResultType PhysicalAsOfJoin::ResolveComplexJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                        OperatorState &lstate_p) const {
	auto &lstate = lstate_p.Cast<AsOfLocalState>();
	auto &gsink = sink_state->Cast<AsOfGlobalSinkState>();

	if (!lstate.fetch_next_left) {
		lstate.fetch_next_left = true;
		if (lstate.left_outer.Enabled()) {
			// left join: before we move to the next chunk, see if we need to output any vectors that didn't
			// have a match found
			lstate.left_outer.ConstructLeftJoinResult(input, chunk);
			lstate.left_outer.Reset();
		}
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// perform the actual join
	AsOfLocalState::Match matches[STANDARD_VECTOR_SIZE];
	lstate.ResolveJoin(input, nullptr, matches);
	lstate.group_payload.Reset();
	lstate.rhs_payload.Reset();

	auto &global_partition = gsink.global_partition;
	hash_t scan_bin = global_partition.bin_groups.size();
	optional_ptr<PartitionGlobalHashGroup> hash_group;
	unique_ptr<PayloadScanner> scanner;
	for (idx_t i = 0; i < lstate.lhs_match_count; ++i) {
		const auto idx = lstate.lhs_matched[i];
		const auto match_bin = matches[idx].first;
		const auto match_pos = matches[idx].second;
		if (match_bin != scan_bin) {
			//	Grab the next group
			const auto group_idx = global_partition.bin_groups[match_bin];
			hash_group = global_partition.hash_groups[group_idx].get();
			scan_bin = match_bin;
			scanner = make_uniq<PayloadScanner>(*hash_group->global_sort, false);
			lstate.group_payload.Reset();
		}
		// Skip to the range containing the match
		while (match_pos >= scanner->Scanned()) {
			lstate.group_payload.Reset();
			scanner->Scan(lstate.group_payload);
		}
		// Append the individual values
		// TODO: Batch the copies
		const auto source_offset = match_pos - (scanner->Scanned() - lstate.group_payload.size());
		for (idx_t col_idx = 0; col_idx < right_projection_map.size(); ++col_idx) {
			const auto rhs_idx = right_projection_map[col_idx];
			auto &source = lstate.group_payload.data[rhs_idx];
			auto &target = chunk.data[input.ColumnCount() + col_idx];
			VectorOperations::Copy(source, target, source_offset + 1, source_offset, i);
		}
	}

	//	Slice the input into the left side
	chunk.Slice(input, lstate.lhs_matched, lstate.lhs_match_count);

	//	If we are doing a left join, come back for the NULLs
	if (lstate.left_outer.Enabled()) {
		lstate.fetch_next_left = false;
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}

	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorResultType PhysicalAsOfJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                     GlobalOperatorState &gstate, OperatorState &lstate) const {
	auto &gsink = sink_state->Cast<AsOfGlobalSinkState>();

	if (gsink.global_partition.count == 0) {
		// empty RHS
		if (!EmptyResultIfRHSIsEmpty()) {
			ConstructEmptyJoinResult(join_type, gsink.has_null, input, chunk);
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
		ResolveSimpleJoin(context, input, chunk, lstate);
		return OperatorResultType::NEED_MORE_INPUT;
	case JoinType::LEFT:
	case JoinType::INNER:
	case JoinType::RIGHT:
	case JoinType::OUTER:
		return ResolveComplexJoin(context, input, chunk, lstate);
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

unique_ptr<GlobalSourceState> PhysicalAsOfJoin::GetGlobalSourceState(ClientContext &context) const {
	auto &gsink = sink_state->Cast<AsOfGlobalSinkState>();
	return make_uniq<AsOfGlobalSourceState>(gsink.global_partition);
}

class AsOfLocalSourceState : public LocalSourceState {
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

unique_ptr<LocalSourceState> PhysicalAsOfJoin::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	auto &gsink = sink_state->Cast<AsOfGlobalSinkState>();
	return make_uniq<AsOfLocalSourceState>(gsink);
}

SourceResultType PhysicalAsOfJoin::GetData(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSourceInput &input) const {
	D_ASSERT(IsRightOuterJoin(join_type));

	auto &gsource = input.global_state.Cast<AsOfGlobalSourceState>();
	auto &lsource = input.local_state.Cast<AsOfLocalSourceState>();
	auto &gsink = gsource.gsink;

	auto &hash_groups = gsink.hash_groups;
	const auto bin_count = hash_groups.size();

	DataChunk rhs_chunk;
	rhs_chunk.Initialize(Allocator::Get(context.client), gsink.payload_types);
	SelectionVector rsel(STANDARD_VECTOR_SIZE);

	while (chunk.size() == 0) {
		//	Move to the next bin if we are done.
		while (!lsource.scanner || !lsource.scanner->Remaining()) {
			lsource.scanner.reset();
			lsource.hash_group.reset();
			auto hash_bin = gsource.next_bin++;
			if (hash_bin >= bin_count) {
				return SourceResultType::FINISHED;
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
			return SourceResultType::FINISHED;
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
				chunk.data[col_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(chunk.data[col_idx], true);
			}
			for (idx_t col_idx = 0; col_idx < right_projection_map.size(); ++col_idx) {
				const auto rhs_idx = right_projection_map[col_idx];
				chunk.data[left_column_count + col_idx].Slice(rhs_chunk.data[rhs_idx], rsel, result_count);
			}
			chunk.SetCardinality(result_count);
			break;
		}
	}

	return chunk.size() > 0 ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
}

} // namespace duckdb
