#include "duckdb/execution/operator/join/physical_asof_join.hpp"

#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/sort/partition_state.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/join/outer_join_marker.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

PhysicalAsOfJoin::PhysicalAsOfJoin(PhysicalPlan &physical_plan, LogicalComparisonJoin &op, PhysicalOperator &left,
                                   PhysicalOperator &right)
    : PhysicalComparisonJoin(physical_plan, op, PhysicalOperatorType::ASOF_JOIN, std::move(op.conditions), op.join_type,
                             op.estimated_cardinality),
      comparison_type(ExpressionType::INVALID), predicate(std::move(op.predicate)) {

	// Convert the conditions partitions and sorts
	for (auto &cond : conditions) {
		D_ASSERT(cond.left->return_type == cond.right->return_type);
		join_key_types.push_back(cond.left->return_type);

		auto left_cond = cond.left->Copy();
		auto right_cond = cond.right->Copy();
		switch (cond.comparison) {
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHAN:
			null_sensitive.emplace_back(lhs_orders.size());
			lhs_orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, std::move(left_cond));
			rhs_orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, std::move(right_cond));
			comparison_type = cond.comparison;
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_LESSTHAN:
			//	Always put NULLS LAST so they can be ignored.
			null_sensitive.emplace_back(lhs_orders.size());
			lhs_orders.emplace_back(OrderType::DESCENDING, OrderByNullType::NULLS_LAST, std::move(left_cond));
			rhs_orders.emplace_back(OrderType::DESCENDING, OrderByNullType::NULLS_LAST, std::move(right_cond));
			comparison_type = cond.comparison;
			break;
		case ExpressionType::COMPARE_EQUAL:
			null_sensitive.emplace_back(lhs_orders.size());
			DUCKDB_EXPLICIT_FALLTHROUGH;
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			lhs_partitions.emplace_back(std::move(left_cond));
			rhs_partitions.emplace_back(std::move(right_cond));
			break;
		default:
			throw NotImplementedException("Unsupported join condition for ASOF join");
		}
	}
	D_ASSERT(!lhs_orders.empty());
	D_ASSERT(!rhs_orders.empty());

	children.push_back(left);
	children.push_back(right);

	//	Fill out the right projection map.
	right_projection_map = op.right_projection_map;
	if (right_projection_map.empty()) {
		const auto right_count = children[1].get().GetTypes().size();
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
	using PartitionSinkPtr = unique_ptr<PartitionGlobalSinkState>;
	using PartitionMarkers = vector<OuterJoinMarker>;
	using LocalBuffers = vector<unique_ptr<PartitionLocalSinkState>>;

	AsOfGlobalSinkState(ClientContext &context, const PhysicalAsOfJoin &op) {
		// Set up partitions for both sides
		partition_sinks.reserve(2);
		const vector<unique_ptr<BaseStatistics>> partitions_stats;
		auto &lhs = op.children[0].get();
		auto sink = make_uniq<PartitionGlobalSinkState>(context, op.lhs_partitions, op.lhs_orders, lhs.GetTypes(),
		                                                partitions_stats, lhs.estimated_cardinality);
		partition_sinks.emplace_back(std::move(sink));
		auto &rhs = op.children[1].get();
		sink = make_uniq<PartitionGlobalSinkState>(context, op.rhs_partitions, op.rhs_orders, rhs.GetTypes(),
		                                           partitions_stats, rhs.estimated_cardinality);
		partition_sinks.emplace_back(std::move(sink));

		// Record join types
		is_outer.push_back(IsRightOuterJoin(op.join_type));
		is_outer.push_back(IsLeftOuterJoin(op.join_type));

		outer_markers.resize(2);
		local_buffers.resize(2);
	}

	idx_t Count() const {
		return partition_sinks[child]->count;
	}

	PartitionLocalSinkState *RegisterBuffer(ClientContext &context) {
		lock_guard<mutex> guard(lock);
		auto &buffers = local_buffers[child];
		buffers.emplace_back(make_uniq<PartitionLocalSinkState>(context, *partition_sinks[child]));
		return buffers.back().get();
	}

	//! The child that is being materialised (right/1 then left/0)
	size_t child = 1;
	//! The child's partitioning buffer
	vector<PartitionSinkPtr> partition_sinks;
	//! Whether the child is outer
	vector<bool> is_outer;
	//! The child's outer join markers (one per partition)
	vector<PartitionMarkers> outer_markers;
	//! The child's NULL count
	vector<idx_t> has_null {false, false};

	mutex lock;
	vector<LocalBuffers> local_buffers;
};

class AsOfLocalSinkState : public LocalSinkState {
public:
	AsOfLocalSinkState(ClientContext &context, AsOfGlobalSinkState &gsink)
	    : local_partition(context, *gsink.partition_sinks[gsink.child]) {
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
	auto &gsink = sink_state->Cast<AsOfGlobalSinkState>();
	return make_uniq<AsOfLocalSinkState>(context.client, gsink);
}

SinkResultType PhysicalAsOfJoin::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<AsOfLocalSinkState>();

	lstate.Sink(chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalAsOfJoin::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<AsOfLocalSinkState>();
	lstate.Combine();
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalAsOfJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<AsOfGlobalSinkState>();

	// The data is all in so we can synchronise the left partitioning.
	auto &partition_sink = *gstate.partition_sinks[gstate.child];
	if (gstate.child == 1) {
		gstate.partition_sinks[1 - gstate.child]->SyncPartitioning(partition_sink);

		// Find the first group to sort
		if (!partition_sink.HasMergeTasks() && EmptyResultIfRHSIsEmpty()) {
			// Empty input!
			return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
		}
	} else {
		D_ASSERT(gstate.child == 1);
		// Find the first group to sort
		if (!partition_sink.HasMergeTasks()) {
			// Empty input!
			return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
		}
	}

	// Schedule all the sorts for maximum thread utilisation
	auto new_event = make_shared_ptr<PartitionMergeEvent>(partition_sink, pipeline, *this);
	event.InsertEvent(std::move(new_event));

	// Switch sides
	gstate.child = 1 - gstate.child;

	return SinkFinalizeType::READY;
}
#if 0
//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class AsOfGlobalState : public GlobalOperatorState {
public:
	explicit AsOfGlobalState(AsOfGlobalSinkState &gsink) {
		// for FULL/RIGHT OUTER JOIN, initialize right_outers to false for every tuple
		auto &rhs_partition = gsink.rhs_sink;
		auto &right_outers = gsink.right_outers;
		right_outers.reserve(rhs_partition.hash_groups.size());
		for (const auto &hash_group : rhs_partition.hash_groups) {
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
	AsOfLocalState(ClientContext &context, const PhysicalAsOfJoin &op)
	    : context(context), allocator(Allocator::Get(context)), op(op), lhs_executor(context),
	      left_outer(IsLeftOuterJoin(op.join_type)), fetch_next_left(true) {
		lhs_keys.Initialize(allocator, op.join_key_types);
		for (const auto &cond : op.conditions) {
			lhs_executor.AddExpression(*cond.left);
		}

		lhs_payload.Initialize(allocator, op.children[0].get().GetTypes());
		lhs_sel.Initialize();
		left_outer.Initialize(STANDARD_VECTOR_SIZE);

		auto &gsink = op.sink_state->Cast<AsOfGlobalSinkState>();
		lhs_partition_sink = gsink.RegisterBuffer(context);
	}

	bool Sink(DataChunk &input);
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk);

	ClientContext &context;
	Allocator &allocator;
	const PhysicalAsOfJoin &op;

	ExpressionExecutor lhs_executor;
	DataChunk lhs_keys;
	ValidityMask lhs_valid_mask;
	SelectionVector lhs_sel;
	DataChunk lhs_payload;

	OuterJoinMarker left_outer;
	bool fetch_next_left;

	optional_ptr<PartitionLocalSinkState> lhs_partition_sink;
};

bool AsOfLocalState::Sink(DataChunk &input) {
	//	Compute the join keys
	lhs_keys.Reset();
	lhs_executor.Execute(input, lhs_keys);
	lhs_keys.Flatten();

	//	Combine the NULLs
	const auto count = input.size();
	lhs_valid_mask.Reset();
	for (auto col_idx : op.null_sensitive) {
		auto &col = lhs_keys.data[col_idx];
		UnifiedVectorFormat unified;
		col.ToUnifiedFormat(count, unified);
		lhs_valid_mask.Combine(unified.validity, count);
	}

	//	Convert the mask to a selection vector
	//	and mark all the rows that cannot match for early return.
	idx_t lhs_valid = 0;
	const auto entry_count = lhs_valid_mask.EntryCount(count);
	idx_t base_idx = 0;
	left_outer.Reset();
	for (idx_t entry_idx = 0; entry_idx < entry_count;) {
		const auto validity_entry = lhs_valid_mask.GetValidityEntry(entry_idx++);
		const auto next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
		if (ValidityMask::AllValid(validity_entry)) {
			for (; base_idx < next; ++base_idx) {
				lhs_sel.set_index(lhs_valid++, base_idx);
				left_outer.SetMatch(base_idx);
			}
		} else if (ValidityMask::NoneValid(validity_entry)) {
			base_idx = next;
		} else {
			const auto start = base_idx;
			for (; base_idx < next; ++base_idx) {
				if (ValidityMask::RowIsValid(validity_entry, base_idx - start)) {
					lhs_sel.set_index(lhs_valid++, base_idx);
					left_outer.SetMatch(base_idx);
				}
			}
		}
	}

	//	Slice the keys to the ones we can match
	lhs_payload.Reset();
	if (lhs_valid == count) {
		lhs_payload.Reference(input);
		lhs_payload.SetCardinality(input);
	} else {
		lhs_payload.Slice(input, lhs_sel, lhs_valid);
		lhs_payload.SetCardinality(lhs_valid);

		//	Flush the ones that can't match
		fetch_next_left = false;
	}

	lhs_partition_sink->Sink(lhs_payload);

	return false;
}

unique_ptr<OperatorState> PhysicalAsOfJoin::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<AsOfLocalState>(context.client, *this);
}

OperatorResultType AsOfLocalState::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk) {
	input.Verify();
	Sink(input);

	//	If there were any unmatchable rows, return them now so we can forget about them.
	if (!fetch_next_left) {
		fetch_next_left = true;
		left_outer.ConstructLeftJoinResult(input, chunk);
		left_outer.Reset();
	}

	//	Just keep asking for data and buffering it
	return OperatorResultType::NEED_MORE_INPUT;
}
#endif

OperatorResultType PhysicalAsOfJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                     GlobalOperatorState &gstate, OperatorState &lstate_p) const {
#if 0
	auto &gsink = sink_state->Cast<AsOfGlobalSinkState>();
	auto &lstate = lstate_p.Cast<AsOfLocalState>();

	if (gsink.rhs_sink.count == 0) {
		// empty RHS
		if (!EmptyResultIfRHSIsEmpty()) {
			ConstructEmptyJoinResult(join_type, gsink.has_null, input, chunk);
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			return OperatorResultType::FINISHED;
		}
	}

	return lstate.ExecuteInternal(context, input, chunk);
#else
	return OperatorResultType::FINISHED;
#endif
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class AsOfProbeBuffer {
public:
	using Orders = vector<BoundOrderByNode>;

	static bool IsExternal(ClientContext &context) {
		return ClientConfig::GetConfig(context).force_external;
	}

	AsOfProbeBuffer(ClientContext &context, const PhysicalAsOfJoin &op);

public:
	void ResolveJoin(bool *found_matches, idx_t *matches = nullptr);
	bool Scanning() const {
		return lhs_scanner.get();
	}
	void BeginLeftScan(hash_t scan_bin);
	bool NextLeft();
	void EndLeftScan();

	// resolve joins that output max N elements (SEMI, ANTI, MARK)
	void ResolveSimpleJoin(ExecutionContext &context, DataChunk &chunk);
	// resolve joins that can potentially output N*M elements (INNER, LEFT, FULL)
	void ResolveComplexJoin(ExecutionContext &context, DataChunk &chunk);
	//	Chunk may be empty
	void GetData(ExecutionContext &context, DataChunk &chunk);
	bool HasMoreData() const {
		return !fetch_next_left || (lhs_scanner && lhs_scanner->Remaining());
	}

	ClientContext &context;
	Allocator &allocator;
	const PhysicalAsOfJoin &op;
	BufferManager &buffer_manager;
	const bool force_external;
	const idx_t memory_per_thread;
	Orders lhs_orders;

	//	LHS scanning
	SelectionVector lhs_sel;
	optional_ptr<PartitionGlobalHashGroup> left_hash;
	OuterJoinMarker left_outer;
	unique_ptr<SBIterator> left_itr;
	unique_ptr<PayloadScanner> lhs_scanner;
	DataChunk lhs_payload;
	idx_t left_group = 0;
	const bool *lhs_matches = {};

	//	RHS scanning
	optional_ptr<PartitionGlobalHashGroup> right_hash;
	optional_ptr<OuterJoinMarker> right_outer;
	unique_ptr<SBIterator> right_itr;
	unique_ptr<PayloadScanner> rhs_scanner;
	DataChunk rhs_payload;
	idx_t right_group = 0;

	//	Predicate evaluation
	SelectionVector filter_sel;
	ExpressionExecutor filterer;

	idx_t lhs_match_count;
	bool fetch_next_left;
};

AsOfProbeBuffer::AsOfProbeBuffer(ClientContext &context, const PhysicalAsOfJoin &op)
    : context(context), allocator(Allocator::Get(context)), op(op),
      buffer_manager(BufferManager::GetBufferManager(context)), force_external(IsExternal(context)),
      memory_per_thread(op.GetMaxThreadMemory(context)), left_outer(IsLeftOuterJoin(op.join_type)), filterer(context),
      fetch_next_left(true) {
	vector<unique_ptr<BaseStatistics>> partition_stats;
	Orders partitions; // Not used.
	PartitionGlobalSinkState::GenerateOrderings(partitions, lhs_orders, op.lhs_partitions, op.lhs_orders,
	                                            partition_stats);

	//	We sort the row numbers of the incoming block, not the rows
	lhs_payload.Initialize(allocator, op.children[0].get().GetTypes());
	rhs_payload.Initialize(allocator, op.children[1].get().GetTypes());

	lhs_sel.Initialize();
	left_outer.Initialize(STANDARD_VECTOR_SIZE);

	if (op.predicate) {
		filter_sel.Initialize();
		filterer.AddExpression(*op.predicate);
	}
}

void AsOfProbeBuffer::BeginLeftScan(hash_t scan_bin) {
	auto &gsink = op.sink_state->Cast<AsOfGlobalSinkState>();

	auto &lhs_sink = *gsink.partition_sinks[0];
	left_group = lhs_sink.bin_groups[scan_bin];
	lhs_matches = gsink.outer_markers[0][left_group].GetMatches();

	//	Always set right_group too for memory management
	auto &rhs_sink = *gsink.partition_sinks[1];
	if (scan_bin < rhs_sink.bin_groups.size()) {
		right_group = rhs_sink.bin_groups[scan_bin];
	} else {
		right_group = rhs_sink.bin_groups.size();
	}

	if (left_group >= lhs_sink.bin_groups.size()) {
		return;
	}

	auto iterator_comp = ExpressionType::INVALID;
	switch (op.comparison_type) {
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		iterator_comp = ExpressionType::COMPARE_LESSTHANOREQUALTO;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		iterator_comp = ExpressionType::COMPARE_LESSTHAN;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		iterator_comp = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		iterator_comp = ExpressionType::COMPARE_GREATERTHAN;
		break;
	default:
		throw NotImplementedException("Unsupported comparison type for ASOF join");
	}

	left_hash = lhs_sink.hash_groups[left_group].get();
	auto &left_sort = *(left_hash->global_sort);
	if (left_sort.sorted_blocks.empty()) {
		return;
	}
	lhs_scanner = make_uniq<PayloadScanner>(left_sort, false);
	left_itr = make_uniq<SBIterator>(left_sort, iterator_comp);

	// We are only probing the corresponding right side bin, which may be empty
	// If they are empty, we leave the iterator as null so we can emit left matches
	if (right_group < rhs_sink.bin_groups.size()) {
		right_hash = rhs_sink.hash_groups[right_group].get();
		right_outer = gsink.outer_markers[1].data() + right_group;
		auto &right_sort = *(right_hash->global_sort);
		right_itr = make_uniq<SBIterator>(right_sort, iterator_comp);
		rhs_scanner = make_uniq<PayloadScanner>(right_sort, false);
	}
}

bool AsOfProbeBuffer::NextLeft() {
	if (!HasMoreData()) {
		return false;
	}

	//	Scan the next sorted chunk
	lhs_payload.Reset();
	left_itr->SetIndex(lhs_scanner->Scanned());
	lhs_scanner->Scan(lhs_payload);

	return true;
}

void AsOfProbeBuffer::EndLeftScan() {
	auto &gsink = op.sink_state->Cast<AsOfGlobalSinkState>();

	right_hash = nullptr;
	right_itr.reset();
	rhs_scanner.reset();
	right_outer = nullptr;

	auto &rhs_sink = *gsink.partition_sinks[1];
	if (!gsink.is_outer[1] && right_group < rhs_sink.bin_groups.size()) {
		rhs_sink.hash_groups[right_group].reset();
	}

	left_hash = nullptr;
	left_itr.reset();
	lhs_scanner.reset();

	auto &lhs_sink = *gsink.partition_sinks[0];
	if (left_group < lhs_sink.bin_groups.size()) {
		lhs_sink.hash_groups[left_group].reset();
	}
}

void AsOfProbeBuffer::ResolveJoin(bool *found_match, idx_t *matches) {
	// If there was no right partition, there are no matches
	lhs_match_count = 0;
	if (!right_itr) {
		return;
	}

	const auto count = lhs_payload.size();
	const auto left_base = left_itr->GetIndex();
	//	Searching for right <= left
	for (idx_t i = 0; i < count; ++i) {
		left_itr->SetIndex(left_base + i);

		//	If right > left, then there is no match
		if (!right_itr->Compare(*left_itr)) {
			continue;
		}

		// Exponential search forward for a non-matching value using radix iterators
		// (We use exponential search to avoid thrashing the block manager on large probes)
		idx_t bound = 1;
		idx_t begin = right_itr->GetIndex();
		right_itr->SetIndex(begin + bound);
		while (right_itr->GetIndex() < right_hash->count) {
			if (right_itr->Compare(*left_itr)) {
				//	If right <= left, jump ahead
				bound *= 2;
				right_itr->SetIndex(begin + bound);
			} else {
				break;
			}
		}

		//	Binary search for the first non-matching value using radix iterators
		//	The previous value (which we know exists) is the match
		auto first = begin + bound / 2;
		auto last = MinValue<idx_t>(begin + bound, right_hash->count);
		while (first < last) {
			const auto mid = first + (last - first) / 2;
			right_itr->SetIndex(mid);
			if (right_itr->Compare(*left_itr)) {
				//	If right <= left, new lower bound
				first = mid + 1;
			} else {
				last = mid;
			}
		}
		right_itr->SetIndex(--first);

		//	Check partitions for strict equality
		if (right_hash->ComparePartitions(*left_itr, *right_itr)) {
			continue;
		}

		// Emit match data
		if (found_match) {
			found_match[i] = true;
		}
		if (matches) {
			matches[i] = first;
		}
		lhs_sel.set_index(lhs_match_count++, i);
	}
}

void AsOfProbeBuffer::ResolveSimpleJoin(ExecutionContext &context, DataChunk &chunk) {
	// perform the actual join
	bool found_match[STANDARD_VECTOR_SIZE] = {false};
	ResolveJoin(found_match);

	// now construct the result based on the join result
	switch (op.join_type) {
	case JoinType::SEMI:
		PhysicalJoin::ConstructSemiJoinResult(lhs_payload, chunk, found_match);
		break;
	case JoinType::ANTI:
		PhysicalJoin::ConstructAntiJoinResult(lhs_payload, chunk, found_match);
		break;
	default:
		throw NotImplementedException("Unimplemented join type for AsOf join");
	}
}

void AsOfProbeBuffer::ResolveComplexJoin(ExecutionContext &context, DataChunk &chunk) {
	// perform the actual join
	idx_t matches[STANDARD_VECTOR_SIZE];
	ResolveJoin(nullptr, matches);

	for (idx_t i = 0; i < lhs_match_count; ++i) {
		const auto idx = lhs_sel[i];
		const auto match_pos = matches[idx];
		// Skip to the range containing the match
		while (match_pos >= rhs_scanner->Scanned()) {
			rhs_payload.Reset();
			rhs_scanner->Scan(rhs_payload);
		}
		// Append the individual values
		// TODO: Batch the copies
		const auto source_offset = match_pos - (rhs_scanner->Scanned() - rhs_payload.size());
		for (column_t col_idx = 0; col_idx < op.right_projection_map.size(); ++col_idx) {
			const auto rhs_idx = op.right_projection_map[col_idx];
			auto &source = rhs_payload.data[rhs_idx];
			auto &target = chunk.data[lhs_payload.ColumnCount() + col_idx];
			VectorOperations::Copy(source, target, source_offset + 1, source_offset, i);
		}
	}

	//	Slice the left payload into the result
	for (column_t i = 0; i < lhs_payload.ColumnCount(); ++i) {
		chunk.data[i].Slice(lhs_payload.data[i], lhs_sel, lhs_match_count);
	}
	chunk.SetCardinality(lhs_match_count);
	auto match_sel = &lhs_sel;
	if (filterer.expressions.size() == 1) {
		lhs_match_count = filterer.SelectExpression(chunk, filter_sel);
		chunk.Slice(filter_sel, lhs_match_count);
		match_sel = &filter_sel;
	}

	//	Update the match masks for the rows we ended up with
	left_outer.Reset();
	for (idx_t i = 0; i < lhs_match_count; ++i) {
		const auto idx = match_sel->get_index(i);
		left_outer.SetMatch(idx);
		const auto first = matches[idx];
		right_outer->SetMatch(first);
	}

	//	If we are doing a left join, come back for the NULLs
	fetch_next_left = !left_outer.Enabled();
}

void AsOfProbeBuffer::GetData(ExecutionContext &context, DataChunk &chunk) {
	//	Handle dangling left join results from current chunk
	if (!fetch_next_left) {
		fetch_next_left = true;
		if (left_outer.Enabled()) {
			// left join: before we move to the next chunk, see if we need to output any vectors that didn't
			// have a match found
			left_outer.ConstructLeftJoinResult(lhs_payload, chunk);
			left_outer.Reset();
		}
		return;
	}

	//	Stop if there is no more data
	if (!NextLeft()) {
		return;
	}

	switch (op.join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
		// simple joins can have max STANDARD_VECTOR_SIZE matches per chunk
		ResolveSimpleJoin(context, chunk);
		break;
	case JoinType::LEFT:
	case JoinType::INNER:
	case JoinType::RIGHT:
	case JoinType::OUTER:
		ResolveComplexJoin(context, chunk);
		break;
	default:
		throw NotImplementedException("Unimplemented type for as-of join!");
	}
}

class AsOfGlobalSourceState : public GlobalSourceState {
public:
	explicit AsOfGlobalSourceState(AsOfGlobalSinkState &gsink_p)
	    : gsink(gsink_p), next_left(0), flushed(0), next_right(0) {

		// for LEFT/FULL/RIGHT OUTER JOIN, initialize to false for every tuple
		for (idx_t child = 0; child < gsink.partition_sinks.size(); ++child) {
			auto &partition_sink = *gsink.partition_sinks[child];
			auto &outer_markers = gsink.outer_markers[child];
			outer_markers.reserve(partition_sink.hash_groups.size());
			for (const auto &hash_group : partition_sink.hash_groups) {
				outer_markers.emplace_back(OuterJoinMarker(gsink.is_outer[child]));
				outer_markers.back().Initialize(hash_group->count);
			}
		}
	}

	AsOfGlobalSinkState &gsink;
	//! The next buffer to flush
	atomic<size_t> next_left;
	//! The number of flushed buffers
	atomic<size_t> flushed;
	//! The right outer output read position.
	atomic<idx_t> next_right;

public:
	idx_t MaxThreads() override {
		return gsink.local_buffers[0].size();
	}
};

unique_ptr<GlobalSourceState> PhysicalAsOfJoin::GetGlobalSourceState(ClientContext &context) const {
	auto &gsink = sink_state->Cast<AsOfGlobalSinkState>();
	return make_uniq<AsOfGlobalSourceState>(gsink);
}

class AsOfLocalSourceState : public LocalSourceState {
public:
	using HashGroupPtr = unique_ptr<PartitionGlobalHashGroup>;

	AsOfLocalSourceState(ExecutionContext &context, AsOfGlobalSourceState &gsource, const PhysicalAsOfJoin &op);

	idx_t BeginRightScan(const idx_t hash_bin);

	AsOfGlobalSourceState &gsource;
	ExecutionContext &context;

	//! The left side partition being probed
	AsOfProbeBuffer probe_buffer;

	//! The read partition
	idx_t hash_bin;
	HashGroupPtr hash_group;
	//! The read cursor
	unique_ptr<PayloadScanner> scanner;
	//! Pointer to the right marker
	const bool *rhs_matches = {};
};

AsOfLocalSourceState::AsOfLocalSourceState(ExecutionContext &context, AsOfGlobalSourceState &gsource,
                                           const PhysicalAsOfJoin &op)
    : gsource(gsource), context(context), probe_buffer(context.client, op) {
}

idx_t AsOfLocalSourceState::BeginRightScan(const idx_t hash_bin_p) {
	hash_bin = hash_bin_p;

	auto &rhs_sink = *gsource.gsink.partition_sinks[1];
	hash_group = std::move(rhs_sink.hash_groups[hash_bin]);
	if (hash_group->global_sort->sorted_blocks.empty()) {
		return 0;
	}
	scanner = make_uniq<PayloadScanner>(*hash_group->global_sort);

	rhs_matches = gsource.gsink.outer_markers[1][hash_bin].GetMatches();

	return scanner->Remaining();
}

unique_ptr<LocalSourceState> PhysicalAsOfJoin::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	auto &gsource = gstate.Cast<AsOfGlobalSourceState>();
	return make_uniq<AsOfLocalSourceState>(context, gsource, *this);
}

SourceResultType PhysicalAsOfJoin::GetData(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSourceInput &input) const {
	auto &gsource = input.global_state.Cast<AsOfGlobalSourceState>();
	auto &lsource = input.local_state.Cast<AsOfLocalSourceState>();
	auto &rhs_sink = *gsource.gsink.partition_sinks[1];
	auto &client = context.client;

	//	Step 1: Join the partitions
	auto &lhs_sink = *gsource.gsink.partition_sinks[0];
	const auto left_bins = lhs_sink.grouping_data ? lhs_sink.grouping_data->GetPartitions().size() : 1;
	while (gsource.flushed < left_bins) {
		//	Make sure we have something to flush
		if (!lsource.probe_buffer.Scanning()) {
			const auto left_bin = gsource.next_left++;
			if (left_bin < left_bins) {
				//	More to flush
				lsource.probe_buffer.BeginLeftScan(left_bin);
			} else if (!IsRightOuterJoin(join_type) || client.interrupted) {
				return SourceResultType::FINISHED;
			} else {
				//	Wait for all threads to finish
				//	TODO: How to implement a spin wait correctly?
				//	Returning BLOCKED seems to hang the system.
				TaskScheduler::GetScheduler(client).YieldThread();
				continue;
			}
		}

		lsource.probe_buffer.GetData(context, chunk);
		if (chunk.size()) {
			return SourceResultType::HAVE_MORE_OUTPUT;
		} else if (lsource.probe_buffer.HasMoreData()) {
			//	Join the next partition
			continue;
		} else {
			lsource.probe_buffer.EndLeftScan();
			gsource.flushed++;
		}
	}

	//	Step 2: Emit left join matches

	//	Step 3: Emit right join matches
	if (!IsRightOuterJoin(join_type)) {
		return SourceResultType::FINISHED;
	}

	auto &hash_groups = rhs_sink.hash_groups;
	const auto right_groups = hash_groups.size();

	DataChunk rhs_chunk;
	rhs_chunk.Initialize(Allocator::Get(context.client), rhs_sink.payload_types);
	SelectionVector rsel(STANDARD_VECTOR_SIZE);

	while (chunk.size() == 0) {
		//	Move to the next bin if we are done.
		while (!lsource.scanner || !lsource.scanner->Remaining()) {
			lsource.scanner.reset();
			lsource.hash_group.reset();
			auto hash_bin = gsource.next_right++;
			if (hash_bin >= right_groups) {
				return SourceResultType::FINISHED;
			}

			for (; hash_bin < hash_groups.size(); hash_bin = gsource.next_right++) {
				if (hash_groups[hash_bin]) {
					break;
				}
			}
			lsource.BeginRightScan(hash_bin);
		}
		const auto rhs_position = lsource.scanner->Scanned();
		lsource.scanner->Scan(rhs_chunk);

		const auto count = rhs_chunk.size();
		if (count == 0) {
			return SourceResultType::FINISHED;
		}

		// figure out which tuples didn't find a match in the RHS
		auto rhs_matches = lsource.rhs_matches;
		idx_t result_count = 0;
		for (idx_t i = 0; i < count; i++) {
			if (!rhs_matches[rhs_position + i]) {
				rsel.set_index(result_count++, i);
			}
		}

		if (result_count > 0) {
			// if there were any tuples that didn't find a match, output them
			const idx_t left_column_count = children[0].get().GetTypes().size();
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

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalAsOfJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	D_ASSERT(children.size() == 2);
	if (meta_pipeline.HasRecursiveCTE()) {
		throw NotImplementedException("AsOf joins are not supported in recursive CTEs yet");
	}

	// becomes a source after both children fully sink their data
	meta_pipeline.GetState().SetPipelineSource(current, *this);

	// Create one child meta pipeline that will hold the LHS and RHS pipelines
	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);

	// Build out RHS first because that is the order the join planner expects.
	auto rhs_pipeline = child_meta_pipeline.GetBasePipeline();
	children[1].get().BuildPipelines(*rhs_pipeline, child_meta_pipeline);

	// Build out LHS
	auto &lhs_pipeline = child_meta_pipeline.CreatePipeline();
	children[0].get().BuildPipelines(lhs_pipeline, child_meta_pipeline);

	// Despite having the same sink, LHS and everything created after it need their own (same) PipelineFinishEvent
	child_meta_pipeline.AddFinishEvent(lhs_pipeline);
}

} // namespace duckdb
