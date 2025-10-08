#include "duckdb/execution/operator/join/physical_asof_join.hpp"

#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/sorting/hashed_sort.hpp"
#include "duckdb/common/sorting/sort_key.hpp"
#include "duckdb/common/sorting/sorted_run.hpp"
#include "duckdb/common/types/row/block_iterator.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/join/outer_join_marker.hpp"
#include "duckdb/execution/operator/join/physical_range_join.hpp"
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
	using HashedSortPtr = unique_ptr<HashedSort>;
	using HashedSinkPtr = unique_ptr<GlobalSinkState>;
	using PartitionMarkers = vector<OuterJoinMarker>;
	using HashGroupPtr = unique_ptr<SortedRun>;
	using HashGroups = vector<HashGroupPtr>;

	AsOfGlobalSinkState(ClientContext &client, const PhysicalAsOfJoin &op) : is_outer(IsRightOuterJoin(op.join_type)) {
		// Set up partitions for both sides
		hashed_sorts.reserve(2);
		hashed_sinks.reserve(2);
		const vector<unique_ptr<BaseStatistics>> partitions_stats;
		auto &lhs = op.children[0].get();
		auto sort = make_uniq<HashedSort>(client, op.lhs_partitions, op.lhs_orders, lhs.GetTypes(), partitions_stats,
		                                  lhs.estimated_cardinality, true);
		hashed_sinks.emplace_back(sort->GetGlobalSinkState(client));
		hashed_sorts.emplace_back(std::move(sort));

		auto &rhs = op.children[1].get();
		sort = make_uniq<HashedSort>(client, op.rhs_partitions, op.rhs_orders, rhs.GetTypes(), partitions_stats,
		                             rhs.estimated_cardinality, true);
		hashed_sinks.emplace_back(sort->GetGlobalSinkState(client));
		hashed_sorts.emplace_back(std::move(sort));

		hash_groups.resize(2);
	}

	//! The child that is being materialised (right/1 then left/0)
	size_t child = 1;
	//! The child's partitioning description
	vector<HashedSortPtr> hashed_sorts;
	//! The child's partitioning buffer
	vector<HashedSinkPtr> hashed_sinks;
	//! The child's hash groups
	vector<HashGroups> hash_groups;
	//! Whether the right side is outer
	const bool is_outer;
	//! The right outer join markers (one per partition)
	vector<OuterJoinMarker> right_outers;
};

class AsOfLocalSinkState : public LocalSinkState {
public:
	AsOfLocalSinkState(ExecutionContext &context, AsOfGlobalSinkState &gsink) {
		auto &hashed_sort = *gsink.hashed_sorts[gsink.child];
		local_partition = hashed_sort.GetLocalSinkState(context);
	}

	unique_ptr<LocalSinkState> local_partition;
};

unique_ptr<GlobalSinkState> PhysicalAsOfJoin::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<AsOfGlobalSinkState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalAsOfJoin::GetLocalSinkState(ExecutionContext &context) const {
	auto &gsink = sink_state->Cast<AsOfGlobalSinkState>();
	return make_uniq<AsOfLocalSinkState>(context, gsink);
}

SinkResultType PhysicalAsOfJoin::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &sink) const {
	auto &gstate = sink.global_state.Cast<AsOfGlobalSinkState>();
	auto &lstate = sink.local_state.Cast<AsOfLocalSinkState>();

	auto &hashed_sort = *gstate.hashed_sorts[gstate.child];
	auto &gsink = *gstate.hashed_sinks[gstate.child];
	auto &lsink = *lstate.local_partition;

	OperatorSinkInput hsink {gsink, lsink, sink.interrupt_state};
	return hashed_sort.Sink(context, chunk, hsink);
}

SinkCombineResultType PhysicalAsOfJoin::Combine(ExecutionContext &context, OperatorSinkCombineInput &combine) const {
	auto &gstate = combine.global_state.Cast<AsOfGlobalSinkState>();
	auto &lstate = combine.local_state.Cast<AsOfLocalSinkState>();

	auto &hashed_sort = *gstate.hashed_sorts[gstate.child];
	auto &gsink = *gstate.hashed_sinks[gstate.child];
	auto &lsink = *lstate.local_partition;

	OperatorSinkCombineInput hcombine {gsink, lsink, combine.interrupt_state};
	return hashed_sort.Combine(context, hcombine);
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalAsOfJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &client,
                                            OperatorSinkFinalizeInput &finalize) const {
	auto &gstate = finalize.global_state.Cast<AsOfGlobalSinkState>();

	// The data is all in so we can synchronise the left partitioning.
	auto &hashed_sort = *gstate.hashed_sorts[gstate.child];
	auto &hashed_sink = *gstate.hashed_sinks[gstate.child];
	OperatorSinkFinalizeInput hfinalize {hashed_sink, finalize.interrupt_state};
	if (gstate.child == 1) {
		auto &lhs_groups = *gstate.hashed_sinks[1 - gstate.child];
		auto &rhs_groups = hashed_sink;
		hashed_sort.Synchronize(rhs_groups, lhs_groups);

		auto result = hashed_sort.Finalize(client, hfinalize);
		if (result != SinkFinalizeType::READY && EmptyResultIfRHSIsEmpty()) {
			// Empty input!
			gstate.child = 1 - gstate.child;
			return result;
		}
	} else {
		hashed_sort.Finalize(client, hfinalize);
	}

	// Switch sides
	gstate.child = 1 - gstate.child;

	// Schedule all the sorts for maximum thread utilisation
	return hashed_sort.MaterializeSortedRuns(pipeline, event, *this, hfinalize);
}

OperatorResultType PhysicalAsOfJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                     GlobalOperatorState &gstate, OperatorState &lstate_p) const {
	return OperatorResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class AsOfPayloadScanner {
public:
	using Types = vector<LogicalType>;
	using Columns = vector<column_t>;

	AsOfPayloadScanner(const SortedRun &sorted_run, const HashedSort &hashed_sort);
	idx_t Base() const {
		return base;
	}
	idx_t Scanned() const {
		return scanned;
	}
	idx_t Remaining() const {
		return count - scanned;
	}
	bool Scan(DataChunk &chunk) {
		//  Free the previous blocks
		block_state.SetKeepPinned(true);
		block_state.SetPinPayload(true);

		base = scanned;
		const auto result = (this->*scan_func)();
		chunk.ReferenceColumns(scan_chunk, scan_ids);
		scanned += scan_chunk.size();
		++chunk_idx;
		return result;
	}

private:
	template <SortKeyType SORT_KEY_TYPE>
	bool TemplatedScan() {
		using SORT_KEY = SortKey<SORT_KEY_TYPE>;
		using BLOCK_ITERATOR = block_iterator_t<ExternalBlockIteratorState, SORT_KEY>;
		BLOCK_ITERATOR itr(block_state, chunk_idx, 0);

		const auto sort_keys = FlatVector::GetData<SORT_KEY *>(sort_key_pointers);
		const auto result_count = MinValue<idx_t>(Remaining(), STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < result_count; ++i) {
			const auto idx = block_state.GetIndex(chunk_idx, i);
			sort_keys[i] = &itr[idx];
		}

		// Scan
		scan_chunk.Reset();
		scan_state.Scan(sorted_run, sort_key_pointers, result_count, scan_chunk);
		return scan_chunk.size() > 0;
	}

	//	Only figure out the scan function once.
	using scan_t = bool (duckdb::AsOfPayloadScanner::*)();
	scan_t scan_func;

	const SortedRun &sorted_run;
	ExternalBlockIteratorState block_state;
	Vector sort_key_pointers = Vector(LogicalType::POINTER);
	SortedRunScanState scan_state;
	const Columns scan_ids;
	DataChunk scan_chunk;
	const idx_t count;
	idx_t base = 0;
	idx_t scanned = 0;
	idx_t chunk_idx = 0;
};

AsOfPayloadScanner::AsOfPayloadScanner(const SortedRun &sorted_run, const HashedSort &hashed_sort)
    : sorted_run(sorted_run), block_state(*sorted_run.key_data, sorted_run.payload_data.get()),
      scan_state(sorted_run.context, sorted_run.sort), scan_ids(hashed_sort.scan_ids), count(sorted_run.Count()) {

	scan_chunk.Initialize(sorted_run.context, hashed_sort.payload_types);
	const auto sort_key_type = sorted_run.key_data->GetLayout().GetSortKeyType();
	switch (sort_key_type) {
	case SortKeyType::NO_PAYLOAD_FIXED_8:
		scan_func = &AsOfPayloadScanner::TemplatedScan<SortKeyType::NO_PAYLOAD_FIXED_8>;
		break;
	case SortKeyType::NO_PAYLOAD_FIXED_16:
		scan_func = &AsOfPayloadScanner::TemplatedScan<SortKeyType::NO_PAYLOAD_FIXED_16>;
		break;
	case SortKeyType::NO_PAYLOAD_FIXED_24:
		scan_func = &AsOfPayloadScanner::TemplatedScan<SortKeyType::NO_PAYLOAD_FIXED_24>;
		break;
	case SortKeyType::NO_PAYLOAD_FIXED_32:
		scan_func = &AsOfPayloadScanner::TemplatedScan<SortKeyType::NO_PAYLOAD_FIXED_32>;
		break;
	case SortKeyType::NO_PAYLOAD_VARIABLE_32:
		scan_func = &AsOfPayloadScanner::TemplatedScan<SortKeyType::NO_PAYLOAD_VARIABLE_32>;
		break;
	case SortKeyType::PAYLOAD_FIXED_16:
		scan_func = &AsOfPayloadScanner::TemplatedScan<SortKeyType::PAYLOAD_FIXED_16>;
		break;
	case SortKeyType::PAYLOAD_FIXED_24:
		scan_func = &AsOfPayloadScanner::TemplatedScan<SortKeyType::PAYLOAD_FIXED_24>;
		break;
	case SortKeyType::PAYLOAD_FIXED_32:
		scan_func = &AsOfPayloadScanner::TemplatedScan<SortKeyType::PAYLOAD_FIXED_32>;
		break;
	case SortKeyType::PAYLOAD_VARIABLE_32:
		scan_func = &AsOfPayloadScanner::TemplatedScan<SortKeyType::PAYLOAD_VARIABLE_32>;
		break;
	default:
		throw NotImplementedException("AsOfPayloadScanner for %s", EnumUtil::ToString(sort_key_type));
	}
}

class AsOfProbeBuffer {
public:
	using Orders = vector<BoundOrderByNode>;

	AsOfProbeBuffer(ClientContext &context, const PhysicalAsOfJoin &op);

public:
	//	Comparison utilities
	static bool IsStrictComparison(ExpressionType comparison) {
		switch (comparison) {
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
			return true;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return false;
		default:
			throw NotImplementedException("Unsupported comparison type for ASOF join");
		}
	}

	//! Is left cmp right?
	template <typename T>
	static inline bool Compare(const T &lhs, const T &rhs, const bool strict) {
		const bool less_than = lhs < rhs;
		if (!less_than && !strict) {
			return !(rhs < lhs);
		}
		return less_than;
	}

	template <SortKeyType SORT_KEY_TYPE>
	void ResolveJoin(bool *found_matches, idx_t *matches);

	using resolve_join_t = void (duckdb::AsOfProbeBuffer::*)(bool *, idx_t *);
	resolve_join_t resolve_join_func;

	bool Scanning() const {
		return lhs_scanner.get();
	}
	void BeginLeftScan(hash_t scan_bin);
	bool NextLeft();
	void EndLeftScan();

	//! Create a new iterator for the sorted run
	static unique_ptr<ExternalBlockIteratorState> CreateIteratorState(SortedRun &sorted) {
		auto state = make_uniq<ExternalBlockIteratorState>(*sorted.key_data, sorted.payload_data.get());

		// Unless we do this, we will only get values from the first chunk
		Repin(*state);

		return state;
	}
	//! Reset the pins for an iterator so we release memory in a timely manner
	static void Repin(ExternalBlockIteratorState &iter) {
		//	Don't pin the payload because we are not using it here.
		iter.SetKeepPinned(true);
	}
	// resolve joins that output max N elements (SEMI, ANTI, MARK)
	void ResolveSimpleJoin(ExecutionContext &context, DataChunk &chunk);
	// resolve joins that can potentially output N*M elements (INNER, LEFT, FULL)
	void ResolveComplexJoin(ExecutionContext &context, DataChunk &chunk);
	//	Chunk may be empty
	void GetData(ExecutionContext &context, DataChunk &chunk);
	bool HasMoreData() const {
		return !fetch_next_left || (lhs_scanner && lhs_scanner->Remaining());
	}

	ClientContext &client;
	const PhysicalAsOfJoin &op;
	//! Is the inequality strict?
	const bool strict;

	//	LHS scanning
	SelectionVector lhs_scan_sel;
	optional_ptr<SortedRun> left_group;
	OuterJoinMarker left_outer;
	unique_ptr<ExternalBlockIteratorState> left_itr;
	unique_ptr<AsOfPayloadScanner> lhs_scanner;
	DataChunk lhs_scanned;
	DataChunk lhs_payload;
	ExpressionExecutor lhs_executor;
	DataChunk lhs_keys;
	ValidityMask lhs_valid_mask;
	idx_t left_bin = 0;
	SelectionVector lhs_match_sel;

	//	RHS scanning
	optional_ptr<SortedRun> right_group;
	optional_ptr<OuterJoinMarker> right_outer;
	unique_ptr<ExternalBlockIteratorState> right_itr;
	idx_t right_pos; // ExternalBlockIteratorState doesn't know this...
	unique_ptr<AsOfPayloadScanner> rhs_scanner;
	DataChunk rhs_payload;
	ExpressionExecutor rhs_executor;
	DataChunk rhs_input;
	DataChunk rhs_keys;
	idx_t right_bin = 0;

	//	Predicate evaluation
	SelectionVector tail_sel;
	SelectionVector filter_sel;
	ExpressionExecutor filterer;

	idx_t lhs_match_count;
	bool fetch_next_left;
};

AsOfProbeBuffer::AsOfProbeBuffer(ClientContext &client, const PhysicalAsOfJoin &op)
    : client(client), op(op), strict(IsStrictComparison(op.comparison_type)), left_outer(IsLeftOuterJoin(op.join_type)),
      lhs_executor(client), rhs_executor(client), filterer(client), fetch_next_left(true) {

	lhs_keys.Initialize(client, op.join_key_types);
	for (const auto &cond : op.conditions) {
		lhs_executor.AddExpression(*cond.left);
	}

	lhs_scanned.Initialize(client, op.children[0].get().GetTypes());
	lhs_payload.Initialize(client, op.children[0].get().GetTypes());
	rhs_payload.Initialize(client, op.children[1].get().GetTypes());
	rhs_input.Initialize(client, op.children[1].get().GetTypes());

	lhs_scan_sel.Initialize();
	lhs_match_sel.Initialize();
	left_outer.Initialize(STANDARD_VECTOR_SIZE);

	//	If we have equality predicates, we need some more buffers.
	if (op.conditions.size() > 1) {
		tail_sel.Initialize();
		rhs_keys.Initialize(client, op.join_key_types);
		for (const auto &cond : op.conditions) {
			rhs_executor.AddExpression(*cond.right);
		}
	}

	if (op.predicate) {
		filter_sel.Initialize();
		filterer.AddExpression(*op.predicate);
	}
}

void AsOfProbeBuffer::BeginLeftScan(hash_t scan_bin) {
	auto &gsink = op.sink_state->Cast<AsOfGlobalSinkState>();

	//	Always set right_bin too for memory management
	auto &rhs_groups = gsink.hash_groups[1];
	if (scan_bin < rhs_groups.size()) {
		right_bin = scan_bin;
	} else {
		right_bin = rhs_groups.size();
	}

	auto &lhs_groups = gsink.hash_groups[0];
	if (scan_bin < lhs_groups.size()) {
		left_bin = scan_bin;
	} else {
		left_bin = lhs_groups.size();
	}

	if (left_bin >= lhs_groups.size()) {
		return;
	}

	left_group = lhs_groups[left_bin].get();
	if (!left_group || !left_group->Count()) {
		return;
	}

	//	Set up function pointer for sort type
	const auto sort_key_type = left_group->key_data->GetLayout().GetSortKeyType();
	switch (sort_key_type) {
	case SortKeyType::NO_PAYLOAD_FIXED_8:
		resolve_join_func = &AsOfProbeBuffer::ResolveJoin<SortKeyType::NO_PAYLOAD_FIXED_8>;
		break;
	case SortKeyType::NO_PAYLOAD_FIXED_16:
		resolve_join_func = &AsOfProbeBuffer::ResolveJoin<SortKeyType::NO_PAYLOAD_FIXED_16>;
		break;
	case SortKeyType::NO_PAYLOAD_FIXED_24:
		resolve_join_func = &AsOfProbeBuffer::ResolveJoin<SortKeyType::NO_PAYLOAD_FIXED_24>;
		break;
	case SortKeyType::NO_PAYLOAD_FIXED_32:
		resolve_join_func = &AsOfProbeBuffer::ResolveJoin<SortKeyType::NO_PAYLOAD_FIXED_32>;
		break;
	case SortKeyType::NO_PAYLOAD_VARIABLE_32:
		resolve_join_func = &AsOfProbeBuffer::ResolveJoin<SortKeyType::NO_PAYLOAD_VARIABLE_32>;
		break;
	case SortKeyType::PAYLOAD_FIXED_16:
		resolve_join_func = &AsOfProbeBuffer::ResolveJoin<SortKeyType::PAYLOAD_FIXED_16>;
		break;
	case SortKeyType::PAYLOAD_FIXED_24:
		resolve_join_func = &AsOfProbeBuffer::ResolveJoin<SortKeyType::PAYLOAD_FIXED_24>;
		break;
	case SortKeyType::PAYLOAD_FIXED_32:
		resolve_join_func = &AsOfProbeBuffer::ResolveJoin<SortKeyType::PAYLOAD_FIXED_32>;
		break;
	case SortKeyType::PAYLOAD_VARIABLE_32:
		resolve_join_func = &AsOfProbeBuffer::ResolveJoin<SortKeyType::PAYLOAD_VARIABLE_32>;
		break;
	default:
		throw NotImplementedException("Unsupported comparison type for ASOF join");
	}

	lhs_scanner = make_uniq<AsOfPayloadScanner>(*left_group, *gsink.hashed_sorts[0]);
	left_itr = CreateIteratorState(*left_group);

	// We are only probing the corresponding right side bin, which may be empty
	// If it is empty, we leave the iterator as null so we can emit left matches
	right_pos = 0;
	if (right_bin < rhs_groups.size()) {
		right_group = rhs_groups[right_bin].get();
		right_outer = gsink.right_outers.data() + right_bin;
		if (right_group && right_group->Count()) {
			right_itr = CreateIteratorState(*right_group);
			rhs_scanner = make_uniq<AsOfPayloadScanner>(*right_group, *gsink.hashed_sorts[1]);
		}
	}
}

bool AsOfProbeBuffer::NextLeft() {
	//	Scan the next sorted chunk
	lhs_scanned.Reset();
	if (!lhs_scanner || !lhs_scanner->Scan(lhs_scanned)) {
		return false;
	}

	//	Compute the join keys
	lhs_keys.Reset();
	lhs_executor.Execute(lhs_scanned, lhs_keys);
	lhs_keys.Flatten();

	//	Combine the NULLs
	const auto count = lhs_scanned.size();
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
	for (idx_t entry_idx = 0; entry_idx < entry_count;) {
		const auto validity_entry = lhs_valid_mask.GetValidityEntry(entry_idx++);
		const auto next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
		if (ValidityMask::AllValid(validity_entry)) {
			for (; base_idx < next; ++base_idx) {
				lhs_scan_sel.set_index(lhs_valid++, base_idx);
			}
		} else if (ValidityMask::NoneValid(validity_entry)) {
			base_idx = next;
		} else {
			const auto start = base_idx;
			for (; base_idx < next; ++base_idx) {
				if (ValidityMask::RowIsValid(validity_entry, base_idx - start)) {
					lhs_scan_sel.set_index(lhs_valid++, base_idx);
				}
			}
		}
	}

	//	Slice the keys to the ones we can match
	if (lhs_valid < count) {
		lhs_payload.Slice(lhs_scanned, lhs_scan_sel, lhs_valid);
	} else {
		lhs_payload.Reference(lhs_scanned);
	}

	return true;
}

void AsOfProbeBuffer::EndLeftScan() {
	auto &gsink = op.sink_state->Cast<AsOfGlobalSinkState>();

	right_group = nullptr;
	right_itr.reset();
	rhs_scanner.reset();
	right_outer = nullptr;

	auto &rhs_groups = gsink.hash_groups[1];
	if (!gsink.is_outer && right_bin < rhs_groups.size()) {
		rhs_groups[right_bin].reset();
	}

	left_group = nullptr;
	left_itr.reset();
	lhs_scanner.reset();

	auto &lhs_groups = gsink.hash_groups[0];
	if (left_bin < lhs_groups.size()) {
		lhs_groups[left_bin].reset();
	}
}

template <SortKeyType SORT_KEY_TYPE>
void AsOfProbeBuffer::ResolveJoin(bool *found_match, idx_t *matches) {
	using SORT_KEY = SortKey<SORT_KEY_TYPE>;
	using BLOCKS_ITERATOR = block_iterator_t<ExternalBlockIteratorState, SORT_KEY>;

	// If there was no right partition, there are no matches
	lhs_match_count = 0;
	if (!right_itr) {
		return;
	}

	Repin(*left_itr);
	BLOCKS_ITERATOR left_key(*left_itr);

	Repin(*right_itr);
	BLOCKS_ITERATOR right_key(*right_itr);

	const auto count = lhs_payload.size();
	const auto left_base = lhs_scanner->Base();
	auto lhs_sel = (count == lhs_scanned.size()) ? FlatVector::IncrementalSelectionVector() : &lhs_scan_sel;
	//	Searching for right <= left
	for (idx_t i = 0; i < count; ++i) {
		//	If right > left, then there is no match
		const auto left_pos = left_base + lhs_sel->get_index(i);
		if (!Compare(right_key[right_pos], left_key[left_pos], strict)) {
			continue;
		}

		// Exponential search forward for a non-matching value using radix iterators
		// (We use exponential search to avoid thrashing the block manager on large probes)
		idx_t bound = 1;
		idx_t begin = right_pos;
		while (begin + bound < right_group->Count()) {
			if (Compare(right_key[begin + bound], left_key[left_pos], strict)) {
				//	If right <= left, jump ahead
				bound *= 2;
			} else {
				break;
			}
		}

		//	Binary search for the first non-matching value using radix iterators
		//	The previous value (which we know exists) is the match
		auto first = begin + bound / 2;
		auto last = MinValue<idx_t>(begin + bound, right_group->Count());
		while (first < last) {
			const auto mid = first + (last - first) / 2;
			if (Compare(right_key[mid], left_key[left_pos], strict)) {
				//	If right <= left, new lower bound
				first = mid + 1;
			} else {
				last = mid;
			}
		}
		right_pos = --first;

		//	TODO: Check partitions for strict equality
		// 		if (right_group->ComparePartitions(*left_itr, *right_itr)) {
		// 			continue;
		// 		}

		// Emit match data
		if (found_match) {
			found_match[i] = true;
		}
		if (matches) {
			matches[i] = first;
		}
		lhs_match_sel.set_index(lhs_match_count++, i);
	}
}

void AsOfProbeBuffer::ResolveSimpleJoin(ExecutionContext &context, DataChunk &chunk) {
	// perform the actual join
	bool found_match[STANDARD_VECTOR_SIZE] = {false};
	(this->*resolve_join_func)(found_match, nullptr);

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

static idx_t SliceSelectionVector(SelectionVector &target, const SelectionVector &source, const idx_t count) {
	idx_t result = 0;
	for (idx_t i = 0; i < count; ++i) {
		target.set_index(result++, target.get_index(source.get_index(i)));
	}

	return result;
}

void AsOfProbeBuffer::ResolveComplexJoin(ExecutionContext &context, DataChunk &chunk) {
	// perform the actual join
	idx_t matches[STANDARD_VECTOR_SIZE];
	(this->*resolve_join_func)(nullptr, matches);

	//	Extract the rhs input columns from the match
	rhs_input.Reset();
	for (idx_t i = 0; i < lhs_match_count; ++i) {
		const auto idx = lhs_match_sel[i];
		const auto match_pos = matches[idx];
		// Skip to the range containing the match
		while (match_pos >= rhs_scanner->Scanned()) {
			rhs_payload.Reset();
			rhs_scanner->Scan(rhs_payload);
		}
		// Append the individual values
		// TODO: Batch the copies
		const auto source_offset = match_pos - (rhs_scanner->Scanned() - rhs_payload.size());
		for (column_t col_idx = 0; col_idx < rhs_payload.data.size(); ++col_idx) {
			auto &source = rhs_payload.data[col_idx];
			auto &target = rhs_input.data[col_idx];
			VectorOperations::Copy(source, target, source_offset + 1, source_offset, i);
		}
	}
	rhs_input.SetCardinality(lhs_match_count);

	//	Slice the left payload into the result
	for (column_t i = 0; i < lhs_payload.ColumnCount(); ++i) {
		chunk.data[i].Slice(lhs_payload.data[i], lhs_match_sel, lhs_match_count);
	}

	//	Reference the projected right payload into the result
	for (column_t col_idx = 0; col_idx < op.right_projection_map.size(); ++col_idx) {
		const auto rhs_idx = op.right_projection_map[col_idx];
		auto &source = rhs_input.data[rhs_idx];
		auto &target = chunk.data[lhs_payload.ColumnCount() + col_idx];
		target.Reference(source);
	}
	chunk.SetCardinality(lhs_match_count);

	//	Filter out partition mismatches
	const auto equal_cols = op.conditions.size() - 1;
	if (equal_cols) {
		//	Prepare the lhs keys
		if (lhs_match_count < lhs_keys.size()) {
			lhs_keys.Slice(lhs_match_sel, lhs_match_count);
		}

		rhs_keys.Reset();
		rhs_executor.Execute(rhs_input, rhs_keys);

		auto sel = FlatVector::IncrementalSelectionVector();
		auto tail_count = lhs_match_count;
		for (size_t cmp_idx = 0; cmp_idx < equal_cols; ++cmp_idx) {
			auto &left = lhs_keys.data[cmp_idx];
			auto &right = rhs_keys.data[cmp_idx];
			if (tail_count < rhs_keys.size()) {
				left.Slice(*sel, tail_count);
				right.Slice(*sel, tail_count);
			}
			tail_count = PhysicalRangeJoin::SelectJoinTail(op.conditions[cmp_idx].comparison, left, right, sel,
			                                               tail_count, &tail_sel);
			sel = &tail_sel;
		}

		//	Did anything get filtered out?
		if (tail_count < lhs_match_count) {
			if (tail_count == 0) {
				// Need to reset here otherwise we may use the non-flat chunk when constructing LEFT/OUTER
				chunk.Reset();
				lhs_match_count = tail_count;
			} else {
				chunk.Slice(*sel, tail_count);
				//	Slice lhs_match_sel to the remaining lhs rows
				lhs_match_count = SliceSelectionVector(lhs_match_sel, *sel, tail_count);
			}
		}
	}

	//	Apply the predicate filter
	//	TODO: This is wrong - we have to search for a match
	if (filterer.expressions.size() == 1) {
		const auto filter_count = filterer.SelectExpression(chunk, filter_sel);
		if (filter_count < chunk.size()) {
			chunk.Slice(filter_sel, filter_count);
			lhs_match_count = SliceSelectionVector(lhs_match_sel, filter_sel, filter_count);
		}
	}

	//	Update the match masks for the rows we ended up with
	left_outer.Reset();
	for (idx_t i = 0; i < lhs_match_count; ++i) {
		const auto idx = lhs_match_sel.get_index(i);
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
			left_outer.ConstructLeftJoinResult(lhs_scanned, chunk);
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
	AsOfGlobalSourceState(ClientContext &client, AsOfGlobalSinkState &gsink_p);

	AsOfGlobalSinkState &gsink;
	//! The next buffer to flush
	atomic<size_t> next_left;
	//! The number of flushed buffers
	atomic<size_t> flushed;
	//! The right outer output read position.
	atomic<idx_t> next_right;

public:
	idx_t MaxThreads() override {
		return gsink.hash_groups[1].size();
	}
};

AsOfGlobalSourceState::AsOfGlobalSourceState(ClientContext &client, AsOfGlobalSinkState &gsink_p)
    : gsink(gsink_p), next_left(0), flushed(0), next_right(0) {

	//	 Take ownership of the hash groups
	for (idx_t child = 0; child < 2; ++child) {
		auto &hashed_sort = *gsink.hashed_sorts[child];
		auto &hashed_sink = *gsink.hashed_sinks[child];
		auto hashed_source = hashed_sort.GetGlobalSourceState(client, hashed_sink);
		auto &sorted_runs = hashed_sort.GetSortedRuns(*hashed_source);
		auto &hash_groups = gsink.hash_groups[child];
		hash_groups.resize(sorted_runs.size());

		for (idx_t group_idx = 0; group_idx < sorted_runs.size(); ++group_idx) {
			hash_groups[group_idx] = std::move(sorted_runs[group_idx]);
		}
	}

	// for FULL/RIGHT OUTER JOIN, initialize right_outers to false for every tuple
	auto &rhs_partition = gsink.hash_groups[1];
	auto &right_outers = gsink.right_outers;
	right_outers.reserve(rhs_partition.size());
	for (const auto &hash_group : rhs_partition) {
		right_outers.emplace_back(OuterJoinMarker(gsink.is_outer));
		right_outers.back().Initialize(hash_group ? hash_group->Count() : 0);
	}
}

unique_ptr<GlobalSourceState> PhysicalAsOfJoin::GetGlobalSourceState(ClientContext &client) const {
	auto &gsink = sink_state->Cast<AsOfGlobalSinkState>();
	return make_uniq<AsOfGlobalSourceState>(client, gsink);
}

class AsOfLocalSourceState : public LocalSourceState {
public:
	using HashGroupPtr = unique_ptr<SortedRun>;

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
	unique_ptr<AsOfPayloadScanner> scanner;
	//! Pointer to the right marker
	const bool *rhs_matches = {};
};

AsOfLocalSourceState::AsOfLocalSourceState(ExecutionContext &context, AsOfGlobalSourceState &gsource,
                                           const PhysicalAsOfJoin &op)
    : gsource(gsource), context(context), probe_buffer(context.client, op) {
}

idx_t AsOfLocalSourceState::BeginRightScan(const idx_t hash_bin_p) {
	hash_bin = hash_bin_p;

	auto &rhs_groups = gsource.gsink.hash_groups[1];
	if (hash_bin >= rhs_groups.size()) {
		return 0;
	}

	hash_group = std::move(rhs_groups[hash_bin]);
	if (!hash_group || !hash_group->Count()) {
		return 0;
	}
	scanner = make_uniq<AsOfPayloadScanner>(*hash_group, *gsource.gsink.hashed_sorts[1]);

	rhs_matches = gsource.gsink.right_outers[hash_bin].GetMatches();

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
	auto &rhs_groups = gsource.gsink.hash_groups[1];
	auto &client = context.client;

	//	Step 1: Join the partitions
	auto &lhs_groups = gsource.gsink.hash_groups[0];
	const auto left_bins = lhs_groups.size();
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

	//	Step 2: Emit right join matches
	if (!IsRightOuterJoin(join_type)) {
		return SourceResultType::FINISHED;
	}

	DataChunk rhs_chunk;
	rhs_chunk.Initialize(context.client, children[1].get().GetTypes());
	SelectionVector rsel(STANDARD_VECTOR_SIZE);

	while (chunk.size() == 0) {
		//	Move to the next bin if we are done.
		while (!lsource.scanner || !lsource.scanner->Remaining()) {
			lsource.scanner.reset();
			lsource.hash_group.reset();
			auto hash_bin = gsource.next_right++;
			if (hash_bin >= rhs_groups.size()) {
				return SourceResultType::FINISHED;
			}

			for (; hash_bin < rhs_groups.size(); hash_bin = gsource.next_right++) {
				if (rhs_groups[hash_bin]) {
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
