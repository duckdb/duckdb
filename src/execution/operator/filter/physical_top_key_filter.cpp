#include "duckdb/execution/operator/filter/physical_top_key_filter.hpp"

#include "duckdb/common/set.hpp"
#include "duckdb/common/sorting/topn_heap.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/chunk_hasher.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

PhysicalTopKeyFilter::PhysicalTopKeyFilter(PhysicalPlan &physical_plan, ClientContext &context,
                                           vector<LogicalType> types, vector<BoundOrderByNode> orders_p, idx_t limit,
                                           shared_ptr<DynamicFilterData> dynamic_filter_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::TOP_KEY_FILTER, std::move(types), estimated_cardinality),
      orders(std::move(orders_p)), limit(limit), dynamic_filter(std::move(dynamic_filter_p)) {
	D_ASSERT(!orders.empty());
	const auto key_count = orders.size();
	for (idx_t key_idx = 0; key_idx < key_count; key_idx++) {
		auto &type = orders[key_idx].expression->GetReturnType();
		not_distinct_from.push_back(BoundComparisonExpression::CreateOnColumns(
		    context, ExpressionType::COMPARE_NOT_DISTINCT_FROM, type, key_idx, type, key_count + key_idx));
	}
}

PhysicalTopKeyFilter::~PhysicalTopKeyFilter() {
}

//===--------------------------------------------------------------------===//
// Global State
//===--------------------------------------------------------------------===//
//! The bound shared by the threads: the smallest of the largest keys of the full local sets
class TopKeyFilterGlobalState : public GlobalOperatorState {
public:
	explicit TopKeyFilterGlobalState(const PhysicalTopKeyFilter &op) : boundary(op.orders[0], op.dynamic_filter) {
	}

	TopNBoundaryValue boundary;
};

//===--------------------------------------------------------------------===//
// Local State
//===--------------------------------------------------------------------===//
class TopKeyFilterState : public OperatorState {
public:
	//! The number of key hashes that the cache of offered keys remembers
	static constexpr idx_t SEEN_HASH_SLOTS = 4096;
	//! After this many chunks without a change of the local set, a chunk is only offered to it when the number of
	//! idle chunks has doubled. Skipping an offer delays the bound, it never makes the filter drop a row that it has
	//! to keep.
	static constexpr idx_t IDLE_WARMUP_CHUNKS = 16;
	//! A thread offers keys to its set only after it has seen this many rows per key of the limit: filling the set
	//! costs a sort key and an insert per distinct key, which a thread that sees few rows never earns back
	static constexpr idx_t WARMUP_ROWS_PER_KEY = 256;
	//! A thread that still passes more than one in INEFFECTIVE_PASS_FRACTION of this many rows stops filtering: the
	//! limit covers too many of the keys for the filter to pay off
	static constexpr idx_t INEFFECTIVE_CHECK_ROWS = idx_t(100) * STANDARD_VECTOR_SIZE;
	static constexpr idx_t INEFFECTIVE_PASS_FRACTION = 8;

public:
	TopKeyFilterState(ExecutionContext &context, const PhysicalTopKeyFilter &op)
	    : op(op), executor(context.client),
	      selector(context.client, BufferManager::GetBufferManager(context.client).GetBufferAllocator(), op.orders,
	               op.not_distinct_from),
	      seen(SEEN_HASH_SLOTS, 0), hashes(LogicalType::HASH), pass_sel(STANDARD_VECTOR_SIZE),
	      candidate_sel(STANDARD_VECTOR_SIZE), new_sel(STANDARD_VECTOR_SIZE) {
		vector<LogicalType> key_types;
		for (auto &order : op.orders) {
			key_types.push_back(order.expression->GetReturnType());
			executor.AddExpression(*order.expression);
			modifiers.emplace_back(order.type, order.null_order);
		}
		hasher = make_uniq<ChunkHasher>(context.client, key_types);
		auto &allocator = BufferManager::GetBufferManager(context.client).GetBufferAllocator();
		key_chunk.Initialize(allocator, key_types);
		local_max_values.Initialize(allocator, key_types);
		candidates.Initialize(allocator, key_types, vector<bool>(key_types.size(), false));
		sort_key_chunk.Initialize(allocator, {LogicalType::BLOB});
	}

	const PhysicalTopKeyFilter &op;
	ExpressionExecutor executor;
	vector<OrderModifiers> modifiers;
	//! The keys of the current input chunk
	DataChunk key_chunk;
	//! The global bound as this thread last saw it, and the selection of the rows up to a bound. A bound that is older
	//! than the global one sorts after it: it only lets more rows pass.
	TopNBoundarySelector selector;
	//! The smallest "limit" distinct sort keys that this thread has seen, and the largest of them as constant vectors
	set<string> heap;
	DataChunk local_max_values;
	//! The hashes of keys that were offered to the set: a hit skips the sort key (a collision only skips an offer)
	vector<hash_t> seen;
	unique_ptr<ChunkHasher> hasher;
	Vector hashes;
	DataChunk candidates;
	DataChunk sort_key_chunk;
	//! The number of chunks since the local set last changed
	idx_t idle_chunks = 0;
	//! The number of rows that this thread has seen
	idx_t rows_seen = 0;
	//! The number of rows that this thread has compared with the bound, and how many of them passed
	idx_t checked_rows = 0;
	idx_t passed_rows = 0;
	//! Whether this thread has stopped filtering
	bool pass_through = false;

	SelectionVector pass_sel;
	SelectionVector candidate_sel;
	SelectionVector new_sel;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op);
	}

	//! Offers the rows that passed (sel, or the whole chunk) to the local set and publishes a changed bound
	void MaintainHeap(optional_ptr<const SelectionVector> sel, idx_t count, TopKeyFilterGlobalState &gstate) {
		if (count == 0) {
			return;
		}
		if (idle_chunks >= IDLE_WARMUP_CHUNKS && !IsPowerOfTwo(idle_chunks)) {
			idle_chunks++;
			return;
		}
		// the rows that can change the set: all of them while it is not full, or else those before its largest key
		const auto is_full = heap.size() >= op.limit;
		auto candidate_count = count;
		auto candidates_sel = sel;
		if (is_full) {
			// strictly before the largest key of the set: an equal key is in the set already
			candidate_count = selector.Select(key_chunk, local_max_values, sel, count, TopNBoundaryTies::EXCLUDE,
			                                  TopNBoundaryRowOrder::ANY, candidate_sel);
			candidates_sel = &candidate_sel;
		}
		if (candidate_count == 0) {
			idle_chunks++;
			return;
		}
		// skip the keys that have been offered before
		hasher->Hash(key_chunk, hashes);
		hashes.Flatten();
		auto hash_data = FlatVector::GetData<hash_t>(hashes);
		idx_t new_count = 0;
		for (idx_t i = 0; i < candidate_count; i++) {
			const auto row = candidates_sel ? candidates_sel->get_index(i) : i;
			const auto hash = hash_data[row];
			auto &slot = seen[hash & (SEEN_HASH_SLOTS - 1)];
			// an empty slot is 0, as is the hash of an integer 0: a zero hash is never remembered
			if (hash != 0 && slot == hash) {
				continue;
			}
			slot = hash;
			new_sel.set_index(new_count++, row);
		}
		if (new_count == 0) {
			idle_chunks++;
			return;
		}
		// create the sort keys of the new rows
		candidates.Reset();
		candidates.Slice(key_chunk, new_sel, new_count);
		sort_key_chunk.Reset();
		CreateSortKeyHelpers::CreateSortKey(candidates, modifiers, sort_key_chunk.data[0]);
		auto sort_keys = FlatVector::GetData<string_t>(sort_key_chunk.data[0]);
		bool changed = false;
		for (idx_t i = 0; i < new_count; i++) {
			auto sort_key = sort_keys[i].GetString();
			if (heap.size() >= op.limit && sort_key >= *heap.rbegin()) {
				continue;
			}
			if (!heap.insert(std::move(sort_key)).second) {
				continue;
			}
			changed = true;
			if (heap.size() > op.limit) {
				heap.erase(std::prev(heap.end()));
			}
		}
		if (!changed) {
			idle_chunks++;
			return;
		}
		idle_chunks = 0;
		if (heap.size() >= op.limit) {
			auto &local_max = *heap.rbegin();
			selector.Decode(local_max, local_max_values);
			gstate.boundary.UpdateValue(string_t(local_max));
		}
	}
};

void PhysicalTopKeyFilter::InitializeOutputChunk(Allocator &allocator, DataChunk &chunk) const {
	// every output column only references or slices the input
	chunk.Initialize(allocator, types, vector<bool>(types.size(), false));
}

unique_ptr<OperatorState> PhysicalTopKeyFilter::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<TopKeyFilterState>(context, *this);
}

unique_ptr<GlobalOperatorState> PhysicalTopKeyFilter::GetGlobalOperatorState(ClientContext &context) const {
	// a new bound for every execution
	if (dynamic_filter) {
		dynamic_filter->Reset();
	}
	return make_uniq<TopKeyFilterGlobalState>(*this);
}

OperatorResultType PhysicalTopKeyFilter::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                 GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = gstate_p.Cast<TopKeyFilterGlobalState>();
	auto &state = state_p.Cast<TopKeyFilterState>();
	const auto count = input.size();
	if (count == 0) {
		return OperatorResultType::NEED_MORE_INPUT;
	}
	if (state.pass_through) {
		// contract: passing a row is always allowed
		chunk.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}
	state.rows_seen += count;
	const auto warming_up = state.rows_seen <= TopKeyFilterState::WARMUP_ROWS_PER_KEY * limit;
	const auto has_boundary = state.selector.Refresh(gstate.boundary);
	if (warming_up && !has_boundary) {
		// no bound to filter with and no key to offer yet: the rows pass as they are (contract: extra rows may pass)
		chunk.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}
	state.key_chunk.Reset();
	state.executor.Execute(input, state.key_chunk);

	// the rows up to the global bound, as far as this thread knows it
	idx_t pass_count = count;
	if (has_boundary) {
		// the rows of the bound key itself pass (it is the key of a group that can be returned), in the order of the
		// input (the aggregate may depend on it)
		pass_count =
		    state.selector.Select(state.key_chunk, state.selector.BoundaryValues(), nullptr, count,
		                          TopNBoundaryTies::INCLUDE, TopNBoundaryRowOrder::INPUT_ORDER, state.pass_sel);
		if (!dynamic_filter && !warming_up) {
			// without a dynamic filter in the scans this operator sees every row: if too many of them pass, the
			// filter costs more than the aggregate saves. Passing every row is always correct. Every compared chunk
			// counts, also one that is rejected as a whole (clustered input).
			state.checked_rows += count;
			state.passed_rows += pass_count;
			if (state.checked_rows >= TopKeyFilterState::INEFFECTIVE_CHECK_ROWS) {
				state.pass_through =
				    state.passed_rows > state.checked_rows / TopKeyFilterState::INEFFECTIVE_PASS_FRACTION;
				state.checked_rows = 0;
				state.passed_rows = 0;
			}
		}
		if (pass_count == 0) {
			return OperatorResultType::NEED_MORE_INPUT;
		}
	}
	const auto filtered = pass_count < count;
	// offer the rows that pass to the local set. A row after the bound cannot enter it, and a thread that is still
	// warming up only filters with the bound of the other threads.
	if (!warming_up) {
		state.MaintainHeap(filtered ? &state.pass_sel : nullptr, pass_count, gstate);
	}
	if (filtered) {
		chunk.Slice(input, state.pass_sel, pass_count);
	} else {
		chunk.Reference(input);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

InsertionOrderPreservingMap<string> PhysicalTopKeyFilter::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string keys;
	for (idx_t i = 0; i < orders.size(); i++) {
		if (i > 0) {
			keys += "\n";
		}
		keys += orders[i].ToString();
	}
	result["Keys"] = keys;
	result["Limit"] = to_string(limit);
	if (dynamic_filter) {
		result["Dynamic Filter"] = "true";
	}
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
