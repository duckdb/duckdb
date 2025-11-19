#include "duckdb/execution/operator/join/physical_iejoin.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/sorting/sort_key.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <utility>

namespace duckdb {

PhysicalIEJoin::PhysicalIEJoin(PhysicalPlan &physical_plan, LogicalComparisonJoin &op, PhysicalOperator &left,
                               PhysicalOperator &right, vector<JoinCondition> cond, JoinType join_type,
                               idx_t estimated_cardinality, unique_ptr<JoinFilterPushdownInfo> pushdown_info)
    : PhysicalRangeJoin(physical_plan, op, PhysicalOperatorType::IE_JOIN, left, right, std::move(cond), join_type,
                        estimated_cardinality, std::move(pushdown_info)) {
	// 1. let L1 (resp. L2) be the array of column X (resp. Y)
	D_ASSERT(conditions.size() >= 2);
	for (idx_t i = 0; i < 2; ++i) {
		auto &cond = conditions[i];
		D_ASSERT(cond.left->return_type == cond.right->return_type);
		join_key_types.push_back(cond.left->return_type);

		// Convert the conditions to sort orders
		auto left = cond.left->Copy();
		auto right = cond.right->Copy();
		auto sense = OrderType::INVALID;

		// 2. if (op1 ∈ {>, ≥}) sort L1 in descending order
		// 3. else if (op1 ∈ {<, ≤}) sort L1 in ascending order
		// 4. if (op2 ∈ {>, ≥}) sort L2 in ascending order
		// 5. else if (op2 ∈ {<, ≤}) sort L2 in descending order
		switch (cond.comparison) {
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			sense = i ? OrderType::ASCENDING : OrderType::DESCENDING;
			break;
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			sense = i ? OrderType::DESCENDING : OrderType::ASCENDING;
			break;
		default:
			throw NotImplementedException("Unimplemented join type for IEJoin");
		}
		lhs_orders.emplace_back(sense, OrderByNullType::NULLS_LAST, std::move(left));
		rhs_orders.emplace_back(sense, OrderByNullType::NULLS_LAST, std::move(right));
	}

	for (idx_t i = 2; i < conditions.size(); ++i) {
		auto &cond = conditions[i];
		D_ASSERT(cond.left->return_type == cond.right->return_type);
		join_key_types.push_back(cond.left->return_type);
	}
}

PhysicalIEJoin::PhysicalIEJoin(PhysicalPlan &physical_plan, LogicalComparisonJoin &op, PhysicalOperator &left,
                               PhysicalOperator &right, vector<JoinCondition> cond, JoinType join_type,
                               idx_t estimated_cardinality)
    : PhysicalIEJoin(physical_plan, op, left, right, std::move(cond), join_type, estimated_cardinality, nullptr) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class IEJoinLocalState;

class IEJoinGlobalState : public GlobalSinkState {
public:
	using GlobalSortedTable = PhysicalRangeJoin::GlobalSortedTable;

public:
	IEJoinGlobalState(ClientContext &context, const PhysicalIEJoin &op) : child(1) {
		tables.resize(2);
		const auto &lhs_types = op.children[0].get().GetTypes();
		vector<BoundOrderByNode> lhs_order;
		lhs_order.emplace_back(op.lhs_orders[0].Copy());
		tables[0] = make_uniq<GlobalSortedTable>(context, lhs_order, lhs_types, op);

		const auto &rhs_types = op.children[1].get().GetTypes();
		vector<BoundOrderByNode> rhs_order;
		rhs_order.emplace_back(op.rhs_orders[0].Copy());
		tables[1] = make_uniq<GlobalSortedTable>(context, rhs_order, rhs_types, op);

		if (op.filter_pushdown) {
			skip_filter_pushdown = op.filter_pushdown->probe_info.empty();
			global_filter_state = op.filter_pushdown->GetGlobalState(context, op);
		}
	}

	void Sink(ExecutionContext &context, DataChunk &input, IEJoinLocalState &lstate);

	void Finalize(ClientContext &client, InterruptState &interrupt) {
		// Sort the current input child
		D_ASSERT(child < tables.size());
		tables[child]->Finalize(client, interrupt);
	};

	void Materialize(Pipeline &pipeline, Event &event) {
		// Sort the current input child
		D_ASSERT(child < tables.size());
		tables[child]->Materialize(pipeline, event);
		child = child ? 0 : 2;
		skip_filter_pushdown = true;
	};

	//! The two input tables (IEJoin materialises both sides)
	vector<unique_ptr<GlobalSortedTable>> tables;
	//! The child that is being materialised (right/1 then left/0)
	size_t child;
	//! Should we not bother pushing down filters?
	bool skip_filter_pushdown = false;
	//! The global filter states to push down (if any)
	unique_ptr<JoinFilterGlobalState> global_filter_state;
};

class IEJoinLocalState : public LocalSinkState {
public:
	using LocalSortedTable = PhysicalRangeJoin::LocalSortedTable;

	IEJoinLocalState(ExecutionContext &context, const PhysicalRangeJoin &op, IEJoinGlobalState &gstate)
	    : table(context, *gstate.tables[gstate.child], gstate.child) {
		if (op.filter_pushdown) {
			local_filter_state = op.filter_pushdown->GetLocalState(*gstate.global_filter_state);
		}
	}

	//! The local sort state
	LocalSortedTable table;
	//! Local state for accumulating filter statistics
	unique_ptr<JoinFilterLocalState> local_filter_state;
};

unique_ptr<GlobalSinkState> PhysicalIEJoin::GetGlobalSinkState(ClientContext &context) const {
	D_ASSERT(!sink_state);
	return make_uniq<IEJoinGlobalState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalIEJoin::GetLocalSinkState(ExecutionContext &context) const {
	auto &ie_sink = sink_state->Cast<IEJoinGlobalState>();
	return make_uniq<IEJoinLocalState>(context, *this, ie_sink);
}

void IEJoinGlobalState::Sink(ExecutionContext &context, DataChunk &input, IEJoinLocalState &lstate) {
	// Sink the data into the local sort state
	lstate.table.Sink(context, input);
}

SinkResultType PhysicalIEJoin::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<IEJoinGlobalState>();
	auto &lstate = input.local_state.Cast<IEJoinLocalState>();

	if (gstate.child == 0 && gstate.tables[1]->Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return SinkResultType::FINISHED;
	}

	gstate.Sink(context, chunk, lstate);

	if (filter_pushdown && !gstate.skip_filter_pushdown) {
		filter_pushdown->Sink(lstate.table.keys, *lstate.local_filter_state);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalIEJoin::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<IEJoinGlobalState>();
	auto &lstate = input.local_state.Cast<IEJoinLocalState>();
	gstate.tables[gstate.child]->Combine(context, lstate.table);
	auto &client_profiler = QueryProfiler::Get(context.client);

	context.thread.profiler.Flush(*this);
	client_profiler.Flush(context.thread.profiler);

	if (filter_pushdown && !gstate.skip_filter_pushdown) {
		filter_pushdown->Combine(*gstate.global_filter_state, *lstate.local_filter_state);
	}

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalIEJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &client,
                                          OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<IEJoinGlobalState>();
	if (filter_pushdown && !gstate.skip_filter_pushdown) {
		(void)filter_pushdown->Finalize(client, nullptr, *gstate.global_filter_state, *this);
	}
	auto &table = *gstate.tables[gstate.child];

	if ((gstate.child == 1 && PropagatesBuildSide(join_type)) || (gstate.child == 0 && IsLeftOuterJoin(join_type))) {
		// for FULL/LEFT/RIGHT OUTER JOIN, initialize found_match to false for every tuple
		table.IntializeMatches();
	}

	SinkFinalizeType res;
	if (gstate.child == 1 && table.Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		// Empty input!
		res = SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	} else {
		res = SinkFinalizeType::READY;
	}

	// Clean up the current table
	gstate.Finalize(client, input.interrupt_state);

	// Move to the next input child
	gstate.Materialize(pipeline, event);

	return res;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
OperatorResultType PhysicalIEJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   GlobalOperatorState &gstate, OperatorState &state) const {
	return OperatorResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
enum class IEJoinSourceStage : uint8_t { INIT, INNER, OUTER, DONE };

struct IEJoinUnion {
	using SortedTable = PhysicalRangeJoin::GlobalSortedTable;
	using ChunkRange = std::pair<idx_t, idx_t>;

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
			throw InternalException("Unimplemented comparison type for IEJoin!");
		}
	}

	template <typename T>
	static inline bool Compare(const T &lhs, const T &rhs, const bool strict) {
		const bool less_than = lhs < rhs;
		if (!less_than && !strict) {
			return !(rhs < lhs);
		}
		return less_than;
	}

	template <SortKeyType SORT_KEY_TYPE>
	static bool TemplatedCompareKeys(ExternalBlockIteratorState &state1, const idx_t pos1,
	                                 ExternalBlockIteratorState &state2, const idx_t pos2, bool strict);

	static bool CompareKeys(ExternalBlockIteratorState &state1, const idx_t pos1, ExternalBlockIteratorState &state2,
	                        const idx_t pos2, bool strict, const SortKeyType &sort_key_type);

	static bool CompareBounds(SortedTable &t1, const ChunkRange &b1, SortedTable &t2, const ChunkRange &b2,
	                          bool strict);

	static idx_t AppendKey(ExecutionContext &context, InterruptState &interrupt, SortedTable &table,
	                       ExpressionExecutor &executor, SortedTable &marked, int64_t increment, int64_t rid,
	                       const ChunkRange &range);

	static void Sort(ExecutionContext &context, InterruptState &interrupt, SortedTable &table) {
		table.Finalize(context.client, interrupt);
		table.Materialize(context, interrupt);
	}

	template <typename T, typename VECTOR_TYPE = T>
	static vector<T> ExtractColumn(SortedTable &table, idx_t col_idx) {
		vector<T> result;
		result.reserve(table.count);

		auto &collection = *table.sorted->payload_data;
		vector<column_t> scan_ids(1, col_idx);
		TupleDataScanState state;
		collection.InitializeScan(state, scan_ids);

		DataChunk payload;
		collection.InitializeScanChunk(state, payload);

		while (collection.Scan(state, payload)) {
			const auto count = payload.size();
			const auto data_ptr = FlatVector::GetData<VECTOR_TYPE>(payload.data[0]);
			for (idx_t i = 0; i < count; i++) {
				result.push_back(UnsafeNumericCast<T>(data_ptr[i]));
			}
		}

		return result;
	}

	class UnionIterator {
	public:
		UnionIterator(SortedTable &table, bool strict) : state(table.CreateIteratorState()), strict(strict) {
		}

		inline idx_t GetIndex() const {
			return index;
		}

		inline void SetIndex(idx_t i) {
			index = i;
		}

		UnionIterator &operator++() {
			++index;
			return *this;
		}

		unique_ptr<ExternalBlockIteratorState> state;
		idx_t index = 0;
		const bool strict;
	};

	IEJoinUnion(SortedTable &t1, const ChunkRange &b1, SortedTable &t2, const ChunkRange &b2);

	void Build(ExecutionContext &context, const PhysicalIEJoin &op);

	idx_t SearchL1(idx_t pos);

	template <SortKeyType SORT_KEY_TYPE>
	bool NextRow();

	using next_row_t = bool (duckdb::IEJoinUnion::*)();
	next_row_t next_row_func;

	//! Constructor arguments
	bool built = false;
	SortedTable &t1;
	const ChunkRange b1;
	SortedTable &t2;
	const ChunkRange b2;

	//! Inverted loop
	idx_t JoinComplexBlocks(SelectionVector &lsel, SelectionVector &rsel);

	//! L1
	unique_ptr<SortedTable> l1;
	//! L2
	unique_ptr<SortedTable> l2;

	//! Li
	vector<int64_t> li;
	//! P
	vector<idx_t> p;

	//! B
	vector<validity_t> bit_array;
	ValidityMask bit_mask;

	//! Bloom Filter
	static constexpr idx_t BLOOM_CHUNK_BITS = 1024;
	idx_t bloom_count;
	vector<validity_t> bloom_array;
	ValidityMask bloom_filter;

	//! Iteration state
	idx_t n;
	idx_t i;
	idx_t j;
	unique_ptr<UnionIterator> op1;
	unique_ptr<UnionIterator> off1;
	unique_ptr<UnionIterator> op2;
	unique_ptr<UnionIterator> off2;
	int64_t lrid;
};

idx_t IEJoinUnion::AppendKey(ExecutionContext &context, InterruptState &interrupt, SortedTable &table,
                             ExpressionExecutor &executor, SortedTable &marked, int64_t increment, int64_t rid,
                             const ChunkRange &chunk_range) {
	const auto chunk_begin = chunk_range.first;
	const auto chunk_end = chunk_range.second;

	// Reading
	const auto valid = table.count - table.has_null;
	auto &source = *table.sorted->payload_data;
	TupleDataScanState scanner;
	source.InitializeScan(scanner);

	DataChunk scanned;
	source.InitializeScanChunk(scanner, scanned);
	idx_t table_idx = source.Seek(scanner, chunk_begin);

	// Writing
	auto &sort = *marked.sort;
	auto local_sort_state = sort.GetLocalSinkState(context);
	vector<LogicalType> types;
	for (const auto &expr : executor.expressions) {
		types.emplace_back(expr->return_type);
	}
	const idx_t rid_idx = types.size();
	types.emplace_back(LogicalType::BIGINT);

	DataChunk keys;
	DataChunk payload;
	keys.Initialize(Allocator::DefaultAllocator(), types);

	OperatorSinkInput sink {*marked.global_sink, *local_sort_state, interrupt};
	idx_t inserted = 0;
	for (auto chunk_idx = chunk_begin; chunk_idx < chunk_end; ++chunk_idx) {
		source.Scan(scanner, scanned);

		// NULLs are at the end, so stop when we reach them
		auto scan_count = scanned.size();
		if (table_idx + scan_count > valid) {
			if (table_idx >= valid) {
				scan_count = 0;
				;
			} else {
				scan_count = valid - table_idx;
				scanned.SetCardinality(scan_count);
			}
		}
		if (scan_count == 0) {
			break;
		}
		table_idx += scan_count;

		// Compute the input columns from the payload
		keys.Reset();
		keys.Split(payload, rid_idx);
		executor.Execute(scanned, keys);

		// Mark the rid column
		payload.data[0].Sequence(rid, increment, scan_count);
		payload.SetCardinality(scan_count);
		keys.Fuse(payload);
		rid += increment * UnsafeNumericCast<int64_t>(scan_count);

		// Sort on the sort columns (which will no longer be needed)
		sort.Sink(context, keys, sink);
		inserted += scan_count;
	}
	OperatorSinkCombineInput combine {*marked.global_sink, *local_sort_state, interrupt};
	sort.Combine(context, combine);
	marked.count += inserted;

	return inserted;
}

//	TODO: Function pointers?
template <SortKeyType SORT_KEY_TYPE>
bool IEJoinUnion::TemplatedCompareKeys(ExternalBlockIteratorState &state1, const idx_t pos1,
                                       ExternalBlockIteratorState &state2, const idx_t pos2, bool strict) {
	using SORT_KEY = SortKey<SORT_KEY_TYPE>;
	using BLOCKS_ITERATOR = block_iterator_t<ExternalBlockIteratorState, SORT_KEY>;

	BLOCKS_ITERATOR bounds1(state1, pos1);
	BLOCKS_ITERATOR bounds2(state2, pos2);

	return Compare(*bounds1, *bounds2, strict);
}

bool IEJoinUnion::CompareKeys(ExternalBlockIteratorState &state1, const idx_t pos1, ExternalBlockIteratorState &state2,
                              const idx_t pos2, bool strict, const SortKeyType &sort_key_type) {
	switch (sort_key_type) {
	case SortKeyType::NO_PAYLOAD_FIXED_8:
		return TemplatedCompareKeys<SortKeyType::NO_PAYLOAD_FIXED_8>(state1, pos1, state2, pos2, strict);
	case SortKeyType::NO_PAYLOAD_FIXED_16:
		return TemplatedCompareKeys<SortKeyType::NO_PAYLOAD_FIXED_16>(state1, pos1, state2, pos2, strict);
	case SortKeyType::NO_PAYLOAD_FIXED_24:
		return TemplatedCompareKeys<SortKeyType::NO_PAYLOAD_FIXED_24>(state1, pos1, state2, pos2, strict);
	case SortKeyType::NO_PAYLOAD_FIXED_32:
		return TemplatedCompareKeys<SortKeyType::NO_PAYLOAD_FIXED_32>(state1, pos1, state2, pos2, strict);
	case SortKeyType::NO_PAYLOAD_VARIABLE_32:
		return TemplatedCompareKeys<SortKeyType::NO_PAYLOAD_VARIABLE_32>(state1, pos1, state2, pos2, strict);
	case SortKeyType::PAYLOAD_FIXED_16:
		return TemplatedCompareKeys<SortKeyType::PAYLOAD_FIXED_16>(state1, pos1, state2, pos2, strict);
	case SortKeyType::PAYLOAD_FIXED_24:
		return TemplatedCompareKeys<SortKeyType::PAYLOAD_FIXED_24>(state1, pos1, state2, pos2, strict);
	case SortKeyType::PAYLOAD_FIXED_32:
		return TemplatedCompareKeys<SortKeyType::PAYLOAD_FIXED_32>(state1, pos1, state2, pos2, strict);
	case SortKeyType::PAYLOAD_VARIABLE_32:
		return TemplatedCompareKeys<SortKeyType::PAYLOAD_VARIABLE_32>(state1, pos1, state2, pos2, strict);
	default:
		throw NotImplementedException("IEJoinUnion::CompareKeys for %s", EnumUtil::ToString(sort_key_type));
	}
}

bool IEJoinUnion::CompareBounds(SortedTable &t1, const ChunkRange &b1, SortedTable &t2, const ChunkRange &b2,
                                bool strict) {
	auto &keys1 = *t1.sorted->key_data;
	ExternalBlockIteratorState state1(keys1, nullptr);
	const idx_t pos1 = t1.BlockStart(b1.first);

	auto &keys2 = *t2.sorted->key_data;
	ExternalBlockIteratorState state2(keys2, nullptr);
	const idx_t pos2 = t2.BlockEnd(b2.second - 1);

	const auto sort_key_type = t1.GetSortKeyType();
	D_ASSERT(sort_key_type == t2.GetSortKeyType());
	return CompareKeys(state1, pos1, state2, pos2, strict, sort_key_type);
}

IEJoinUnion::IEJoinUnion(SortedTable &t1, const ChunkRange &b1, SortedTable &t2, const ChunkRange &b2)
    : t1(t1), b1(b1), t2(t2), b2(b2), n(0), i(0) {
}

void IEJoinUnion::Build(ExecutionContext &context, const PhysicalIEJoin &op) {
	if (built) {
		return;
	}
	built = true;

	// input : query Q with 2 join predicates t1.X op1 t2.X' and t1.Y op2 t2.Y', tables T, T' of sizes m and n resp.
	// output: a list of tuple pairs (ti , tj)
	// Note that T/T' are already sorted on X/X' and contain the payload data
	// We only join the two block numbers and use the sizes of the blocks as the counts

	InterruptState interrupt;

	// 0. Filter out tables with no overlap
	if (t1.sorted->key_data->ChunkCount() <= b1.first || t2.sorted->key_data->ChunkCount() <= b2.first) {
		return;
	}

	// t1.X[0] op1 t2.X'[-1]
	const auto strict1 = IsStrictComparison(op.conditions[0].comparison);
	if (!CompareBounds(t1, b1, t2, b2, strict1)) {
		return;
	}

	// 1. let L1 (resp. L2) be the array of column X (resp. Y )
	const auto &order1 = op.lhs_orders[0];
	const auto &order2 = op.lhs_orders[1];

	// 2. if (op1 ∈ {>, ≥}) sort L1 in descending order
	// 3. else if (op1 ∈ {<, ≤}) sort L1 in ascending order

	// For the union algorithm, we make a unified table with the keys and the rids as the payload:
	//		X/X', Y/Y', R/R'/Li
	// The first position is the sort key.
	vector<LogicalType> types;
	types.emplace_back(order2.expression->return_type);
	types.emplace_back(LogicalType::BIGINT);

	// Sort on the first expression
	auto ref = make_uniq<BoundReferenceExpression>(order1.expression->return_type, 0U);
	vector<BoundOrderByNode> orders;
	orders.emplace_back(order1.type, order1.null_order, std::move(ref));
	// The goal is to make i (from the left table) < j (from the right table),
	// if value[i] and value[j] match the condition 1.
	// Add a column from_left to solve the problem when there exist multiple equal values in l1.
	// If the operator is loose inequality, make t1.from_left (== true) sort BEFORE t2.from_left (== false).
	// Otherwise, make t1.from_left sort (== true) sort AFTER t2.from_left (== false).
	// For example, if t1.time <= t2.time
	// | value     | 1     | 1     | 1     | 1     |
	// | --------- | ----- | ----- | ----- | ----- |
	// | from_left | T(l2) | T(l2) | F(r1) | F(r2) |
	// if t1.time < t2.time
	// | value     | 1     | 1     | 1     | 1     |
	// | --------- | ----- | ----- | ----- | ----- |
	// | from_left | F(r2) | F(r1) | T(l2) | T(l1) |
	// Using this OrderType, if i < j then value[i] (from left table) and value[j] (from right table) match
	// the condition (t1.time <= t2.time or t1.time < t2.time), then from_left will force them into the correct order.
	auto from_left = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
	orders.emplace_back(!strict1 ? OrderType::DESCENDING : OrderType::ASCENDING, OrderByNullType::ORDER_DEFAULT,
	                    std::move(from_left));

	l1 = make_uniq<SortedTable>(context.client, orders, types, op);

	// LHS has positive rids
	ExpressionExecutor l_executor(context.client);
	l_executor.AddExpression(*order1.expression);
	// add const column true
	auto left_const = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
	l_executor.AddExpression(*left_const);
	l_executor.AddExpression(*order2.expression);
	AppendKey(context, interrupt, t1, l_executor, *l1, 1, 1, b1);

	// RHS has negative rids
	ExpressionExecutor r_executor(context.client);
	r_executor.AddExpression(*op.rhs_orders[0].expression);
	// add const column flase
	auto right_const = make_uniq<BoundConstantExpression>(Value::BOOLEAN(false));
	r_executor.AddExpression(*right_const);
	r_executor.AddExpression(*op.rhs_orders[1].expression);
	AppendKey(context, interrupt, t2, r_executor, *l1, -1, -1, b2);

	if (!l1->Count()) {
		return;
	}

	Sort(context, interrupt, *l1);

	op1 = make_uniq<UnionIterator>(*l1, strict1);
	off1 = make_uniq<UnionIterator>(*l1, strict1);

	// We don't actually need the L1 column, just its sort key, which is in the sort blocks
	li = ExtractColumn<int64_t>(*l1, types.size() - 1);

	// 4. if (op2 ∈ {>, ≥}) sort L2 in ascending order
	// 5. else if (op2 ∈ {<, ≤}) sort L2 in descending order

	// We sort on Y/Y' to obtain the sort keys and the permutation array.
	// For this we just need a two-column table of Y, P
	types.clear();
	types.emplace_back(LogicalType::BIGINT);

	// Sort on the first expression
	orders.clear();
	ref = make_uniq<BoundReferenceExpression>(order2.expression->return_type, 0U);
	orders.emplace_back(order2.type, order2.null_order, std::move(ref));

	ExpressionExecutor executor(context.client);
	executor.AddExpression(*orders[0].expression);

	l2 = make_uniq<SortedTable>(context.client, orders, types, op);
	AppendKey(context, interrupt, *l1, executor, *l2, 1, 0, {0, l1->BlockCount()});

	Sort(context, interrupt, *l2);

	// We don't actually need the L2 column, just its sort key, which is in the sort blocks

	// 6. compute the permutation array P of L2 w.r.t. L1
	p = ExtractColumn<idx_t, int64_t>(*l2, types.size() - 1);

	// 7. initialize bit-array B (|B| = n), and set all bits to 0
	n = l2->count.load();
	bit_array.resize(ValidityMask::EntryCount(n), 0);
	bit_mask.Initialize(bit_array.data(), n);

	// Bloom filter
	bloom_count = (n + (BLOOM_CHUNK_BITS - 1)) / BLOOM_CHUNK_BITS;
	bloom_array.resize(ValidityMask::EntryCount(bloom_count), 0);
	bloom_filter.Initialize(bloom_array.data(), bloom_count);

	// 11. for(i←1 to n) do
	const auto strict2 = IsStrictComparison(op.conditions[1].comparison);
	op2 = make_uniq<UnionIterator>(*l2, strict2);
	off2 = make_uniq<UnionIterator>(*l2, strict2);
	i = 0;
	j = 0;

	const auto sort_key_type = l2->GetSortKeyType();
	switch (sort_key_type) {
	case SortKeyType::NO_PAYLOAD_FIXED_8:
		next_row_func = &IEJoinUnion::NextRow<SortKeyType::NO_PAYLOAD_FIXED_8>;
		break;
	case SortKeyType::NO_PAYLOAD_FIXED_16:
		next_row_func = &IEJoinUnion::NextRow<SortKeyType::NO_PAYLOAD_FIXED_16>;
		break;
	case SortKeyType::NO_PAYLOAD_FIXED_24:
		next_row_func = &IEJoinUnion::NextRow<SortKeyType::NO_PAYLOAD_FIXED_24>;
		break;
	case SortKeyType::NO_PAYLOAD_FIXED_32:
		next_row_func = &IEJoinUnion::NextRow<SortKeyType::NO_PAYLOAD_FIXED_32>;
		break;
	case SortKeyType::NO_PAYLOAD_VARIABLE_32:
		next_row_func = &IEJoinUnion::NextRow<SortKeyType::NO_PAYLOAD_VARIABLE_32>;
		break;
	case SortKeyType::PAYLOAD_FIXED_16:
		next_row_func = &IEJoinUnion::NextRow<SortKeyType::PAYLOAD_FIXED_16>;
		break;
	case SortKeyType::PAYLOAD_FIXED_24:
		next_row_func = &IEJoinUnion::NextRow<SortKeyType::PAYLOAD_FIXED_24>;
		break;
	case SortKeyType::PAYLOAD_FIXED_32:
		next_row_func = &IEJoinUnion::NextRow<SortKeyType::PAYLOAD_FIXED_32>;
		break;
	case SortKeyType::PAYLOAD_VARIABLE_32:
		next_row_func = &IEJoinUnion::NextRow<SortKeyType::PAYLOAD_VARIABLE_32>;
		break;
	default:
		throw NotImplementedException("IEJoinUnion for %s", EnumUtil::ToString(sort_key_type));
	}

	(this->*next_row_func)();
}

template <SortKeyType SORT_KEY_TYPE>
bool IEJoinUnion::NextRow() {
	using SORT_KEY = SortKey<SORT_KEY_TYPE>;
	using BLOCKS_ITERATOR = block_iterator_t<ExternalBlockIteratorState, SORT_KEY>;

	BLOCKS_ITERATOR off2_itr(*off2->state);
	BLOCKS_ITERATOR op2_itr(*op2->state);
	const auto strict = off2->strict;

	for (; i < n; ++i) {
		// 12. pos ← P[i]
		auto pos = p[i];
		lrid = li[pos];
		if (lrid < 0) {
			continue;
		}

		// 16. B[pos] ← 1
		op2->SetIndex(i);
		for (; off2->GetIndex() < n; ++(*off2)) {
			if (!Compare(off2_itr[off2->GetIndex()], op2_itr[op2->GetIndex()], strict)) {
				break;
			}
			const auto p2 = p[off2->GetIndex()];
			if (li[p2] < 0) {
				// Only mark rhs matches.
				bit_mask.SetValid(p2);
				bloom_filter.SetValid(p2 / BLOOM_CHUNK_BITS);
			}
		}

		// 9.  if (op1 ∈ {≤,≥} and op2 ∈ {≤,≥}) eqOff = 0
		// 10. else eqOff = 1
		// No, because there could be more than one equal value.
		// Find the leftmost off1 where L1[pos] op1 L1[off1..n]
		// These are the rows that satisfy the op1 condition
		// and that is where we should start scanning B from
		j = pos;

		return true;
	}
	return false;
}

static idx_t NextValid(const ValidityMask &bits, idx_t j, const idx_t n) {
	if (j >= n) {
		return n;
	}

	// We can do a first approximation by checking entries one at a time
	// which gives 64:1.
	idx_t entry_idx, idx_in_entry;
	bits.GetEntryIndex(j, entry_idx, idx_in_entry);
	auto entry = bits.GetValidityEntry(entry_idx++);

	// Trim the bits before the start position
	entry &= (ValidityMask::ValidityBuffer::MAX_ENTRY << idx_in_entry);

	// Check the non-ragged entries
	for (const auto entry_count = bits.EntryCount(n); entry_idx < entry_count; ++entry_idx) {
		if (entry) {
			for (; idx_in_entry < bits.BITS_PER_VALUE; ++idx_in_entry, ++j) {
				if (bits.RowIsValid(entry, idx_in_entry)) {
					return j;
				}
			}
		} else {
			j += bits.BITS_PER_VALUE - idx_in_entry;
		}

		entry = bits.GetValidityEntry(entry_idx);
		idx_in_entry = 0;
	}

	// Check the final entry
	for (; j < n; ++idx_in_entry, ++j) {
		if (bits.RowIsValid(entry, idx_in_entry)) {
			return j;
		}
	}

	return j;
}

idx_t IEJoinUnion::JoinComplexBlocks(SelectionVector &lsel, SelectionVector &rsel) {
	// 8. initialize join result as an empty list for tuple pairs
	idx_t result_count = 0;

	// 11. for(i←1 to n) do
	while (i < n) {
		// 13. for (j ← pos+eqOff to n) do
		for (;;) {
			// 14. if B[j] = 1 then

			//	Use the Bloom filter to find candidate blocks
			while (j < n) {
				auto bloom_begin = NextValid(bloom_filter, j / BLOOM_CHUNK_BITS, bloom_count) * BLOOM_CHUNK_BITS;
				auto bloom_end = MinValue<idx_t>(n, bloom_begin + BLOOM_CHUNK_BITS);

				j = MaxValue<idx_t>(j, bloom_begin);
				j = NextValid(bit_mask, j, bloom_end);
				if (j < bloom_end) {
					break;
				}
			}

			if (j >= n) {
				break;
			}

			// Filter out tuples with the same sign (they come from the same table)
			const auto rrid = li[j];
			++j;

			D_ASSERT(lrid > 0 && rrid < 0);
			// 15. add tuples w.r.t. (L1[j], L1[i]) to join result
			lsel.set_index(result_count, sel_t(+lrid - 1));
			rsel.set_index(result_count, sel_t(-rrid - 1));
			++result_count;
			if (result_count == STANDARD_VECTOR_SIZE) {
				// out of space!
				return result_count;
			}
		}
		++i;

		if (!(this->*next_row_func)()) {
			break;
		}
	}

	return result_count;
}

class IEJoinLocalSourceState;

class IEJoinGlobalSourceState : public GlobalSourceState {
public:
	IEJoinGlobalSourceState(const PhysicalIEJoin &op, IEJoinGlobalState &gsink)
	    : op(op), gsink(gsink), stage(IEJoinSourceStage::INIT), next_pair(0), completed(0), left_outers(0),
	      next_left(0), right_outers(0), next_right(0) {
		auto &left_table = *gsink.tables[0];
		auto &right_table = *gsink.tables[1];

		left_blocks = left_table.BlockCount();
		left_ranges = (left_blocks + left_per_thread - 1) / left_per_thread;

		right_blocks = right_table.BlockCount();
		right_ranges = (right_blocks + right_per_thread - 1) / right_per_thread;

		pair_count = left_ranges * right_ranges;
	}

	void Initialize();
	bool TryPrepareNextStage();
	bool AssignTask(ExecutionContext &context, IEJoinLocalSourceState &lstate);

public:
	idx_t MaxThreads() override;

	ProgressData GetProgress() const;

	const PhysicalIEJoin &op;
	IEJoinGlobalState &gsink;

	atomic<IEJoinSourceStage> stage;

	// Join queue state
	idx_t left_blocks = 0;
	idx_t left_ranges = 0;
	const idx_t left_per_thread = 1024;
	idx_t right_blocks = 0;
	idx_t right_ranges = 0;
	const idx_t right_per_thread = 1024;
	idx_t pair_count;
	atomic<size_t> next_pair;
	atomic<size_t> completed;

	// Outer joins
	atomic<idx_t> left_outers;
	atomic<idx_t> next_left;

	atomic<idx_t> right_outers;
	atomic<idx_t> next_right;
};

class IEJoinLocalSourceState : public LocalSourceState {
public:
	IEJoinLocalSourceState(ClientContext &client, IEJoinGlobalSourceState &gsource)
	    : gsource(gsource), lsel(STANDARD_VECTOR_SIZE), rsel(STANDARD_VECTOR_SIZE), true_sel(STANDARD_VECTOR_SIZE),
	      left_executor(client), right_executor(client), left_matches(nullptr), right_matches(nullptr)

	{
		auto &op = gsource.op;
		auto &allocator = Allocator::Get(client);
		unprojected.InitializeEmpty(op.unprojected_types);
		lpayload.Initialize(allocator, op.children[0].get().GetTypes());
		rpayload.Initialize(allocator, op.children[1].get().GetTypes());

		auto &ie_sink = op.sink_state->Cast<IEJoinGlobalState>();
		auto &left_table = *ie_sink.tables[0];
		auto &right_table = *ie_sink.tables[1];

		left_iterator = left_table.CreateIteratorState();
		right_iterator = right_table.CreateIteratorState();

		left_table.InitializePayloadState(left_chunk_state);
		right_table.InitializePayloadState(right_chunk_state);

		left_scan_state = left_table.CreateScanState(client);
		right_scan_state = right_table.CreateScanState(client);

		if (op.conditions.size() < 3) {
			return;
		}

		vector<LogicalType> left_types;
		vector<LogicalType> right_types;
		for (idx_t i = 2; i < op.conditions.size(); ++i) {
			const auto &cond = op.conditions[i];

			left_types.push_back(cond.left->return_type);
			left_executor.AddExpression(*cond.left);

			right_types.push_back(cond.left->return_type);
			right_executor.AddExpression(*cond.right);
		}

		left_keys.Initialize(allocator, left_types);
		right_keys.Initialize(allocator, right_types);
	}

	idx_t SelectOuterRows(bool *matches) {
		idx_t count = 0;
		for (; outer_idx < outer_count; ++outer_idx) {
			if (!matches[outer_idx]) {
				true_sel.set_index(count++, outer_idx);
				if (count >= STANDARD_VECTOR_SIZE) {
					outer_idx++;
					break;
				}
			}
		}

		return count;
	}

	//	Are we executing a task?
	bool TaskFinished() const {
		return !joiner && !left_matches && !right_matches;
	}

	// resolve joins that can potentially output N*M elements (INNER, LEFT, FULL)
	void ResolveComplexJoin(ExecutionContext &context, DataChunk &result);
	//	Resolve left join results
	void ExecuteLeftTask(ExecutionContext &context, DataChunk &result);
	//	Resolve right join results
	void ExecuteRightTask(ExecutionContext &context, DataChunk &result);
	//	Execute the current task
	void ExecuteTask(ExecutionContext &context, DataChunk &result);

	IEJoinGlobalSourceState &gsource;

	// Joining
	unique_ptr<IEJoinUnion> joiner;

	idx_t left_base;
	idx_t left_block_index;
	unique_ptr<ExternalBlockIteratorState> left_iterator;
	TupleDataChunkState left_chunk_state;
	SelectionVector lsel;
	DataChunk lpayload;
	unique_ptr<SortedRunScanState> left_scan_state;

	idx_t right_base;
	idx_t right_block_index;
	unique_ptr<ExternalBlockIteratorState> right_iterator;
	TupleDataChunkState right_chunk_state;
	SelectionVector rsel;
	DataChunk rpayload;
	unique_ptr<SortedRunScanState> right_scan_state;

	// Trailing predicates
	SelectionVector true_sel;

	ExpressionExecutor left_executor;
	DataChunk left_keys;

	ExpressionExecutor right_executor;
	DataChunk right_keys;

	DataChunk unprojected;

	// Outer joins
	idx_t outer_idx;
	idx_t outer_count;
	bool *left_matches;
	bool *right_matches;
};

void IEJoinLocalSourceState::ExecuteTask(ExecutionContext &context, DataChunk &result) {
	if (joiner) {
		ResolveComplexJoin(context, result);
	} else if (left_matches != nullptr) {
		ExecuteLeftTask(context, result);
	} else if (right_matches != nullptr) {
		ExecuteRightTask(context, result);
	}
}

void IEJoinLocalSourceState::ResolveComplexJoin(ExecutionContext &context, DataChunk &result) {
	auto &op = gsource.op;
	auto &ie_sink = op.sink_state->Cast<IEJoinGlobalState>();
	const auto &conditions = op.conditions;
	joiner->Build(context, op);

	auto &chunk = unprojected;

	auto &left_table = *ie_sink.tables[0];
	const auto left_cols = op.children[0].get().GetTypes().size();

	auto &right_table = *ie_sink.tables[1];

	do {
		auto result_count = joiner->JoinComplexBlocks(lsel, rsel);
		if (result_count == 0) {
			// exhausted this pair
			joiner.reset();
			++gsource.completed;
			return;
		}

		// found matches: extract them

		left_table.Repin(*left_iterator);
		right_table.Repin(*right_iterator);

		op.SliceSortedPayload(lpayload, left_table, *left_iterator, left_chunk_state, left_block_index, lsel,
		                      result_count, *left_scan_state);
		op.SliceSortedPayload(rpayload, right_table, *right_iterator, right_chunk_state, right_block_index, rsel,
		                      result_count, *right_scan_state);

		auto sel = FlatVector::IncrementalSelectionVector();
		if (conditions.size() > 2) {
			// If there are more expressions to compute,
			// use the left and right payloads
			// to we can compute the values for comparison.
			const auto tail_cols = conditions.size() - 2;

			left_executor.SetChunk(lpayload);
			right_executor.SetChunk(rpayload);

			auto tail_count = result_count;
			auto match_sel = &true_sel;
			for (size_t cmp_idx = 0; cmp_idx < tail_cols; ++cmp_idx) {
				auto &left = left_keys.data[cmp_idx];
				left_executor.ExecuteExpression(cmp_idx, left);

				auto &right = right_keys.data[cmp_idx];
				right_executor.ExecuteExpression(cmp_idx, right);

				if (tail_count < result_count) {
					left.Slice(*sel, tail_count);
					right.Slice(*sel, tail_count);
				}
				tail_count =
				    op.SelectJoinTail(conditions[cmp_idx + 2].comparison, left, right, sel, tail_count, match_sel);
				sel = match_sel;
			}

			if (tail_count < result_count) {
				result_count = tail_count;
				lpayload.Slice(*sel, result_count);
				rpayload.Slice(*sel, result_count);
			}
		}

		//	Merge the payloads
		chunk.Reset();
		for (column_t col_idx = 0; col_idx < chunk.ColumnCount(); ++col_idx) {
			if (col_idx < left_cols) {
				chunk.data[col_idx].Reference(lpayload.data[col_idx]);
			} else {
				chunk.data[col_idx].Reference(rpayload.data[col_idx - left_cols]);
			}
		}
		chunk.SetCardinality(result_count);

		//	We need all of the data to compute other predicates,
		//	but we only return what is in the projection map
		op.ProjectResult(chunk, result);

		// found matches: mark the found matches if required
		if (left_table.found_match) {
			for (idx_t i = 0; i < result_count; i++) {
				left_table.found_match[left_base + lsel[sel->get_index(i)]] = true;
			}
		}
		if (right_table.found_match) {
			for (idx_t i = 0; i < result_count; i++) {
				right_table.found_match[right_base + rsel[sel->get_index(i)]] = true;
			}
		}
		result.Verify();
	} while (result.size() == 0);
}

void IEJoinGlobalSourceState::Initialize() {
	auto guard = Lock();
	if (stage != IEJoinSourceStage::INIT) {
		return;
	}

	// Compute the starting row for each block
	auto &left_table = *gsink.tables[0];
	const auto left_blocks = left_table.BlockCount();

	auto &right_table = *gsink.tables[1];
	const auto right_blocks = right_table.BlockCount();

	// Outer join block counts
	if (left_table.found_match) {
		left_outers = left_blocks;
	}

	if (right_table.found_match) {
		right_outers = right_blocks;
	}

	// Ready for action
	stage = IEJoinSourceStage::INNER;
}
bool IEJoinGlobalSourceState::TryPrepareNextStage() {
	//	Inside lock
	switch (stage.load()) {
	case IEJoinSourceStage::INNER:
		if (completed >= pair_count) {
			stage = IEJoinSourceStage::OUTER;
			return true;
		}
		break;
	case IEJoinSourceStage::OUTER:
		if (next_left >= left_outers && next_right >= right_outers) {
			stage = IEJoinSourceStage::DONE;
			return true;
		}
		break;
	default:
		break;
	}

	return false;
}

idx_t IEJoinGlobalSourceState::MaxThreads() {
	// We can't leverage any more threads than block pairs.
	const auto &sink_state = (op.sink_state->Cast<IEJoinGlobalState>());
	return sink_state.tables[0]->BlockCount() * sink_state.tables[1]->BlockCount();
}

bool IEJoinGlobalSourceState::AssignTask(ExecutionContext &context, IEJoinLocalSourceState &lstate) {
	auto guard = Lock();

	using ChunkRange = IEJoinUnion::ChunkRange;
	auto &left_table = *gsink.tables[0];
	auto &right_table = *gsink.tables[1];

	// Regular block
	switch (stage.load()) {
	case IEJoinSourceStage::INNER:
		if (next_pair < pair_count) {
			const auto i = next_pair++;
			const auto b1 = (i / right_ranges) * left_per_thread;
			const auto b2 = (i % right_ranges) * right_per_thread;

			ChunkRange l_range {b1, MinValue(left_blocks, b1 + left_per_thread)};
			lstate.left_block_index = l_range.first;
			lstate.left_base = left_table.BlockStart(l_range.first);

			ChunkRange r_range {b2, MinValue(right_blocks, b2 + right_per_thread)};
			lstate.right_block_index = r_range.first;
			lstate.right_base = right_table.BlockStart(r_range.first);

			lstate.joiner = make_uniq<IEJoinUnion>(left_table, l_range, right_table, r_range);
			return true;
		}
		break;
	case IEJoinSourceStage::OUTER:
		// Left outer blocks
		if (next_left < left_outers) {
			const auto l = next_left++;
			lstate.joiner = nullptr;
			lstate.left_block_index = l;
			lstate.left_base = left_table.BlockStart(l);

			lstate.left_matches = left_table.found_match.get() + lstate.left_base;
			lstate.outer_idx = 0;
			lstate.outer_count = left_table.BlockSize(l);
			return true;
		} else {
			lstate.left_matches = nullptr;
		}

		// Right outer blocks
		if (next_right < right_outers) {
			const auto r = next_right++;
			lstate.joiner = nullptr;
			lstate.right_block_index = r;
			lstate.right_base = right_table.BlockStart(r);

			lstate.right_matches = right_table.found_match.get() + lstate.right_base;
			lstate.outer_idx = 0;
			lstate.outer_count = right_table.BlockSize(r);
			return true;
		} else {
			lstate.right_matches = nullptr;
		}
		break;
	default:
		break;
	}

	return false;
}

ProgressData IEJoinGlobalSourceState::GetProgress() const {
	const auto count = pair_count + left_outers + right_outers;

	const auto l = MinValue(next_left.load(), left_outers.load());
	const auto r = MinValue(next_right.load(), right_outers.load());
	const auto returned = completed.load() + l + r;

	ProgressData res;
	if (count) {
		res.done = double(returned);
		res.total = double(count);
	} else {
		res.SetInvalid();
	}
	return res;
}
unique_ptr<GlobalSourceState> PhysicalIEJoin::GetGlobalSourceState(ClientContext &context) const {
	auto &gsink = sink_state->Cast<IEJoinGlobalState>();
	return make_uniq<IEJoinGlobalSourceState>(*this, gsink);
}

unique_ptr<LocalSourceState> PhysicalIEJoin::GetLocalSourceState(ExecutionContext &context,
                                                                 GlobalSourceState &gstate) const {
	auto &gsource = gstate.Cast<IEJoinGlobalSourceState>();
	return make_uniq<IEJoinLocalSourceState>(context.client, gsource);
}

ProgressData PhysicalIEJoin::GetProgress(ClientContext &context, GlobalSourceState &gsource_p) const {
	auto &gsource = gsource_p.Cast<IEJoinGlobalSourceState>();
	return gsource.GetProgress();
}

SourceResultType PhysicalIEJoin::GetDataInternal(ExecutionContext &context, DataChunk &result,
                                                 OperatorSourceInput &input) const {
	auto &gsource = input.global_state.Cast<IEJoinGlobalSourceState>();
	auto &lsource = input.local_state.Cast<IEJoinLocalSourceState>();

	gsource.Initialize();

	// Any call to GetData must produce tuples, otherwise the pipeline executor thinks that we're done
	// Therefore, we loop until we've produced tuples, or until the operator is actually done
	while (gsource.stage != IEJoinSourceStage::DONE && result.size() == 0) {
		if (!lsource.TaskFinished() || gsource.AssignTask(context, lsource)) {
			lsource.ExecuteTask(context, result);
		} else {
			auto guard = gsource.Lock();
			if (gsource.TryPrepareNextStage() || gsource.stage == IEJoinSourceStage::DONE) {
				gsource.UnblockTasks(guard);
			} else {
				return gsource.BlockSource(guard, input.interrupt_state);
			}
		}
	}
	return result.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

void IEJoinLocalSourceState::ExecuteLeftTask(ExecutionContext &context, DataChunk &result) {
	auto &op = gsource.op;
	auto &ie_sink = op.sink_state->Cast<IEJoinGlobalState>();

	const auto left_cols = op.children[0].get().GetTypes().size();
	auto &chunk = unprojected;

	const idx_t count = SelectOuterRows(left_matches);
	if (!count) {
		left_matches = nullptr;
		return;
	}

	auto &left_table = *ie_sink.tables[0];

	left_table.Repin(*left_iterator);
	op.SliceSortedPayload(lpayload, left_table, *left_iterator, left_chunk_state, left_block_index, true_sel, count,
	                      *left_scan_state);

	// Fill in NULLs to the right
	chunk.Reset();
	for (column_t col_idx = 0; col_idx < chunk.ColumnCount(); ++col_idx) {
		if (col_idx < left_cols) {
			chunk.data[col_idx].Reference(lpayload.data[col_idx]);
		} else {
			chunk.data[col_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(chunk.data[col_idx], true);
		}
	}

	op.ProjectResult(chunk, result);
	result.SetCardinality(count);
	result.Verify();
}

void IEJoinLocalSourceState::ExecuteRightTask(ExecutionContext &context, DataChunk &result) {
	auto &op = gsource.op;
	auto &ie_sink = op.sink_state->Cast<IEJoinGlobalState>();
	const auto left_cols = op.children[0].get().GetTypes().size();

	auto &chunk = unprojected;

	const idx_t count = SelectOuterRows(right_matches);
	if (!count) {
		right_matches = nullptr;
		return;
	}

	auto &right_table = *ie_sink.tables[1];
	auto &rsel = true_sel;

	right_table.Repin(*right_iterator);
	op.SliceSortedPayload(rpayload, right_table, *right_iterator, right_chunk_state, right_block_index, rsel, count,
	                      *right_scan_state);

	// Fill in NULLs to the left
	chunk.Reset();
	for (column_t col_idx = 0; col_idx < chunk.ColumnCount(); ++col_idx) {
		if (col_idx < left_cols) {
			chunk.data[col_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(chunk.data[col_idx], true);
		} else {
			chunk.data[col_idx].Reference(rpayload.data[col_idx - left_cols]);
		}
	}

	op.ProjectResult(chunk, result);
	result.SetCardinality(count);
	result.Verify();
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalIEJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	D_ASSERT(children.size() == 2);
	if (meta_pipeline.HasRecursiveCTE()) {
		throw NotImplementedException("IEJoins are not supported in recursive CTEs yet");
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
