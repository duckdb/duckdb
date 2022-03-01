#include "duckdb/execution/operator/join/physical_iejoin.hpp"

#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/sort/comparators.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

PhysicalIEJoin::PhysicalIEJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                               unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                               idx_t estimated_cardinality)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::IE_JOIN, move(cond), join_type, estimated_cardinality) {
	// Reorder the conditions so that ranges are at the front.
	// TODO: use stats to improve the choice?
	// TODO: Prefer fixed length types?
	auto conditions_p = std::move(conditions);
	conditions.resize(conditions_p.size());
	idx_t range_position = 0;
	idx_t other_position = conditions_p.size();
	for (idx_t i = 0; i < conditions_p.size(); ++i) {
		switch (conditions_p[i].comparison) {
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			conditions[range_position++] = std::move(conditions_p[i]);
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_DISTINCT_FROM:
			// Allowed in multi-predicate joins, but can't be first/sort.
			conditions[--other_position] = std::move(conditions_p[i]);
			break;
		default:
			// COMPARE EQUAL not supported with iejoin join
			throw NotImplementedException("Unimplemented join type for IEJoin");
		}
	}

	// IEJoin requires at least two comparisons.
	D_ASSERT(range_position > 1);

	// 1. let L1 (resp. L2) be the array of column X (resp. Y)
	D_ASSERT(conditions.size() == 2);
	lhs_orders.resize(2);
	rhs_orders.resize(2);
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
		default:
			sense = i ? OrderType::DESCENDING : OrderType::ASCENDING;
			break;
		}
		lhs_orders[i].emplace_back(BoundOrderByNode(sense, OrderByNullType::NULLS_LAST, move(left)));
		rhs_orders[i].emplace_back(BoundOrderByNode(sense, OrderByNullType::NULLS_LAST, move(right)));
	}

	for (idx_t i = 2; i < conditions.size(); ++i) {
		auto &cond = conditions[i];
		D_ASSERT(cond.left->return_type == cond.right->return_type);
		join_key_types.push_back(cond.left->return_type);
	}

	children.push_back(move(left));
	children.push_back(move(right));
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class IEJoinLocalState : public LocalSinkState {
public:
	explicit IEJoinLocalState(const vector<BoundOrderByNode> &orders, const vector<JoinCondition> &conditions)
	    : has_null(0), count(0) {
		// Initialize order clause expression executor and DataChunk
		vector<LogicalType> types;
		for (const auto &order : orders) {
			types.push_back(order.expression->return_type);
			executor.AddExpression(*order.expression);
		}
		keys.Initialize(types);

		for (const auto &cond : conditions) {
			comparisons.emplace_back(cond.comparison);
		}
	}

	//! The local sort state
	LocalSortState local_sort_state;
	//! Local copy of the sorting expression executor
	ExpressionExecutor executor;
	//! Holds a vector of incoming sorting columns
	DataChunk keys;
	//! The comparison list (for null merging)
	vector<ExpressionType> comparisons;
	//! The number of NULL values
	idx_t has_null;
	//! The total number of rows
	idx_t count;

	idx_t MergeKeyNulls();

	void Sink(DataChunk &input, GlobalSortState &global_sort_state) {
		// Initialize local state (if necessary)
		if (!local_sort_state.initialized) {
			local_sort_state.Initialize(global_sort_state, global_sort_state.buffer_manager);
		}

		// Obtain sorting columns
		keys.Reset();
		executor.Execute(input, keys);

		// Count the NULLs so we can exclude them later
		has_null += MergeKeyNulls();
		count += keys.size();

		// Sink the data into the local sort state
		if (keys.ColumnCount() > 1) {
			//	Only sort the primary key
			DataChunk join_head;
			join_head.data.emplace_back(Vector(keys.data[0]));
			join_head.SetCardinality(keys.size());

			local_sort_state.SinkChunk(join_head, input);
		} else {
			local_sort_state.SinkChunk(keys, input);
		}
	}

	void Sort(GlobalSortState &gss) {
		local_sort_state.Sort(gss, true);
	}
	void Reset() {
		has_null = 0;
		count = 0;
	}
};

idx_t IEJoinLocalState::MergeKeyNulls() {
	// Merge the validity masks of the comparison keys into the primary
	// Return the number of NULLs in the resulting chunk
	D_ASSERT(keys.ColumnCount() > 0);
	const auto count = keys.size();

	size_t all_constant = 0;
	for (auto &v : keys.data) {
		all_constant += int(v.GetVectorType() == VectorType::CONSTANT_VECTOR);
	}

	auto &primary = keys.data[0];
	if (all_constant == keys.data.size()) {
		//	Either all NULL or no NULLs
		for (auto &v : keys.data) {
			if (ConstantVector::IsNull(v)) {
				ConstantVector::SetNull(primary, true);
				return count;
			}
		}
		return 0;
	} else if (keys.ColumnCount() > 1) {
		//	Normalify the primary, as it will need to merge arbitrary validity masks
		primary.Normalify(count);
		auto &pvalidity = FlatVector::Validity(primary);
		D_ASSERT(keys.ColumnCount() == comparisons.size());
		for (size_t c = 1; c < keys.data.size(); ++c) {
			// Skip comparisons that accept NULLs
			if (comparisons[c] == ExpressionType::COMPARE_DISTINCT_FROM) {
				continue;
			}
			//	Orrify the rest, as the sort code will do this anyway.
			auto &v = keys.data[c];
			VectorData vdata;
			v.Orrify(count, vdata);
			auto &vvalidity = vdata.validity;
			if (vvalidity.AllValid()) {
				continue;
			}
			pvalidity.EnsureWritable();
			auto pmask = pvalidity.GetData();
			if (v.GetVectorType() == VectorType::FLAT_VECTOR) {
				//	Merge entire entries
				const auto entry_count = pvalidity.EntryCount(count);
				for (idx_t entry_idx = 0; entry_idx < entry_count; ++entry_idx) {
					pmask[entry_idx] &= vvalidity.GetValidityEntry(entry_idx);
				}
			}
		}
		return count - pvalidity.CountValid(count);
	} else {
		return count - VectorOperations::CountNotNull(primary, count);
	}
}

unique_ptr<LocalSinkState> PhysicalIEJoin::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<IEJoinLocalState>(rhs_orders[0], conditions);
}

class IEJoinGlobalState : public GlobalSinkState {
public:
	IEJoinGlobalState(ClientContext &context, const vector<BoundOrderByNode> &orders, RowLayout &payload_layout)
	    : global_sort_state(BufferManager::GetBufferManager(context), orders, payload_layout), has_null(0), count(0),
	      memory_per_thread(0) {
		D_ASSERT(orders.size() == 1);

		// Set external (can be force with the PRAGMA)
		auto &config = ClientConfig::GetConfig(context);
		global_sort_state.external = config.force_external;
		// Memory usage per thread should scale with max mem / num threads
		// We take 1/4th of this, to be conservative
		idx_t max_memory = global_sort_state.buffer_manager.GetMaxMemory();
		idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
		memory_per_thread = (max_memory / num_threads) / 4;
	}

	inline idx_t Count() const {
		return count;
	}

	inline idx_t BlockCount() const {
		return global_sort_state.sorted_blocks[0]->radix_sorting_data.size();
	}

	inline idx_t BlockSize(idx_t i) const {
		return global_sort_state.sorted_blocks[0]->radix_sorting_data[i].count;
	}

	inline void Combine(IEJoinLocalState &lstate) {
		global_sort_state.AddLocalState(lstate.local_sort_state);
		has_null += lstate.has_null;
		count += lstate.count;
	}

	void Print() {
		PayloadScanner scanner(global_sort_state, false);
		DataChunk chunk;
		chunk.Initialize(scanner.GetPayloadTypes());
		for (;;) {
			scanner.Scan(chunk);
			const auto count = chunk.size();
			if (!count) {
				break;
			}
			chunk.Print();
		}
	}

	GlobalSortState global_sort_state;
	//! Whether or not the RHS has NULL values
	atomic<idx_t> has_null;
	//! The total number of rows in the RHS
	atomic<idx_t> count;
	//! A bool indicating for each tuple in the RHS if they found a match (only used in FULL OUTER JOIN)
	unique_ptr<bool[]> found_match;
	//! Memory usage per thread
	idx_t memory_per_thread;
};

unique_ptr<GlobalSinkState> PhysicalIEJoin::GetGlobalSinkState(ClientContext &context) const {
	// Get the payload layout from the rhs types and tail predicates
	RowLayout rhs_layout;
	rhs_layout.Initialize(children[1]->types);
	vector<BoundOrderByNode> rhs_order;
	rhs_order.emplace_back(rhs_orders[0][0].Copy());
	return make_unique<IEJoinGlobalState>(context, rhs_order, rhs_layout);
}

SinkResultType PhysicalIEJoin::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                    DataChunk &input) const {
	auto &gstate = (IEJoinGlobalState &)gstate_p;
	auto &lstate = (IEJoinLocalState &)lstate_p;

	auto &global_sort_state = gstate.global_sort_state;
	auto &local_sort_state = lstate.local_sort_state;

	// Sink the data into the local sort state
	lstate.Sink(input, global_sort_state);

	// When sorting data reaches a certain size, we sort it
	if (local_sort_state.SizeInBytes() >= gstate.memory_per_thread) {
		local_sort_state.Sort(global_sort_state, true);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalIEJoin::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &gstate = (IEJoinGlobalState &)gstate_p;
	auto &lstate = (IEJoinLocalState &)lstate_p;
	gstate.Combine(lstate);
	auto &client_profiler = QueryProfiler::Get(context.client);

	context.thread.profiler.Flush(this, &lstate.executor, "rhs_executor", 1);
	client_profiler.Flush(context.thread.profiler);
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
class IEJoinFinalizeTask : public ExecutorTask {
public:
	IEJoinFinalizeTask(shared_ptr<Event> event_p, ClientContext &context, IEJoinGlobalState &state)
	    : ExecutorTask(context), event(move(event_p)), context(context), state(state) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		// Initialize iejoin sorted and iterate until done
		auto &global_sort_state = state.global_sort_state;
		MergeSorter iejoin_sorter(global_sort_state, BufferManager::GetBufferManager(context));
		iejoin_sorter.PerformInMergeRound();
		event->FinishTask();

		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	ClientContext &context;
	IEJoinGlobalState &state;
};

class IEJoinFinalizeEvent : public Event {
public:
	IEJoinFinalizeEvent(IEJoinGlobalState &gstate_p, Pipeline &pipeline_p)
	    : Event(pipeline_p.executor), gstate(gstate_p), pipeline(pipeline_p) {
	}

	IEJoinGlobalState &gstate;
	Pipeline &pipeline;

public:
	void Schedule() override {
		auto &context = pipeline.GetClientContext();

		// Schedule tasks equal to the number of threads, which will each iejoin multiple partitions
		auto &ts = TaskScheduler::GetScheduler(context);
		idx_t num_threads = ts.NumberOfThreads();

		vector<unique_ptr<Task>> iejoin_tasks;
		for (idx_t tnum = 0; tnum < num_threads; tnum++) {
			iejoin_tasks.push_back(make_unique<IEJoinFinalizeTask>(shared_from_this(), context, gstate));
		}
		SetTasks(move(iejoin_tasks));
	}

	void FinishEvent() override {
		auto &global_sort_state = gstate.global_sort_state;

		global_sort_state.CompleteMergeRound(true);
		if (global_sort_state.sorted_blocks.size() > 1) {
			// Multiple blocks remaining: Schedule the next round
			PhysicalIEJoin::ScheduleMergeTasks(pipeline, *this, gstate);
		}
	}
};

void PhysicalIEJoin::ScheduleMergeTasks(Pipeline &pipeline, Event &event, IEJoinGlobalState &gstate) {
	// Initialize global sort state for a round of merging
	gstate.global_sort_state.InitializeMergeRound();
	auto new_event = make_shared<IEJoinFinalizeEvent>(gstate, pipeline);
	event.InsertEvent(move(new_event));
}

SinkFinalizeType PhysicalIEJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          GlobalSinkState &gstate_p) const {
	auto &gstate = (IEJoinGlobalState &)gstate_p;
	auto &global_sort_state = gstate.global_sort_state;

	if (IsRightOuterJoin(join_type)) {
		// for FULL/RIGHT OUTER JOIN, initialize found_match to false for every tuple
		gstate.found_match = unique_ptr<bool[]>(new bool[gstate.Count()]);
		memset(gstate.found_match.get(), 0, sizeof(bool) * gstate.Count());
	}
	if (global_sort_state.sorted_blocks.empty() && EmptyResultIfRHSIsEmpty()) {
		// Empty input!
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Prepare for iejoin sort phase
	global_sort_state.PrepareMergePhase();

	// Start the iejoin phase or finish if a iejoin is not necessary
	if (global_sort_state.sorted_blocks.size() > 1) {
		PhysicalIEJoin::ScheduleMergeTasks(pipeline, event, gstate);
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
struct SBIterator {
	static int ComparisonValue(ExpressionType comparison) {
		switch (comparison) {
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
			return -1;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return 0;
		default:
			throw InternalException("Unimplemented comparison type for IEJoin!");
		}
	}

	explicit SBIterator(GlobalSortState &gss, ExpressionType comparison, idx_t entry_idx_p = 0)
	    : sort_layout(gss.sort_layout), block_count(gss.sorted_blocks[0]->radix_sorting_data.size()),
	      block_capacity(gss.block_capacity), cmp_size(sort_layout.comparison_size), entry_size(sort_layout.entry_size),
	      all_constant(sort_layout.all_constant), external(gss.external), cmp(ComparisonValue(comparison)),
	      scan(gss.buffer_manager, gss), block_ptr(nullptr), entry_ptr(nullptr) {

		scan.sb = gss.sorted_blocks[0].get();
		scan.block_idx = block_count;
		SetIndex(entry_idx_p);
	}

	inline idx_t GetIndex() const {
		return entry_idx;
	}

	inline void SetIndex(idx_t entry_idx_p) {
		const auto new_block_idx = entry_idx_p / block_capacity;
		if (new_block_idx != scan.block_idx) {
			scan.SetIndices(new_block_idx, 0);
			if (new_block_idx < block_count) {
				scan.PinRadix(scan.block_idx);
				block_ptr = scan.RadixPtr();
			}
		}

		scan.entry_idx = entry_idx_p % block_capacity;
		entry_ptr = block_ptr + scan.entry_idx * entry_size;
		entry_idx = entry_idx_p;
	}

	inline SBIterator &operator++() {
		if (++scan.entry_idx < block_capacity) {
			entry_ptr += entry_size;
			++entry_idx;
		} else {
			SetIndex(entry_idx + 1);
		}

		return *this;
	}

	inline SBIterator &operator--() {
		if (scan.entry_idx) {
			--scan.entry_idx;
			--entry_idx;
			entry_ptr -= entry_size;
		} else {
			SetIndex(entry_idx - 1);
		}

		return *this;
	}

	inline bool Compare(const SBIterator &other) const {
		int comp_res;
		if (all_constant) {
			comp_res = FastMemcmp(entry_ptr, other.entry_ptr, cmp_size);
		} else {
			comp_res = Comparators::CompareTuple(scan, other.scan, entry_ptr, other.entry_ptr, sort_layout, external);
		}

		return comp_res <= cmp;
	}

	// Fixed comparison parameters
	const SortLayout &sort_layout;
	const idx_t block_count;
	const idx_t block_capacity;
	const size_t cmp_size;
	const size_t entry_size;
	const bool all_constant;
	const bool external;
	const int cmp;

	// Iteration state
	SBScanState scan;
	idx_t entry_idx;
	data_ptr_t block_ptr;
	data_ptr_t entry_ptr;
};

struct IEJoinUnion {
	using SortedTable = IEJoinGlobalState;

	static idx_t AppendKey(SortedTable &table, ExpressionExecutor &executor, SortedTable &marked, int64_t increment,
	                       int64_t base, const idx_t block_idx);

	static void Sort(SortedTable &table) {
		auto &global_sort_state = table.global_sort_state;
		global_sort_state.PrepareMergePhase();
		while (global_sort_state.sorted_blocks.size() > 1) {
			global_sort_state.InitializeMergeRound();
			MergeSorter merge_sorter(global_sort_state, global_sort_state.buffer_manager);
			merge_sorter.PerformInMergeRound();
			global_sort_state.CompleteMergeRound(true);
		}
	}

	template <typename T>
	static vector<T> ExtractColumn(SortedTable &table, idx_t col_idx) {
		vector<T> result;
		result.reserve(table.count);

		auto &gstate = table.global_sort_state;
		auto &blocks = *gstate.sorted_blocks[0]->payload_data;
		PayloadScanner scanner(blocks, gstate, false);

		DataChunk payload;
		payload.Initialize(gstate.payload_layout.GetTypes());
		for (;;) {
			scanner.Scan(payload);
			const auto count = payload.size();
			if (!count) {
				break;
			}

			const auto data_ptr = FlatVector::GetData<T>(payload.data[col_idx]);
			result.insert(result.end(), data_ptr, data_ptr + count);
		}

		return result;
	}

	IEJoinUnion(ClientContext &context, const PhysicalIEJoin &op, SortedTable &t1, const idx_t b1, SortedTable &t2,
	            const idx_t b2);

	bool NextRow();

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

	//! Iteration state
	idx_t n;
	idx_t i;
	unique_ptr<SBIterator> op1;
	unique_ptr<SBIterator> off1;
	unique_ptr<SBIterator> op2;
	unique_ptr<SBIterator> off2;
	int64_t lrid;
};

idx_t IEJoinUnion::AppendKey(SortedTable &table, ExpressionExecutor &executor, SortedTable &marked, int64_t increment,
                             int64_t base, const idx_t block_idx) {
	LocalSortState local_sort_state;
	local_sort_state.Initialize(marked.global_sort_state, marked.global_sort_state.buffer_manager);

	// Reading
	const auto valid = table.count - table.has_null;
	auto &gstate = table.global_sort_state;
	PayloadScanner scanner(gstate, block_idx);
	auto table_idx = block_idx * gstate.block_capacity;

	DataChunk scanned;
	scanned.Initialize(scanner.GetPayloadTypes());

	// Writing
	auto types = local_sort_state.sort_layout->logical_types;
	const idx_t payload_idx = types.size();

	const auto &payload_types = local_sort_state.payload_layout->GetTypes();
	types.insert(types.end(), payload_types.begin(), payload_types.end());
	const idx_t rid_idx = types.size() - 1;

	DataChunk keys;
	DataChunk payload;
	keys.Initialize(types);

	idx_t inserted = 0;
	for (auto rid = base; table_idx < valid;) {
		scanner.Scan(scanned);

		// NULLs are at the end, so stop when we reach them
		auto scan_count = scanned.size();
		if (table_idx + scan_count > valid) {
			scan_count = valid - table_idx;
			scanned.SetCardinality(scan_count);
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
		payload.data[0].Sequence(rid, increment);
		payload.SetCardinality(scan_count);
		keys.Fuse(payload);
		rid += increment * scan_count;

		// Sort on the sort columns (which will no longer be needed)
		keys.Split(payload, payload_idx);
		local_sort_state.SinkChunk(keys, payload);
		inserted += scan_count;
		keys.Fuse(payload);

		// Flush when we have enough data
		if (local_sort_state.SizeInBytes() >= marked.memory_per_thread) {
			local_sort_state.Sort(marked.global_sort_state, true);
		}
	}
	marked.global_sort_state.AddLocalState(local_sort_state);
	marked.count += inserted;

	return inserted;
}

IEJoinUnion::IEJoinUnion(ClientContext &context, const PhysicalIEJoin &op, SortedTable &t1, const idx_t b1,
                         SortedTable &t2, const idx_t b2) {
	// input : query Q with 2 join predicates t1.X op1 t2.X' and t1.Y op2 t2.Y', tables T, T' of sizes m and n resp.
	// output: a list of tuple pairs (ti , tj)
	// Note that T/T' are already sorted on X/X' and contain the payload data
	// We only join the two block numbers and use the sizes of the blocks as the counts

	// 1. let L1 (resp. L2) be the array of column X (resp. Y )
	const auto &order1 = op.lhs_orders[0][0];
	const auto &order2 = op.lhs_orders[1][0];

	// 2. if (op1 ∈ {>, ≥}) sort L1 in descending order
	// 3. else if (op1 ∈ {<, ≤}) sort L1 in ascending order

	// For the union algorithm, we make a unified table with the keys and the rids as the payload:
	//		X/X', Y/Y', R/R'/Li
	// The first position is the sort key.
	vector<LogicalType> types;
	types.emplace_back(order2.expression->return_type);
	types.emplace_back(LogicalType::BIGINT);
	RowLayout payload_layout;
	payload_layout.Initialize(types);

	// Sort on the first expression
	auto ref = make_unique<BoundReferenceExpression>(order1.expression->return_type, 0);
	vector<BoundOrderByNode> orders;
	orders.emplace_back(BoundOrderByNode(order1.type, order1.null_order, move(ref)));

	l1 = make_unique<SortedTable>(context, orders, payload_layout);

	// LHS has positive rids
	ExpressionExecutor l_executor;
	l_executor.AddExpression(*order1.expression);
	l_executor.AddExpression(*order2.expression);
	AppendKey(t1, l_executor, *l1, 1, 1, b1);

	// RHS has negative rids
	ExpressionExecutor r_executor;
	r_executor.AddExpression(*op.rhs_orders[0][0].expression);
	r_executor.AddExpression(*op.rhs_orders[1][0].expression);
	AppendKey(t2, r_executor, *l1, -1, -1, b2);

	Sort(*l1);

	const auto &cmp1 = op.conditions[0].comparison;
	op1 = make_unique<SBIterator>(l1->global_sort_state, cmp1);
	off1 = make_unique<SBIterator>(l1->global_sort_state, cmp1);

	// We don't actually need the L1 column, just its sort key, which is in the sort blocks
	li = ExtractColumn<int64_t>(*l1, types.size() - 1);

	// 4. if (op2 ∈ {>, ≥}) sort L2 in ascending order
	// 5. else if (op2 ∈ {<, ≤}) sort L2 in descending order

	// We sort on Y/Y' to obtain the sort keys and the permutation array.
	// For this we just need a two-column table of Y, P
	types.clear();
	types.emplace_back(LogicalType::BIGINT);
	payload_layout.Initialize(types);

	// Sort on the first expression
	orders.clear();
	ref = make_unique<BoundReferenceExpression>(order2.expression->return_type, 0);
	orders.emplace_back(BoundOrderByNode(order2.type, order2.null_order, move(ref)));

	ExpressionExecutor executor;
	executor.AddExpression(*orders[0].expression);

	l2 = make_unique<SortedTable>(context, orders, payload_layout);
	for (idx_t base = 0, block_idx = 0; block_idx < l1->BlockCount(); ++block_idx) {
		base += AppendKey(*l1, executor, *l2, 1, base, block_idx);
	}

	Sort(*l2);

	// We don't actually need the L2 column, just its sort key, which is in the sort blocks

	// 6. compute the permutation array P of L2 w.r.t. L1
	p = ExtractColumn<idx_t>(*l2, types.size() - 1);

	// 7. initialize bit-array B (|B| = n), and set all bits to 0
	n = l2->count.load();
	bit_array.resize(ValidityMask::EntryCount(n), 0);
	bit_mask.Initialize(bit_array.data());

	// 11. for(i←1 to n) do
	const auto &cmp2 = op.conditions[1].comparison;
	op2 = make_unique<SBIterator>(l2->global_sort_state, cmp2);
	off2 = make_unique<SBIterator>(l2->global_sort_state, cmp2);
	i = 0;
	(void)NextRow();
}

bool IEJoinUnion::NextRow() {
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
			if (!off2->Compare(*op2)) {
				break;
			}
			bit_mask.SetValid(p[off2->GetIndex()]);
		}

		// 9.  if (op1 ∈ {≤,≥} and op2 ∈ {≤,≥}) eqOff = 0
		// 10. else eqOff = 1
		// No, because there could be more than one equal value.
		// Scan the neighborhood instead
		op1->SetIndex(pos);
		off1->SetIndex(pos);
		for (; off1->GetIndex() > 0 and op1->Compare(*off1); --(*off1)) {
			continue;
		}
		for (; off1->GetIndex() < n and !op1->Compare(*off1); ++(*off1)) {
			continue;
		}

		return true;
	}
	return false;
}

idx_t IEJoinUnion::JoinComplexBlocks(SelectionVector &lsel, SelectionVector &rsel) {
	// 8. initialize join result as an empty list for tuple pairs
	idx_t result_count = 0;

	// 11. for(i←1 to n) do
	for (;;) {
		// 13. for (j ← pos+eqOff to n) do
		for (;;) {
			// 14. if B[j] = 1 then
			auto j = off1->GetIndex();
			for (; j < n; ++j) {
				if (bit_mask.RowIsValidUnsafe(j)) {
					break;
				}
			}
			if (j >= n) {
				break;
			}

			// Filter out tuples with the same sign (they come from the same table)
			const auto rrid = li[j];

			// 15. add tuples w.r.t. (L1[j], L1[i]) to join result
			if (lrid > 0 and rrid < 0) {
				lsel.set_index(result_count, sel_t(+lrid - 1));
				rsel.set_index(result_count, sel_t(-rrid - 1));
				++result_count;
				if (result_count == STANDARD_VECTOR_SIZE) {
					// out of space!
					return result_count;
				}
			}

			off1->SetIndex(j + 1);
		}
		++i;

		if (!NextRow()) {
			break;
		}
	}

	return result_count;
}

class IEJoinState : public OperatorState {
public:
	explicit IEJoinState(const vector<LogicalType> &types) : first_fetch(true), finished(false), right_block_index(0) {
		left_payload.Initialize(types);
	};

	bool first_fetch;
	bool finished;

	unique_ptr<IEJoinGlobalState> left_table;
	DataChunk left_payload;

	//! The current right chunk being joined
	idx_t right_block_index;
	//! The absolute rid for the start of the chunk
	idx_t right_base;

	unique_ptr<IEJoinUnion> joiner;
};

unique_ptr<OperatorState> PhysicalIEJoin::GetOperatorState(ClientContext &context) const {
	return make_unique<IEJoinState>(children[0]->types);
}

static idx_t SliceSortedPayload(DataChunk &payload, GlobalSortState &state, const idx_t block_idx,
                                const idx_t entry_idx, SelectionVector &result, const idx_t result_count,
                                const idx_t left_cols = 0) {
	// There should only be one sorted block if they have been sorted
	D_ASSERT(state.sorted_blocks.size() == 1);
	SBScanState read_state(state.buffer_manager, state);
	read_state.sb = state.sorted_blocks[0].get();
	auto &sorted_data = *read_state.sb->payload_data;

	// We have to create pointers for the entire block
	// because unswizzle works on ranges not selections.
	const auto first_idx = result.get_index(0);
	read_state.SetIndices(block_idx, first_idx);
	read_state.PinData(sorted_data);
	const auto data_ptr = read_state.DataPtr(sorted_data);

	// Set up a batch of pointers to scan data from
	Vector addresses(LogicalType::POINTER, result_count);
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);

	// Set up the data pointers for the values that are actually referenced
	// and normalise the selection vector to zero
	data_ptr_t row_ptr = data_ptr;
	const idx_t &row_width = sorted_data.layout.GetRowWidth();

	auto prev_idx = first_idx;
	result.set_index(0, 0);
	idx_t addr_count = 0;
	data_pointers[addr_count++] = row_ptr;
	for (idx_t i = 1; i < result_count; ++i) {
		const auto row_idx = result.get_index(i);
		result.set_index(i, row_idx - first_idx);
		if (row_idx == prev_idx) {
			continue;
		}
		row_ptr += (row_idx - prev_idx) * row_width;
		data_pointers[addr_count++] = row_ptr;
		prev_idx = row_idx;
	}
	// Unswizzle the offsets back to pointers (if needed)
	if (!sorted_data.layout.AllConstant() && state.external) {
		const auto next = prev_idx + 1;
		RowOperations::UnswizzlePointers(sorted_data.layout, data_ptr, read_state.payload_heap_handle->Ptr(), next);
	}

	// Deserialize the payload data
	auto sel = FlatVector::IncrementalSelectionVector();
	for (idx_t col_idx = 0; col_idx < sorted_data.layout.ColumnCount(); col_idx++) {
		const auto col_offset = sorted_data.layout.GetOffsets()[col_idx];
		auto &col = payload.data[left_cols + col_idx];
		RowOperations::Gather(addresses, *sel, col, *sel, addr_count, col_offset, col_idx);
		col.Slice(result, result_count);
	}

	return first_idx;
}

void PhysicalIEJoin::ResolveSimpleJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                       OperatorState &state_p) const {
	throw NotImplementedException("Unimplemented join type for IEJoin!");
}

OperatorResultType PhysicalIEJoin::ResolveComplexJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                      OperatorState &state_p) const {
	auto &state = (IEJoinState &)state_p;
	auto &right_table = (IEJoinGlobalState &)*sink_state;

	const auto left_cols = input.ColumnCount();
	do {
		if (state.first_fetch) {
			RowLayout lhs_layout;
			lhs_layout.Initialize(input.GetTypes());
			state.left_table = make_unique<IEJoinGlobalState>(context.client, lhs_orders[0], lhs_layout);
			auto &gss = state.left_table->global_sort_state;

			auto local_left = make_unique<IEJoinLocalState>(lhs_orders[0], conditions);
			local_left->Sink(input, gss);
			local_left->Sort(gss);
			state.left_table->Combine(*local_left);
			IEJoinUnion::Sort(*state.left_table);

			// Extract the sorted LHS
			PayloadScanner scanner(gss, false);
			scanner.Scan(state.left_payload);

			state.right_base = 0;
			state.right_block_index = 0;
			state.joiner = make_unique<IEJoinUnion>(context.client, *this, *state.left_table, 0, right_table, 0);

			state.first_fetch = false;
			state.finished = false;
		}

		if (state.finished) {
			if (IsLeftOuterJoin(join_type)) {
				// left join: before we move to the next chunk, see if we need to output any vectors that didn't
				// have a match found
				PhysicalJoin::ConstructLeftJoinResult(state.left_payload, chunk, state.left_table->found_match.get());
				memset(state.left_table->found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
			}
			state.first_fetch = true;
			state.finished = false;
			return OperatorResultType::NEED_MORE_INPUT;
		}

		SelectionVector lsel(STANDARD_VECTOR_SIZE);
		SelectionVector rsel(STANDARD_VECTOR_SIZE);
		auto result_count = state.joiner->JoinComplexBlocks(lsel, rsel);
		if (result_count == 0) {
			// exhausted this chunk on the right side
			// move to the next right chunk
			state.right_base += right_table.BlockSize(state.right_block_index);
			++state.right_block_index;
			if (state.right_block_index < right_table.BlockCount()) {
				state.joiner = make_unique<IEJoinUnion>(context.client, *this, *state.left_table, 0, right_table,
				                                        state.right_block_index);
			} else {
				state.finished = true;
			}
		} else {
			// found matches: extract them
			chunk.Reset();
			for (idx_t c = 0; c < state.left_payload.ColumnCount(); ++c) {
				chunk.data[c].Slice(state.left_payload.data[c], lsel, result_count);
			}
			const auto first_idx = SliceSortedPayload(chunk, right_table.global_sort_state, state.right_block_index, 0,
			                                          rsel, result_count, left_cols);
			chunk.SetCardinality(result_count);

			auto sel = FlatVector::IncrementalSelectionVector();
			// TODO: extra predicates

			// found matches: mark the found matches if required
			auto &left_table = *state.left_table;
			if (left_table.found_match) {
				for (idx_t i = 0; i < result_count; i++) {
					left_table.found_match[lsel[sel->get_index(i)]] = true;
				}
			}
			if (right_table.found_match) {
				//	Absolute position of the block + start position inside that block
				const idx_t base_index = state.right_base + first_idx;
				for (idx_t i = 0; i < result_count; i++) {
					right_table.found_match[base_index + rsel[sel->get_index(i)]] = true;
				}
			}
			chunk.Verify();
		}
	} while (chunk.size() == 0);

	return OperatorResultType::HAVE_MORE_OUTPUT;
}

OperatorResultType PhysicalIEJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                           OperatorState &state) const {
	auto &gstate = (IEJoinGlobalState &)*sink_state;

	if (gstate.count == 0) {
		// empty RHS
		if (!EmptyResultIfRHSIsEmpty()) {
			ConstructEmptyJoinResult(join_type, gstate.has_null, input, chunk);
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			return OperatorResultType::FINISHED;
		}
	}

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
		throw NotImplementedException("Unimplemented type for piecewise iejoin loop join!");
	}
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class IEJoinScanState : public GlobalSourceState {
public:
	explicit IEJoinScanState(const PhysicalIEJoin &op) : op(op), right_outer_position(0) {
	}

	mutex lock;
	const PhysicalIEJoin &op;
	unique_ptr<PayloadScanner> scanner;
	idx_t right_outer_position;

public:
	idx_t MaxThreads() override {
		auto &sink = (IEJoinGlobalState &)*op.sink_state;
		return sink.Count() / (STANDARD_VECTOR_SIZE * idx_t(10));
	}
};

unique_ptr<GlobalSourceState> PhysicalIEJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<IEJoinScanState>(*this);
}

void PhysicalIEJoin::GetData(ExecutionContext &context, DataChunk &result, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	D_ASSERT(IsRightOuterJoin(join_type));
	// check if we need to scan any unmatched tuples from the RHS for the full/right outer join
	auto &sink = (IEJoinGlobalState &)*sink_state;
	auto &state = (IEJoinScanState &)gstate;

	lock_guard<mutex> l(state.lock);
	if (!state.scanner) {
		// Initialize scanner (if not yet initialized)
		auto &sort_state = sink.global_sort_state;
		if (sort_state.sorted_blocks.empty()) {
			return;
		}
		state.scanner = make_unique<PayloadScanner>(*sort_state.sorted_blocks[0]->payload_data, sort_state);
	}

	// if the LHS is exhausted in a FULL/RIGHT OUTER JOIN, we scan the found_match for any chunks we
	// still need to output
	const auto found_match = sink.found_match.get();

	// ConstructFullOuterJoinResult(sink.rhs_found_match.get(), sink.right_chunks, chunk, state.right_outer_position);
	DataChunk rhs_chunk;
	rhs_chunk.Initialize(sink.global_sort_state.payload_layout.GetTypes());
	SelectionVector rsel(STANDARD_VECTOR_SIZE);
	for (;;) {
		// Read the next sorted chunk
		state.scanner->Scan(rhs_chunk);

		const auto count = rhs_chunk.size();
		if (count == 0) {
			return;
		}

		idx_t result_count = 0;
		// figure out which tuples didn't find a match in the RHS
		for (idx_t i = 0; i < count; i++) {
			if (!found_match[state.right_outer_position + i]) {
				rsel.set_index(result_count++, i);
			}
		}
		state.right_outer_position += count;

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
