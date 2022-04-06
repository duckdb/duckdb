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

#include <thread>

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
	D_ASSERT(conditions.size() >= 2);
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
	explicit IEJoinLocalState(const vector<JoinCondition> &conditions, const idx_t child) : has_null(0), count(0) {
		// Initialize order clause expression executor and key DataChunk
		vector<LogicalType> types;
		for (const auto &cond : conditions) {
			comparisons.emplace_back(cond.comparison);

			const auto &expr = child ? cond.right : cond.left;
			executor.AddExpression(*expr);

			types.push_back(expr->return_type);
		}
		keys.Initialize(types);
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
		D_ASSERT(keys.ColumnCount() > 1);
		//	Only sort the primary key
		DataChunk join_head;
		join_head.data.emplace_back(Vector(keys.data[0]));
		join_head.SetCardinality(keys.size());

		local_sort_state.SinkChunk(join_head, input);
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

class IEJoinSortedTable {
public:
	IEJoinSortedTable(ClientContext &context, const vector<BoundOrderByNode> &orders, RowLayout &payload_layout)
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
		if (global_sort_state.sorted_blocks.empty()) {
			return 0;
		}
		D_ASSERT(global_sort_state.sorted_blocks.size() == 1);
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

	inline void IntializeMatches() {
		found_match = unique_ptr<bool[]>(new bool[Count()]);
		memset(found_match.get(), 0, sizeof(bool) * Count());
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

class IEJoinGlobalState : public GlobalSinkState {
public:
	IEJoinGlobalState(ClientContext &context, const PhysicalIEJoin &op) : child(0) {
		tables.resize(2);
		RowLayout lhs_layout;
		lhs_layout.Initialize(op.children[0]->types);
		vector<BoundOrderByNode> lhs_order;
		lhs_order.emplace_back(op.lhs_orders[0][0].Copy());
		tables[0] = make_unique<IEJoinSortedTable>(context, lhs_order, lhs_layout);

		RowLayout rhs_layout;
		rhs_layout.Initialize(op.children[1]->types);
		vector<BoundOrderByNode> rhs_order;
		rhs_order.emplace_back(op.rhs_orders[0][0].Copy());
		tables[1] = make_unique<IEJoinSortedTable>(context, rhs_order, rhs_layout);
	}

	IEJoinGlobalState(IEJoinGlobalState &prev)
	    : GlobalSinkState(prev), tables(move(prev.tables)), child(prev.child + 1) {
	}

	void Sink(DataChunk &input, IEJoinLocalState &lstate) {
		auto &table = *tables[child];
		auto &global_sort_state = table.global_sort_state;
		auto &local_sort_state = lstate.local_sort_state;

		// Sink the data into the local sort state
		lstate.Sink(input, global_sort_state);

		// When sorting data reaches a certain size, we sort it
		if (local_sort_state.SizeInBytes() >= table.memory_per_thread) {
			local_sort_state.Sort(global_sort_state, true);
		}
	}

	vector<unique_ptr<IEJoinSortedTable>> tables;
	size_t child;
};

unique_ptr<GlobalSinkState> PhysicalIEJoin::GetGlobalSinkState(ClientContext &context) const {
	D_ASSERT(!sink_state);
	return make_unique<IEJoinGlobalState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalIEJoin::GetLocalSinkState(ExecutionContext &context) const {
	idx_t sink_child = 0;
	if (sink_state) {
		const auto &ie_sink = (IEJoinGlobalState &)*sink_state;
		sink_child = ie_sink.child;
	}
	return make_unique<IEJoinLocalState>(conditions, sink_child);
}

SinkResultType PhysicalIEJoin::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                    DataChunk &input) const {
	auto &gstate = (IEJoinGlobalState &)gstate_p;
	auto &lstate = (IEJoinLocalState &)lstate_p;

	gstate.Sink(input, lstate);

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalIEJoin::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &gstate = (IEJoinGlobalState &)gstate_p;
	auto &lstate = (IEJoinLocalState &)lstate_p;
	gstate.tables[gstate.child]->Combine(lstate);
	auto &client_profiler = QueryProfiler::Get(context.client);

	context.thread.profiler.Flush(this, &lstate.executor, gstate.child ? "rhs_executor" : "lhs_executor", 1);
	client_profiler.Flush(context.thread.profiler);
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
class IEJoinFinalizeTask : public ExecutorTask {
public:
	IEJoinFinalizeTask(shared_ptr<Event> event_p, ClientContext &context, IEJoinSortedTable &table)
	    : ExecutorTask(context), event(move(event_p)), context(context), table(table) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		// Initialize iejoin sorted and iterate until done
		auto &global_sort_state = table.global_sort_state;
		MergeSorter merge_sorter(global_sort_state, BufferManager::GetBufferManager(context));
		merge_sorter.PerformInMergeRound();
		event->FinishTask();

		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	ClientContext &context;
	IEJoinSortedTable &table;
};

class IEJoinFinalizeEvent : public Event {
public:
	IEJoinFinalizeEvent(IEJoinSortedTable &table_p, Pipeline &pipeline_p)
	    : Event(pipeline_p.executor), table(table_p), pipeline(pipeline_p) {
	}

	IEJoinSortedTable &table;
	Pipeline &pipeline;

public:
	void Schedule() override {
		auto &context = pipeline.GetClientContext();

		// Schedule tasks equal to the number of threads, which will each iejoin multiple partitions
		auto &ts = TaskScheduler::GetScheduler(context);
		idx_t num_threads = ts.NumberOfThreads();

		vector<unique_ptr<Task>> iejoin_tasks;
		for (idx_t tnum = 0; tnum < num_threads; tnum++) {
			iejoin_tasks.push_back(make_unique<IEJoinFinalizeTask>(shared_from_this(), context, table));
		}
		SetTasks(move(iejoin_tasks));
	}

	void FinishEvent() override {
		auto &global_sort_state = table.global_sort_state;

		global_sort_state.CompleteMergeRound(true);
		if (global_sort_state.sorted_blocks.size() > 1) {
			// Multiple blocks remaining: Schedule the next round
			PhysicalIEJoin::ScheduleMergeTasks(pipeline, *this, table);
		}
	}
};

void PhysicalIEJoin::ScheduleMergeTasks(Pipeline &pipeline, Event &event, IEJoinSortedTable &table) {
	// Initialize global sort state for a round of merging
	table.global_sort_state.InitializeMergeRound();
	auto new_event = make_shared<IEJoinFinalizeEvent>(table, pipeline);
	event.InsertEvent(move(new_event));
}

SinkFinalizeType PhysicalIEJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          GlobalSinkState &gstate_p) const {
	auto &gstate = (IEJoinGlobalState &)gstate_p;
	auto &table = *gstate.tables[gstate.child];
	auto &global_sort_state = table.global_sort_state;

	if ((gstate.child == 1 && IsRightOuterJoin(join_type)) || (gstate.child == 0 && IsLeftOuterJoin(join_type))) {
		// for FULL/LEFT/RIGHT OUTER JOIN, initialize found_match to false for every tuple
		table.IntializeMatches();
	}
	if (gstate.child == 1 && global_sort_state.sorted_blocks.empty() && EmptyResultIfRHSIsEmpty()) {
		// Empty input!
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Prepare for child sort phase
	global_sort_state.PrepareMergePhase();

	// Start the iejoin phase or finish if a iejoin is not necessary
	if (global_sort_state.sorted_blocks.size() > 1) {
		PhysicalIEJoin::ScheduleMergeTasks(pipeline, event, table);
	}

	++gstate.child;

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
				if (!all_constant) {
					scan.PinData(*scan.sb->blob_sorting_data);
				}
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
	using SortedTable = IEJoinSortedTable;

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

	//! Bloom Filter
	static constexpr idx_t BLOOM_CHUNK_BITS = 1024;
	idx_t bloom_count;
	vector<validity_t> bloom_array;
	ValidityMask bloom_filter;

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
                         SortedTable &t2, const idx_t b2)
    : n(0), i(0) {
	// input : query Q with 2 join predicates t1.X op1 t2.X' and t1.Y op2 t2.Y', tables T, T' of sizes m and n resp.
	// output: a list of tuple pairs (ti , tj)
	// Note that T/T' are already sorted on X/X' and contain the payload data
	// We only join the two block numbers and use the sizes of the blocks as the counts

	// 0. Filter out tables with no overlap
	if (!t1.BlockSize(b1) || !t2.BlockSize(b2)) {
		return;
	}

	const auto &cmp1 = op.conditions[0].comparison;
	SBIterator bounds1(t1.global_sort_state, cmp1);
	SBIterator bounds2(t2.global_sort_state, cmp1);

	// t1.X[0] op1 t2.X'[-1]
	bounds1.SetIndex(bounds1.block_capacity * b1);
	bounds2.SetIndex(bounds2.block_capacity * b2 + t2.BlockSize(b2) - 1);
	if (!bounds1.Compare(bounds2)) {
		return;
	}

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

	// Bloom filter
	bloom_count = (n + (BLOOM_CHUNK_BITS - 1)) / BLOOM_CHUNK_BITS;
	bloom_array.resize(ValidityMask::EntryCount(bloom_count), 0);
	bloom_filter.Initialize(bloom_array.data());

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
			const auto p2 = p[off2->GetIndex()];
			bit_mask.SetValid(p2);
			bloom_filter.SetValid(p2 / BLOOM_CHUNK_BITS);
		}

		// 9.  if (op1 ∈ {≤,≥} and op2 ∈ {≤,≥}) eqOff = 0
		// 10. else eqOff = 1
		// No, because there could be more than one equal value.
		// Scan the neighborhood instead
		op1->SetIndex(pos);
		off1->SetIndex(pos);
		for (; off1->GetIndex() > 0 && op1->Compare(*off1); --(*off1)) {
			continue;
		}
		for (; off1->GetIndex() < n && !op1->Compare(*off1); ++(*off1)) {
			continue;
		}

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
			auto j = off1->GetIndex();

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
			off1->SetIndex(j + 1);

			// Filter out tuples with the same sign (they come from the same table)
			const auto rrid = li[j];

			// 15. add tuples w.r.t. (L1[j], L1[i]) to join result
			if (lrid > 0 && rrid < 0) {
				lsel.set_index(result_count, sel_t(+lrid - 1));
				rsel.set_index(result_count, sel_t(-rrid - 1));
				++result_count;
				if (result_count == STANDARD_VECTOR_SIZE) {
					// out of space!
					return result_count;
				}
			}
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
	explicit IEJoinState(const PhysicalIEJoin &op) : local_left(op.conditions, 0) {};

	IEJoinLocalState local_left;
};

static void SliceSortedPayload(DataChunk &payload, GlobalSortState &state, const idx_t block_idx,
                               const SelectionVector &result, const idx_t result_count, const idx_t left_cols = 0) {
	// There should only be one sorted block if they have been sorted
	D_ASSERT(state.sorted_blocks.size() == 1);
	SBScanState read_state(state.buffer_manager, state);
	read_state.sb = state.sorted_blocks[0].get();
	auto &sorted_data = *read_state.sb->payload_data;

	read_state.SetIndices(block_idx, 0);
	read_state.PinData(sorted_data);
	const auto data_ptr = read_state.DataPtr(sorted_data);

	// Set up a batch of pointers to scan data from
	Vector addresses(LogicalType::POINTER, result_count);
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);

	// Set up the data pointers for the values that are actually referenced
	const idx_t &row_width = sorted_data.layout.GetRowWidth();

	auto prev_idx = result.get_index(0);
	SelectionVector gsel(result_count);
	idx_t addr_count = 0;
	gsel.set_index(0, addr_count);
	data_pointers[addr_count] = data_ptr + prev_idx * row_width;
	for (idx_t i = 1; i < result_count; ++i) {
		const auto row_idx = result.get_index(i);
		if (row_idx != prev_idx) {
			data_pointers[++addr_count] = data_ptr + row_idx * row_width;
			prev_idx = row_idx;
		}
		gsel.set_index(i, addr_count);
	}
	++addr_count;

	// Unswizzle the offsets back to pointers (if needed)
	if (!sorted_data.layout.AllConstant() && state.external) {
		RowOperations::UnswizzlePointers(sorted_data.layout, data_ptr, read_state.payload_heap_handle->Ptr(),
		                                 addr_count);
	}

	// Deserialize the payload data
	auto sel = FlatVector::IncrementalSelectionVector();
	for (idx_t col_idx = 0; col_idx < sorted_data.layout.ColumnCount(); col_idx++) {
		const auto col_offset = sorted_data.layout.GetOffsets()[col_idx];
		auto &col = payload.data[left_cols + col_idx];
		RowOperations::Gather(addresses, *sel, col, *sel, addr_count, col_offset, col_idx);
		col.Slice(gsel, result_count);
	}
}

class IEJoinLocalSourceState : public LocalSourceState {
public:
	explicit IEJoinLocalSourceState(const PhysicalIEJoin &op)
	    : op(op), true_sel(STANDARD_VECTOR_SIZE), left_matches(nullptr), right_matches(nullptr) {

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

		left_keys.Initialize(left_types);
		right_keys.Initialize(right_types);
	}

	idx_t SelectJoinTail(const ExpressionType &condition, Vector &left, Vector &right, const SelectionVector *sel,
	                     idx_t count);

	idx_t SelectOuterRows(bool *matches) {
		idx_t count = 0;
		for (; outer_idx < outer_count; ++outer_idx) {
			if (!matches[outer_idx]) {
				true_sel.set_index(count++, outer_idx);
				if (count >= STANDARD_VECTOR_SIZE) {
					break;
				}
			}
		}

		return count;
	}

	const PhysicalIEJoin &op;

	// Joining
	unique_ptr<IEJoinUnion> joiner;

	idx_t left_base;
	idx_t left_block_index;

	idx_t right_base;
	idx_t right_block_index;

	// Trailing predicates
	SelectionVector true_sel;

	ExpressionExecutor left_executor;
	DataChunk left_keys;

	ExpressionExecutor right_executor;
	DataChunk right_keys;

	// Outer joins
	idx_t outer_idx;
	idx_t outer_count;
	bool *left_matches;
	bool *right_matches;
};

idx_t IEJoinLocalSourceState::SelectJoinTail(const ExpressionType &condition, Vector &left, Vector &right,
                                             const SelectionVector *sel, idx_t count) {
	switch (condition) {
	case ExpressionType::COMPARE_NOTEQUAL:
		return VectorOperations::NotEquals(left, right, sel, count, &true_sel, nullptr);
	case ExpressionType::COMPARE_LESSTHAN:
		return VectorOperations::LessThan(left, right, sel, count, &true_sel, nullptr);
	case ExpressionType::COMPARE_GREATERTHAN:
		return VectorOperations::GreaterThan(left, right, sel, count, &true_sel, nullptr);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return VectorOperations::LessThanEquals(left, right, sel, count, &true_sel, nullptr);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return VectorOperations::GreaterThanEquals(left, right, sel, count, &true_sel, nullptr);
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return VectorOperations::DistinctFrom(left, right, sel, count, &true_sel, nullptr);
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
	case ExpressionType::COMPARE_EQUAL:
	default:
		throw InternalException("Unsupported comparison type for PhysicalIEJoin");
	}

	return count;
}

void PhysicalIEJoin::ResolveComplexJoin(ExecutionContext &context, DataChunk &chunk, LocalSourceState &state_p) const {
	auto &state = (IEJoinLocalSourceState &)state_p;
	auto &ie_sink = (IEJoinGlobalState &)*sink_state;
	auto &left_table = *ie_sink.tables[0];
	auto &right_table = *ie_sink.tables[1];

	const auto left_cols = children[0]->GetTypes().size();
	do {
		SelectionVector lsel(STANDARD_VECTOR_SIZE);
		SelectionVector rsel(STANDARD_VECTOR_SIZE);
		auto result_count = state.joiner->JoinComplexBlocks(lsel, rsel);
		if (result_count == 0) {
			// exhausted this pair
			return;
		}

		// found matches: extract them
		chunk.Reset();
		SliceSortedPayload(chunk, left_table.global_sort_state, state.left_block_index, lsel, result_count, 0);
		SliceSortedPayload(chunk, right_table.global_sort_state, state.right_block_index, rsel, result_count,
		                   left_cols);
		chunk.SetCardinality(result_count);

		auto sel = FlatVector::IncrementalSelectionVector();
		if (conditions.size() > 2) {
			// If there are more expressions to compute,
			// split the result chunk into the left and right halves
			// so we can compute the values for comparison.
			const auto tail_cols = conditions.size() - 2;

			DataChunk right_chunk;
			chunk.Split(right_chunk, left_cols);
			state.left_executor.SetChunk(chunk);
			state.right_executor.SetChunk(right_chunk);

			auto tail_count = result_count;
			for (size_t cmp_idx = 0; cmp_idx < tail_cols; ++cmp_idx) {
				auto &left = state.left_keys.data[cmp_idx];
				state.left_executor.ExecuteExpression(cmp_idx, left);

				auto &right = state.right_keys.data[cmp_idx];
				state.right_executor.ExecuteExpression(cmp_idx, right);

				if (tail_count < result_count) {
					left.Slice(*sel, tail_count);
					right.Slice(*sel, tail_count);
				}
				tail_count = state.SelectJoinTail(conditions[cmp_idx + 2].comparison, left, right, sel, tail_count);
				sel = &state.true_sel;
			}
			chunk.Fuse(right_chunk);

			if (tail_count < result_count) {
				result_count = tail_count;
				chunk.Slice(*sel, result_count);
			}
		}

		// found matches: mark the found matches if required
		if (left_table.found_match) {
			for (idx_t i = 0; i < result_count; i++) {
				left_table.found_match[state.left_base + lsel[sel->get_index(i)]] = true;
			}
		}
		if (right_table.found_match) {
			for (idx_t i = 0; i < result_count; i++) {
				right_table.found_match[state.right_base + rsel[sel->get_index(i)]] = true;
			}
		}
		chunk.Verify();
	} while (chunk.size() == 0);
}

OperatorResultType PhysicalIEJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                           GlobalOperatorState &gstate, OperatorState &state) const {
	return OperatorResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class IEJoinGlobalSourceState : public GlobalSourceState {
public:
	explicit IEJoinGlobalSourceState(const PhysicalIEJoin &op)
	    : op(op), initialized(false), next_pair(0), completed(0), left_outers(0), next_left(0), right_outers(0),
	      next_right(0) {
	}

	void Initialize(IEJoinGlobalState &sink_state) {
		lock_guard<mutex> initializing(lock);
		if (initialized) {
			return;
		}

		// Compute the starting row for reach block
		// (In theory these are all the same size, but you never know...)
		auto &left_table = *sink_state.tables[0];
		const auto left_blocks = left_table.BlockCount();
		idx_t left_base = 0;

		for (size_t lhs = 0; lhs < left_blocks; ++lhs) {
			left_bases.emplace_back(left_base);
			left_base += left_table.BlockSize(lhs);
		}

		auto &right_table = *sink_state.tables[1];
		const auto right_blocks = right_table.BlockCount();
		idx_t right_base = 0;
		for (size_t rhs = 0; rhs < right_blocks; ++rhs) {
			right_bases.emplace_back(right_base);
			right_base += right_table.BlockSize(rhs);
		}

		// Outer join block counts
		if (left_table.found_match) {
			left_outers = left_blocks;
		}

		if (right_table.found_match) {
			right_outers = right_blocks;
		}

		// Ready for action
		initialized = true;
	}

public:
	idx_t MaxThreads() override {
		// We can't leverage any more threads than block pairs.
		const auto &sink_state = ((IEJoinGlobalState &)*op.sink_state);
		return sink_state.tables[0]->BlockCount() * sink_state.tables[1]->BlockCount();
	}

	void GetNextPair(ClientContext &client, IEJoinGlobalState &gstate, IEJoinLocalSourceState &lstate) {
		auto &left_table = *gstate.tables[0];
		auto &right_table = *gstate.tables[1];

		const auto left_blocks = left_table.BlockCount();
		const auto right_blocks = right_table.BlockCount();
		const auto pair_count = left_blocks * right_blocks;

		// Regular block
		const auto i = next_pair++;
		if (i < pair_count) {
			const auto b1 = i / right_blocks;
			const auto b2 = i % right_blocks;

			lstate.left_block_index = b1;
			lstate.left_base = left_bases[b1];

			lstate.right_block_index = b2;
			lstate.right_base = right_bases[b2];

			lstate.joiner = make_unique<IEJoinUnion>(client, op, left_table, b1, right_table, b2);
			return;
		} else {
			--next_pair;
		}

		// Outer joins
		if (!left_outers && !right_outers) {
			return;
		}

		// Spin wait for regular blocks to finish(!)
		while (completed < pair_count) {
			std::this_thread::yield();
		}

		// Left outer blocks
		const auto l = next_left++;
		if (l < left_outers) {
			lstate.left_block_index = l;
			lstate.left_base = left_bases[l];

			lstate.left_matches = left_table.found_match.get() + lstate.left_base;
			lstate.outer_idx = 0;
			lstate.outer_count = left_table.BlockSize(l);
			return;
		} else {
			lstate.left_matches = nullptr;
			--next_left;
		}

		// Right outer block
		const auto r = next_right++;
		if (r < right_outers) {
			lstate.right_block_index = r;
			lstate.right_base = right_bases[r];

			lstate.right_matches = right_table.found_match.get() + lstate.right_base;
			lstate.outer_idx = 0;
			lstate.outer_count = right_table.BlockSize(r);
			return;
		} else {
			lstate.right_matches = nullptr;
			--next_right;
		}
	}

	void PairCompleted(ClientContext &client, IEJoinGlobalState &gstate, IEJoinLocalSourceState &lstate) {
		lstate.joiner.reset();
		++completed;
		GetNextPair(client, gstate, lstate);
	}

	const PhysicalIEJoin &op;

	mutex lock;
	bool initialized;

	// Join queue state
	std::atomic<size_t> next_pair;
	std::atomic<size_t> completed;

	// Block base row number
	vector<idx_t> left_bases;
	vector<idx_t> right_bases;

	// Outer joins
	idx_t left_outers;
	std::atomic<idx_t> next_left;

	idx_t right_outers;
	std::atomic<idx_t> next_right;
};

unique_ptr<GlobalSourceState> PhysicalIEJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<IEJoinGlobalSourceState>(*this);
}

unique_ptr<LocalSourceState> PhysicalIEJoin::GetLocalSourceState(ExecutionContext &context,
                                                                 GlobalSourceState &gstate) const {
	return make_unique<IEJoinLocalSourceState>(*this);
}

void PhysicalIEJoin::GetData(ExecutionContext &context, DataChunk &result, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	auto &ie_sink = (IEJoinGlobalState &)*sink_state;
	auto &ie_gstate = (IEJoinGlobalSourceState &)gstate;
	auto &ie_lstate = (IEJoinLocalSourceState &)lstate;

	ie_gstate.Initialize(ie_sink);

	if (!ie_lstate.joiner) {
		ie_gstate.GetNextPair(context.client, ie_sink, ie_lstate);
	}

	// Process INNER results
	while (ie_lstate.joiner) {
		ResolveComplexJoin(context, result, ie_lstate);

		if (result.size()) {
			return;
		}

		ie_gstate.PairCompleted(context.client, ie_sink, ie_lstate);
	}

	// Process LEFT OUTER results
	const auto left_cols = children[0]->GetTypes().size();
	while (ie_lstate.left_matches) {
		const idx_t count = ie_lstate.SelectOuterRows(ie_lstate.left_matches);
		if (!count) {
			ie_gstate.GetNextPair(context.client, ie_sink, ie_lstate);
			continue;
		}

		SliceSortedPayload(result, ie_sink.tables[0]->global_sort_state, ie_lstate.left_base, ie_lstate.true_sel,
		                   count);

		// Fill in NULLs to the right
		for (auto col_idx = left_cols; col_idx < result.ColumnCount(); ++col_idx) {
			result.data[col_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result.data[col_idx], true);
		}

		result.SetCardinality(count);
		result.Verify();

		return;
	}

	// Process RIGHT OUTER results
	while (ie_lstate.right_matches) {
		const idx_t count = ie_lstate.SelectOuterRows(ie_lstate.right_matches);
		if (!count) {
			ie_gstate.GetNextPair(context.client, ie_sink, ie_lstate);
		}

		SliceSortedPayload(result, ie_sink.tables[1]->global_sort_state, ie_lstate.right_base, ie_lstate.true_sel,
		                   count, left_cols);

		// Fill in NULLs to the left
		for (idx_t col_idx = 0; col_idx < left_cols; ++col_idx) {
			result.data[col_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result.data[col_idx], true);
		}

		result.SetCardinality(count);
		result.Verify();

		return;
	}
}

} // namespace duckdb
