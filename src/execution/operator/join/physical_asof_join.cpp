#include "duckdb/execution/operator/join/physical_asof_join.hpp"

#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/sorting/hashed_sort.hpp"
#include "duckdb/common/sorting/sort_key.hpp"
#include "duckdb/common/sorting/sorted_run.hpp"
#include "duckdb/common/types/row/block_iterator.hpp"
#include "duckdb/execution/operator/join/outer_join_marker.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"

namespace duckdb {

PhysicalAsOfJoin::PhysicalAsOfJoin(PhysicalPlan &physical_plan, LogicalComparisonJoin &op, PhysicalOperator &left,
                                   PhysicalOperator &right)
    : PhysicalComparisonJoin(physical_plan, op, PhysicalOperatorType::ASOF_JOIN, std::move(op.conditions), op.join_type,
                             op.estimated_cardinality),
      comparison_type(ExpressionType::INVALID) {
	// Convert the conditions partitions and sorts
	D_ASSERT(!op.predicate.get());
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

	AsOfGlobalSinkState(ClientContext &client, const PhysicalAsOfJoin &op) {
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
	}

	//! The child that is being materialised (right/1 then left/0)
	size_t child = 1;
	//! The child's partitioning description
	vector<HashedSortPtr> hashed_sorts;
	//! The child's partitioning buffer
	vector<HashedSinkPtr> hashed_sinks;
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
	}

	// Switch sides
	gstate.child = 1 - gstate.child;

	return hashed_sort.Finalize(client, hfinalize);
}

OperatorResultType PhysicalAsOfJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                     GlobalOperatorState &gstate, OperatorState &lstate_p) const {
	return OperatorResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
enum class AsOfJoinSourceStage : uint8_t { INIT, SORT, MATERIALIZE, GET, LEFT, RIGHT, DONE };

struct AsOfSourceTask {
	AsOfSourceTask() {
	}

	AsOfJoinSourceStage stage = AsOfJoinSourceStage::DONE;
	//! The hash group
	idx_t group_idx = 0;
	//! The thread index (for local state)
	idx_t thread_idx = 0;
	//! The total block index count
	idx_t max_idx = 0;
	//! The first block index count
	idx_t begin_idx = 0;
	//! The last block index count
	idx_t end_idx = 0;
};

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
	idx_t NextSize() const {
		return MinValue<idx_t>(Remaining(), STANDARD_VECTOR_SIZE);
	}
	void SeekBlock(idx_t block_idx) {
		chunk_idx = block_idx;
		base = MinValue<idx_t>(chunk_idx * STANDARD_VECTOR_SIZE, count);
		scanned = base;
	}
	inline void SeekRow(idx_t row_idx) {
		SeekBlock(row_idx / STANDARD_VECTOR_SIZE);
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
		const auto result_count = NextSize();
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

class AsOfHashGroup {
public:
	using HashGroupPtr = unique_ptr<SortedRun>;
	using ChunkRow = HashedSort::ChunkRow;

	template <typename T>
	static T BinValue(T n, T val) {
		return ((n + (val - 1)) / val);
	}

	AsOfHashGroup(const PhysicalAsOfJoin &op, const ChunkRow &left_stats, const ChunkRow &right_stats,
	              const idx_t hash_group);

	//! Is this a right join (do we have a RIGHT stage?)
	inline bool IsRightOuter() const {
		return right_outer.Enabled();
	}

	//! The processing stage for this group
	AsOfJoinSourceStage GetStage() const {
		return stage;
	}

	//! The total number of tasks we will execute
	idx_t GetTaskCount() const {
		return stage_begin[size_t(AsOfJoinSourceStage::DONE)];
	}

	//! The number of left chunks
	inline idx_t LeftChunks() const {
		return left_stats.chunks;
	}

	//! The number of right chunks
	inline idx_t RightChunks() const {
		return right_stats.chunks;
	}

	// Set up the task parameters
	idx_t InitTasks(idx_t per_thread);

	//! The maximum number of chunks that we will scan for each state
	idx_t MaximumChunks() const {
		return MaxValue(LeftChunks(), RightChunks());
	}

	//! Try to move to the next stage
	bool TryPrepareNextStage();
	//! Try to get another task for this group
	bool TryNextTask(AsOfSourceTask &task);
	//! Finish the given task. Returns true if there are no more tasks.
	bool FinishTask(AsOfSourceTask &task);

	//! The parent operator
	const PhysicalAsOfJoin &op;
	//! The group number
	const idx_t group_idx;
	//! The number of left chunks/rows
	const ChunkRow left_stats;
	//! The number of right chunks/rows
	const ChunkRow right_stats;
	//! The left hash partition data
	HashGroupPtr left_group;
	//! The right hash partition data
	HashGroupPtr right_group;
	//! The right outer join markers
	OuterJoinMarker right_outer;
	// The processing stage for this group
	AsOfJoinSourceStage stage;
	//! The the number of blocks per thread.
	idx_t per_thread = 0;
	//! The the number of tasks per stage.
	vector<idx_t> stage_tasks;
	//! The the first task in the stage.
	vector<idx_t> stage_begin;
	//! The next task to process
	idx_t next_task = 0;
	//! Count of sorting tasks completed
	std::atomic<idx_t> sorted;
	//! Count of materialization tasks completed
	std::atomic<idx_t> materialized;
	//! Count of get tasks completed
	std::atomic<idx_t> gotten;
	//! Count of left side tasks completed
	std::atomic<idx_t> left_completed;
	//! Count of right side tasks completed
	std::atomic<idx_t> right_completed;
};

AsOfHashGroup::AsOfHashGroup(const PhysicalAsOfJoin &op, const ChunkRow &left_stats, const ChunkRow &right_stats,
                             const idx_t hash_group)
    : op(op), group_idx(hash_group), left_stats(left_stats), right_stats(right_stats),
      right_outer(IsRightOuterJoin(op.join_type)), stage(AsOfJoinSourceStage::INIT), sorted(0), materialized(0),
      gotten(0), left_completed(0), right_completed(0) {
	right_outer.Initialize(right_stats.count);
};

idx_t AsOfHashGroup::InitTasks(idx_t per_thread_p) {
	per_thread = per_thread_p;

	//	INIT
	stage_tasks.emplace_back(0);

	//	SORT
	auto materialize_tasks = BinValue(LeftChunks(), per_thread);
	materialize_tasks += BinValue(RightChunks(), per_thread);
	stage_tasks.emplace_back(materialize_tasks);

	//	MATERIALIZE
	stage_tasks.emplace_back(materialize_tasks);

	//	GET
	stage_tasks.emplace_back(materialize_tasks ? 1 : 0);

	//	LEFT
	const auto left_tasks = BinValue(LeftChunks(), per_thread);
	stage_tasks.emplace_back(left_tasks);

	//	RIGHT
	const auto right_chunks = IsRightOuter() ? RightChunks() : 0;
	const auto right_tasks = BinValue(right_chunks, per_thread);
	stage_tasks.emplace_back(right_tasks);

	//	DONE
	stage_tasks.emplace_back(0);

	//	Accumulate task counts so we can find boundaries reliably
	idx_t begin = 0;
	for (const auto &stage_task : stage_tasks) {
		stage_begin.emplace_back(begin);
		begin += stage_task;
	}

	stage = AsOfJoinSourceStage(1);

	return GetTaskCount();
}

bool AsOfHashGroup::TryPrepareNextStage() {
	switch (stage) {
	case AsOfJoinSourceStage::INIT:
		stage = AsOfJoinSourceStage::SORT;
		return true;
	case AsOfJoinSourceStage::SORT:
		if (sorted >= stage_tasks[size_t(stage)]) {
			stage = AsOfJoinSourceStage::MATERIALIZE;
			return true;
		}
		break;
	case AsOfJoinSourceStage::MATERIALIZE:
		if (materialized >= stage_tasks[size_t(stage)]) {
			stage = AsOfJoinSourceStage::GET;
			return true;
		}
		break;
	case AsOfJoinSourceStage::GET:
		if (gotten >= stage_tasks[size_t(stage)]) {
			stage = AsOfJoinSourceStage::LEFT;
			return true;
		}
		break;
	case AsOfJoinSourceStage::LEFT:
		if (left_completed >= stage_tasks[size_t(stage)]) {
			stage = stage_tasks[size_t(AsOfJoinSourceStage::RIGHT)] ? AsOfJoinSourceStage::RIGHT
			                                                        : AsOfJoinSourceStage::DONE;
			return true;
		}
		break;
	case AsOfJoinSourceStage::RIGHT:
		if (right_completed >= stage_tasks[size_t(stage)]) {
			stage = AsOfJoinSourceStage::DONE;
			return true;
		}
		break;
	case AsOfJoinSourceStage::DONE:
		return true;
	}

	return false;
}

bool AsOfHashGroup::TryNextTask(AsOfSourceTask &task) {
	if (next_task >= GetTaskCount()) {
		return false;
	}

	//	Search for where we are in the task list
	for (idx_t stage = idx_t(AsOfJoinSourceStage::INIT); stage <= idx_t(AsOfJoinSourceStage::DONE); ++stage) {
		if (next_task < stage_begin[stage]) {
			task.stage = AsOfJoinSourceStage(stage - 1);
			task.thread_idx = next_task - stage_begin[size_t(task.stage)];
			break;
		}
	}

	if (task.stage != GetStage()) {
		return false;
	}

	task.group_idx = group_idx;
	task.begin_idx = 0;
	task.end_idx = 0;

	switch (task.stage) {
	case AsOfJoinSourceStage::SORT:
		task.begin_idx = task.thread_idx * per_thread;
		task.max_idx = LeftChunks() + RightChunks();
		task.end_idx = MinValue<idx_t>(task.begin_idx + per_thread, task.max_idx);
		break;
	case AsOfJoinSourceStage::MATERIALIZE:
		if (!left_group || !right_group) {
			task.begin_idx = task.thread_idx * per_thread;
			task.max_idx = LeftChunks() + RightChunks();
			task.end_idx = MinValue<idx_t>(task.begin_idx + per_thread, task.max_idx);
		}
		break;
	case AsOfJoinSourceStage::GET:
		task.begin_idx = 0;
		task.end_idx = 1;
		task.max_idx = 1;
		break;
	case AsOfJoinSourceStage::LEFT:
		if (left_group) {
			task.begin_idx = task.thread_idx * per_thread;
			task.max_idx = LeftChunks();
			task.end_idx = MinValue<idx_t>(task.begin_idx + per_thread, task.max_idx);
		}
		break;
	case AsOfJoinSourceStage::RIGHT:
		if (right_group) {
			task.begin_idx = task.thread_idx * per_thread;
			task.max_idx = RightChunks();
			task.end_idx = MinValue<idx_t>(task.begin_idx + per_thread, task.max_idx);
		}
		break;
	case AsOfJoinSourceStage::INIT:
	case AsOfJoinSourceStage::DONE:
		break;
	}

	++next_task;

	return true;
}

bool AsOfHashGroup::FinishTask(AsOfSourceTask &task) {
	//	Inside the lock
	switch (task.stage) {
	case AsOfJoinSourceStage::SORT:
	case AsOfJoinSourceStage::MATERIALIZE:
	case AsOfJoinSourceStage::GET:
		break;
	case AsOfJoinSourceStage::LEFT:
		if (left_completed >= stage_tasks[size_t(task.stage)]) {
			left_group.reset();
			if (!IsRightOuter()) {
				right_group.reset();
			}
		}
		break;
	case AsOfJoinSourceStage::RIGHT:
		if (right_completed >= stage_tasks[size_t(task.stage)]) {
			right_group.reset();
		}
		break;
	case AsOfJoinSourceStage::INIT:
	case AsOfJoinSourceStage::DONE:
		break;
	}

	return (materialized + gotten + left_completed + right_completed) >= GetTaskCount();
}

class AsOfLocalSourceState;

class AsOfGlobalSourceState : public GlobalSourceState {
public:
	using AsOfHashGroupPtr = unique_ptr<AsOfHashGroup>;
	using AsOfHashGroups = vector<AsOfHashGroupPtr>;
	using HashedSourceStatePtr = unique_ptr<GlobalSourceState>;
	using Task = AsOfSourceTask;
	using TaskPtr = optional_ptr<Task>;
	using PartitionBlock = std::pair<idx_t, idx_t>;

	AsOfGlobalSourceState(ClientContext &client, const PhysicalAsOfJoin &op);

	//! Are there any more tasks?
	bool HasMoreTasks() const {
		return !stopped && started < total_tasks;
	}
	bool HasUnfinishedTasks() const {
		return !stopped && finished < total_tasks;
	}

	//! Assign a new task to the local state
	bool TryNextTask(TaskPtr &task, Task &task_local);

	//! The parent operator
	const PhysicalAsOfJoin &op;
	//! The source states for the hashed sort
	vector<HashedSourceStatePtr> hashed_sources;
	//! The hash groups
	AsOfHashGroups asof_groups;
	//! The sorted list of (blocks, group_idx) pairs
	vector<PartitionBlock> partition_blocks;
	//! The ordered set of active groups
	vector<idx_t> active_groups;
	//! The next group to start
	atomic<idx_t> next_group;
	//! The total number of tasks
	idx_t total_tasks = 0;
	//! The number of started tasks
	atomic<idx_t> started;
	//! The number of tasks finished.
	atomic<idx_t> finished;
	//! Stop producing tasks
	atomic<bool> stopped;

public:
	idx_t MaxThreads() override {
		return total_tasks;
	}

protected:
	//! Build task list
	void CreateTaskList(ClientContext &client);
	//! Finish a task
	void FinishTask(TaskPtr task);
};

AsOfGlobalSourceState::AsOfGlobalSourceState(ClientContext &client, const PhysicalAsOfJoin &op)
    : op(op), next_group(0), started(0), finished(0), stopped(false) {
	//	 Take ownership of the hash groups
	auto &gsink = op.sink_state->Cast<AsOfGlobalSinkState>();

	using ChunkRow = HashedSort::ChunkRow;
	using ChunkRows = HashedSort::ChunkRows;
	vector<ChunkRows> child_groups(2);
	for (idx_t child = 0; child < child_groups.size(); ++child) {
		auto &hashed_sort = *gsink.hashed_sorts[child];
		auto &hashed_sink = *gsink.hashed_sinks[child];
		auto hashed_source = hashed_sort.GetGlobalSourceState(client, hashed_sink);
		child_groups[child] = hashed_sort.GetHashGroups(*hashed_source);
		hashed_sources.emplace_back(std::move(hashed_source));
	}

	//	Pivot into AsOfHashGroups
	auto &lhs_groups = child_groups[0];
	auto &rhs_groups = child_groups[1];
	const auto group_count = MaxValue<idx_t>(lhs_groups.size(), rhs_groups.size());
	for (idx_t group_idx = 0; group_idx < group_count; ++group_idx) {
		ChunkRow lhs_stats;
		if (group_idx < lhs_groups.size()) {
			lhs_stats = lhs_groups[group_idx];
		}
		ChunkRow rhs_stats;
		if (group_idx < rhs_groups.size()) {
			rhs_stats = rhs_groups[group_idx];
		}
		auto asof_group = make_uniq<AsOfHashGroup>(op, lhs_stats, rhs_stats, group_idx);
		asof_groups.emplace_back(std::move(asof_group));
	}

	CreateTaskList(client);
}

void AsOfGlobalSourceState::CreateTaskList(ClientContext &client) {
	//    Sort the groups from largest to smallest
	if (asof_groups.empty()) {
		return;
	}

	//	Count chunks, not rows (otherwise left and right raggedness could give the wrong answer
	for (idx_t group_idx = 0; group_idx < asof_groups.size(); ++group_idx) {
		auto &asof_hash_group = asof_groups[group_idx];
		if (!asof_hash_group) {
			continue;
		}
		partition_blocks.emplace_back(asof_hash_group->MaximumChunks(), group_idx);
	}
	std::sort(partition_blocks.begin(), partition_blocks.end(), std::greater<PartitionBlock>());
	const auto &max_block = partition_blocks.front();

	//	Schedule the largest group on as many threads as possible
	auto &ts = TaskScheduler::GetScheduler(client);
	const auto threads = NumericCast<idx_t>(ts.NumberOfThreads());

	const auto per_thread = AsOfHashGroup::BinValue(max_block.first, threads);
	if (!per_thread) {
		throw InternalException("No blocks per AsOf thread! %ld threads, %ld groups, %ld blocks, %ld hash group",
		                        threads, partition_blocks.size(), max_block.first, max_block.second);
	}

	for (const auto &b : partition_blocks) {
		total_tasks += asof_groups[b.second]->InitTasks(per_thread);
	}
}

enum class SortKeyPrefixComparisonType : uint8_t { FIXED, VARCHAR, NESTED };

struct SortKeyPrefixComparisonColumn {
	SortKeyPrefixComparisonType type;
	idx_t size;
};

struct SortKeyPrefixComparisonResult {
	//! The column at which the sides are no longer equal,
	//! e.g., Compare([42, 84], [42, 83]) would return {1, COMPARE_GREATERTHAN}
	idx_t column_index;
	//! Either COMPARE_EQUAL, COMPARE_LESSTHAN, COMPARE_GREATERTHAN
	ExpressionType type;
};

struct SortKeyPrefixComparison {
	unsafe_vector<SortKeyPrefixComparisonColumn> columns;
	//! Two row buffer for measuring lhs and rhs widths for nested types.
	//! Gross, but there is currently no way to measure the width of a single key
	//! except as a side-effect of decoding it...
	DataChunk decoded;

	template <class SORT_KEY>
	SortKeyPrefixComparisonResult Compare(const SORT_KEY &lhs, const SORT_KEY &rhs) {
		SortKeyPrefixComparisonResult result {0, ExpressionType::COMPARE_EQUAL};

		auto lhs_copy = lhs;
		string_t lhs_key;
		lhs_copy.Deconstruct(lhs_key);
		auto lhs_ptr = lhs_key.GetData();

		auto rhs_copy = rhs;
		string_t rhs_key;
		rhs_copy.Deconstruct(rhs_key);
		auto rhs_ptr = rhs_key.GetData();

		//	Partition keys are always sorted this way.
		OrderModifiers modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST);

		for (column_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
			const auto &col = columns[col_idx];
			auto &vec = decoded.data[col_idx];
			auto lhs_width = col.size;
			auto rhs_width = col.size;
			int cmp = 1;
			switch (col.type) {
			case SortKeyPrefixComparisonType::FIXED:
				cmp = memcmp(lhs_ptr, rhs_ptr, lhs_width);
				break;
			case SortKeyPrefixComparisonType::VARCHAR:
				//	Include first null byte.
				lhs_width = 1 + strlen(lhs_ptr);
				rhs_width = 1 + strlen(rhs_ptr);
				cmp = memcmp(lhs_ptr, rhs_ptr, MinValue<idx_t>(lhs_width, rhs_width));
				break;
			case SortKeyPrefixComparisonType::NESTED:
				decoded.Reset();
				lhs_width = CreateSortKeyHelpers::DecodeSortKey(lhs_key, vec, 0, modifiers);
				rhs_width = CreateSortKeyHelpers::DecodeSortKey(rhs_key, vec, 1, modifiers);
				cmp = memcmp(lhs_ptr, rhs_ptr, MinValue<idx_t>(lhs_width, rhs_width));
				if (!cmp) {
					cmp = (rhs_width < lhs_width) - (lhs_width < rhs_width);
				}
				break;
			}

			if (cmp) {
				result.type = (cmp < 0) ? ExpressionType::COMPARE_LESSTHAN : ExpressionType::COMPARE_GREATERTHAN;
				return result;
			}

			++result.column_index;
			lhs_ptr += lhs_width;
			rhs_ptr += rhs_width;
		}

		return result;
	}
};

class AsOfProbeBuffer {
public:
	using Orders = vector<BoundOrderByNode>;
	using Task = AsOfSourceTask;
	using TaskPtr = optional_ptr<Task>;

	AsOfProbeBuffer(ClientContext &client, const PhysicalAsOfJoin &op, AsOfGlobalSourceState &gsource);

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
	void ResolveJoin(idx_t *matches);

	using resolve_join_t = void (duckdb::AsOfProbeBuffer::*)(idx_t *);
	resolve_join_t resolve_join_func;

	void BeginLeftScan(TaskPtr task);
	bool NextLeft();
	void ScanLeft();
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
	// resolve joins that can potentially output N*M elements (LEFT, LEFT, FULL)
	void ResolveComplexJoin(ExecutionContext &context, DataChunk &chunk);
	//	Chunk may be empty
	void GetData(ExecutionContext &context, DataChunk &chunk);
	bool HasMoreData() const {
		return !fetch_next_left || (task->begin_idx < task->end_idx);
	}

	ClientContext &client;
	const PhysicalAsOfJoin &op;
	//! The source state
	AsOfGlobalSourceState &gsource;
	//! Is the inequality strict?
	const bool strict;
	//! The current hash group
	optional_ptr<AsOfHashGroup> asof_hash_group;
	//! The task we are processing
	TaskPtr task;

	//	LHS scanning
	optional_ptr<SortedRun> left_group;
	OuterJoinMarker left_outer;
	unique_ptr<ExternalBlockIteratorState> left_itr;
	unique_ptr<AsOfPayloadScanner> lhs_scanner;
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
	DataChunk rhs_input;
	SelectionVector rhs_match_sel;
	idx_t right_bin = 0;

	//	Predicate evaluation
	idx_t lhs_match_count;
	bool fetch_next_left;

	SortKeyPrefixComparison prefix;
};

AsOfProbeBuffer::AsOfProbeBuffer(ClientContext &client, const PhysicalAsOfJoin &op, AsOfGlobalSourceState &gsource)
    : client(client), op(op), gsource(gsource), strict(IsStrictComparison(op.comparison_type)),
      left_outer(IsLeftOuterJoin(op.join_type)), lhs_executor(client), fetch_next_left(true) {
	lhs_keys.Initialize(client, op.join_key_types);
	for (const auto &cond : op.conditions) {
		lhs_executor.AddExpression(*cond.left);
	}

	lhs_payload.Initialize(client, op.children[0].get().GetTypes());
	rhs_payload.Initialize(client, op.children[1].get().GetTypes());
	rhs_input.Initialize(client, op.children[1].get().GetTypes());

	lhs_match_sel.Initialize();
	rhs_match_sel.Initialize();
	left_outer.Initialize(STANDARD_VECTOR_SIZE);

	//	If we have equality predicates, set up the prefix data.
	vector<LogicalType> prefix_types;
	for (idx_t i = 0; i < op.conditions.size() - 1; ++i) {
		const auto &cond = op.conditions[i];
		const auto &type = cond.left->return_type;
		prefix_types.emplace_back(type);
		SortKeyPrefixComparisonColumn col;
		col.size = DConstants::INVALID_INDEX;
		switch (type.id()) {
		case LogicalTypeId::VARCHAR:
		case LogicalTypeId::BLOB:
			col.type = SortKeyPrefixComparisonType::VARCHAR;
			break;
		case LogicalTypeId::STRUCT:
		case LogicalTypeId::LIST:
		case LogicalTypeId::ARRAY:
			col.type = SortKeyPrefixComparisonType::NESTED;
			break;
		default:
			col.type = SortKeyPrefixComparisonType::FIXED;
			col.size = 1 + GetTypeIdSize(type.InternalType());
			break;
		}
		prefix.columns.emplace_back(col);
	}
	if (!prefix_types.empty()) {
		//	LHS, RHS
		prefix.decoded.Initialize(client, prefix_types, 2);
	}
}

void AsOfProbeBuffer::BeginLeftScan(TaskPtr task_p) {
	auto &gsink = op.sink_state->Cast<AsOfGlobalSinkState>();
	task = task_p;
	const auto scan_bin = task->group_idx;

	asof_hash_group = gsource.asof_groups[scan_bin].get();

	//	Always set right_bin too for memory management
	right_group = asof_hash_group->right_group;
	right_bin = right_group ? scan_bin : gsource.asof_groups.size();

	left_group = asof_hash_group->left_group;
	left_bin = left_group ? scan_bin : gsource.asof_groups.size();
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
	lhs_scanner->SeekBlock(task->begin_idx);
	left_itr = CreateIteratorState(*left_group);

	// We are only probing the corresponding right side bin, which may be empty
	// If it is empty, we leave the iterator as null so we can emit left matches
	right_pos = 0;
	if (right_group) {
		right_outer = &asof_hash_group->right_outer;
		if (right_group && right_group->Count()) {
			right_itr = CreateIteratorState(*right_group);
			rhs_scanner = make_uniq<AsOfPayloadScanner>(*right_group, *gsink.hashed_sorts[1]);
		}
	}
}

bool AsOfProbeBuffer::NextLeft() {
	return task->begin_idx < task->end_idx;
}

void AsOfProbeBuffer::ScanLeft() {
	//	Scan the next sorted chunk
	lhs_payload.Reset();
	lhs_scanner->Scan(lhs_payload);
	++task->begin_idx;

	//	Compute the join keys
	lhs_keys.Reset();
	lhs_executor.Execute(lhs_payload, lhs_keys);
	lhs_keys.Flatten();

	//	Combine the NULLs
	const auto count = lhs_payload.size();
	lhs_valid_mask.Reset();
	for (auto col_idx : op.null_sensitive) {
		auto &col = lhs_keys.data[col_idx];
		UnifiedVectorFormat unified;
		col.ToUnifiedFormat(count, unified);
		lhs_valid_mask.Combine(unified.validity, count);
	}

	// Filter out NULL matches
	if (!lhs_valid_mask.AllValid()) {
		const auto count = lhs_match_count;
		lhs_match_count = 0;
		for (idx_t i = 0; i < count; ++i) {
			const auto idx = lhs_match_sel.get_index(i);
			if (lhs_valid_mask.RowIsValidUnsafe(idx)) {
				lhs_match_sel.set_index(lhs_match_count++, idx);
			}
		}
	}
}

void AsOfProbeBuffer::EndLeftScan() {
	if (task->stage != AsOfJoinSourceStage::LEFT) {
		return;
	}
	task->stage = AsOfJoinSourceStage::DONE;

	D_ASSERT(asof_hash_group);
	asof_hash_group->left_completed++;

	right_group = nullptr;
	right_itr.reset();
	rhs_scanner.reset();
	right_outer = nullptr;

	left_group = nullptr;
	left_itr.reset();
	lhs_scanner.reset();
}

template <SortKeyType SORT_KEY_TYPE>
void AsOfProbeBuffer::ResolveJoin(idx_t *matches) {
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

	const auto count = lhs_scanner->NextSize();
	const auto left_base = lhs_scanner->Scanned();
	//	Searching for right <= left
	for (idx_t i = 0; i < count; ++i) {
		//	If right > left, then there is no match
		const auto left_pos = left_base + i;
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

		//	Check partitions for strict equality
		if (!prefix.columns.empty()) {
			const auto cmp = prefix.Compare(left_key[left_pos], right_key[right_pos]);
			if (cmp.column_index < prefix.columns.size()) {
				continue;
			}
		}

		// Emit match data
		if (matches) {
			matches[i] = first;
		}
		lhs_match_sel.set_index(lhs_match_count++, i);
	}
}

void AsOfProbeBuffer::ResolveSimpleJoin(ExecutionContext &context, DataChunk &chunk) {
	// perform the actual join
	(this->*resolve_join_func)(nullptr);

	// Scan the lhs values (after comparing keys) and filter out the LHS NULLs
	ScanLeft();

	// Convert the match selection to simple join mask
	bool found_match[STANDARD_VECTOR_SIZE] = {false};
	for (idx_t i = 0; i < lhs_match_count; ++i) {
		found_match[lhs_match_sel.get_index(i)] = true;
	}

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
	(this->*resolve_join_func)(matches);

	// Scan the lhs values (after comparing keys) and filter out the LHS NULLs
	ScanLeft();

	//	Extract the rhs input columns from the match
	rhs_input.Reset();
	idx_t rhs_match_count = 0;
	for (idx_t i = 0; i < lhs_match_count; ++i) {
		const auto idx = lhs_match_sel[i];
		const auto match_pos = matches[idx];
		// Skip to the range containing the match
		if (match_pos >= rhs_scanner->Scanned()) {
			if (rhs_match_count) {
				rhs_input.Append(rhs_payload, false, &rhs_match_sel, rhs_match_count);
				rhs_match_count = 0;
			}
			rhs_payload.Reset();
			rhs_scanner->SeekRow(match_pos);
			rhs_scanner->Scan(rhs_payload);
		}
		// Select the individual values
		const auto source_offset = match_pos - rhs_scanner->Base();
		rhs_match_sel.set_index(rhs_match_count++, source_offset);
	}
	rhs_input.Append(rhs_payload, false, &rhs_match_sel, rhs_match_count);

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

unique_ptr<GlobalSourceState> PhysicalAsOfJoin::GetGlobalSourceState(ClientContext &client) const {
	return make_uniq<AsOfGlobalSourceState>(client, *this);
}

class AsOfLocalSourceState : public LocalSourceState {
public:
	using HashGroupPtr = optional_ptr<SortedRun>;
	using Task = AsOfSourceTask;
	using TaskPtr = optional_ptr<Task>;

	AsOfLocalSourceState(ExecutionContext &context, AsOfGlobalSourceState &gsource, const PhysicalAsOfJoin &op);

	//! Task management
	bool TaskFinished() const;
	//! Assign the next task
	bool TryAssignTask();

	void ExecuteSortTask(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &source);
	void ExecuteMaterializeTask(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &source);
	void ExecuteGetTask(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &source);
	void ExecuteLeftTask(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &source);
	void ExecuteRightTask(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &source);

	void ExecuteTask(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &source) {
		switch (task->stage) {
		case AsOfJoinSourceStage::SORT:
			ExecuteSortTask(context, chunk, source);
			break;
		case AsOfJoinSourceStage::MATERIALIZE:
			ExecuteMaterializeTask(context, chunk, source);
			break;
		case AsOfJoinSourceStage::GET:
			ExecuteGetTask(context, chunk, source);
			break;
		case AsOfJoinSourceStage::LEFT:
			ExecuteLeftTask(context, chunk, source);
			break;
		case AsOfJoinSourceStage::RIGHT:
			ExecuteRightTask(context, chunk, source);
			break;
		case AsOfJoinSourceStage::INIT:
		case AsOfJoinSourceStage::DONE:
			throw InternalException("Invalid state for AsOf Task");
		}
	}

	void BeginRightScan();
	void EndRightScan();

	AsOfGlobalSourceState &gsource;
	ExecutionContext &context;

	//! The left side partition being probed
	AsOfProbeBuffer probe_buffer;

	//! The task this thread is working on
	TaskPtr task;
	//! The task storage
	Task task_local;
	//! The rhs group
	HashGroupPtr hash_group;
	//! The read cursor
	unique_ptr<AsOfPayloadScanner> scanner;
	//! The right outer buffer
	DataChunk rhs_chunk;
	//! The right outer slicer
	SelectionVector rsel;
	//! Pointer to the right marker
	const bool *rhs_matches = {};
};

AsOfLocalSourceState::AsOfLocalSourceState(ExecutionContext &context, AsOfGlobalSourceState &gsource,
                                           const PhysicalAsOfJoin &op)
    : gsource(gsource), context(context), probe_buffer(context.client, op, gsource), rsel(STANDARD_VECTOR_SIZE) {
	rhs_chunk.Initialize(context.client, op.children[1].get().GetTypes());
}

bool AsOfLocalSourceState::TaskFinished() const {
	if (!task) {
		return true;
	}

	if (task->stage == AsOfJoinSourceStage::LEFT && !probe_buffer.fetch_next_left) {
		return false;
	}

	return task->begin_idx >= task->end_idx;
}

void AsOfLocalSourceState::BeginRightScan() {
	const auto hash_bin = task->group_idx;

	auto &asof_groups = gsource.asof_groups;
	if (hash_bin >= asof_groups.size()) {
		return;
	}

	hash_group = asof_groups[hash_bin]->right_group.get();
	if (!hash_group || !hash_group->Count()) {
		return;
	}
	auto &gsink = gsource.op.sink_state->Cast<AsOfGlobalSinkState>();
	scanner = make_uniq<AsOfPayloadScanner>(*hash_group, *gsink.hashed_sorts[1]);
	scanner->SeekBlock(task->begin_idx);

	rhs_matches = asof_groups[hash_bin]->right_outer.GetMatches();
}

void AsOfLocalSourceState::EndRightScan() {
	D_ASSERT(task->stage == AsOfJoinSourceStage::RIGHT);

	auto &asof_groups = gsource.asof_groups;
	const auto hash_bin = task->group_idx;
	const auto &asof_hash_group = asof_groups[hash_bin];
	asof_hash_group->right_completed++;
}

unique_ptr<LocalSourceState> PhysicalAsOfJoin::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	auto &gsource = gstate.Cast<AsOfGlobalSourceState>();
	return make_uniq<AsOfLocalSourceState>(context, gsource, *this);
}

void AsOfGlobalSourceState::FinishTask(TaskPtr task) {
	//	Inside the lock
	if (!task) {
		return;
	}

	++finished;

	const auto group_idx = task->group_idx;
	auto &finished_hash_group = asof_groups[group_idx];
	D_ASSERT(finished_hash_group);

	if (finished_hash_group->FinishTask(*task)) {
		//	Remove it from the active groups
		auto &v = active_groups;
		v.erase(std::remove(v.begin(), v.end(), group_idx), v.end());
	}
}

bool AsOfLocalSourceState::TryAssignTask() {
	D_ASSERT(TaskFinished());
	// Because downstream operators may be using our internal buffers,
	// we can't "finish" a task until we are about to get the next one.
	if (task) {
		switch (task->stage) {
		case AsOfJoinSourceStage::SORT:
			gsource.asof_groups[task_local.group_idx]->sorted++;
			break;
		case AsOfJoinSourceStage::MATERIALIZE:
			gsource.asof_groups[task_local.group_idx]->materialized++;
			break;
		case AsOfJoinSourceStage::GET:
			gsource.asof_groups[task_local.group_idx]->gotten++;
			break;
		case AsOfJoinSourceStage::LEFT:
			probe_buffer.EndLeftScan();
			break;
		case AsOfJoinSourceStage::RIGHT:
			EndRightScan();
			break;
		case AsOfJoinSourceStage::INIT:
		case AsOfJoinSourceStage::DONE:
			break;
		}
	}

	if (!gsource.TryNextTask(task, task_local)) {
		return false;
	}

	switch (task->stage) {
	case AsOfJoinSourceStage::SORT:
	case AsOfJoinSourceStage::MATERIALIZE:
	case AsOfJoinSourceStage::GET:
		break;
	case AsOfJoinSourceStage::LEFT:
		probe_buffer.BeginLeftScan(*task);
		break;
	case AsOfJoinSourceStage::RIGHT:
		BeginRightScan();
		break;
	case AsOfJoinSourceStage::INIT:
	case AsOfJoinSourceStage::DONE:
		break;
	}

	return true;
}

bool AsOfGlobalSourceState::TryNextTask(TaskPtr &task, Task &task_local) {
	auto guard = Lock();
	FinishTask(task);

	if (!HasMoreTasks()) {
		task = nullptr;
		return false;
	}

	//	Run through the active groups looking for one that can assign a task
	for (const auto &group_idx : active_groups) {
		auto &asof_group = asof_groups[group_idx];
		if (asof_group->TryPrepareNextStage()) {
			UnblockTasks(guard);
		}
		if (asof_group->TryNextTask(task_local)) {
			task = task_local;
			++started;
			return true;
		}
	}

	//	All active groups are busy or blocked, so start the next one (if any)
	while (next_group < partition_blocks.size()) {
		const auto group_idx = partition_blocks[next_group++].second;
		active_groups.emplace_back(group_idx);

		auto &asof_group = asof_groups[group_idx];
		if (asof_group->TryPrepareNextStage()) {
			UnblockTasks(guard);
		}
		if (!asof_group->TryNextTask(task_local)) {
			//	Group has no tasks (empty?)
			continue;
		}

		task = task_local;
		++started;
		return true;
	}

	task = nullptr;

	return false;
}

void AsOfLocalSourceState::ExecuteSortTask(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &source) {
	auto &asof_group = *gsource.asof_groups[task_local.group_idx];

	//	Left or right?
	const idx_t child = task_local.begin_idx >= asof_group.LeftChunks();
	const auto &gsink = gsource.op.sink_state->Cast<AsOfGlobalSinkState>();
	auto &hashed_sort = *gsink.hashed_sorts[child];
	auto &hashed_sink = *gsink.hashed_sinks[child];

	OperatorSinkFinalizeInput finalize {hashed_sink, source.interrupt_state};
	hashed_sort.SortColumnData(context, task_local.group_idx, finalize);

	//	Mark this range as done
	task->begin_idx = task->end_idx;
}

void AsOfLocalSourceState::ExecuteMaterializeTask(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &source) {
	auto &asof_group = *gsource.asof_groups[task_local.group_idx];

	//	Left or right?
	const idx_t child = task_local.begin_idx >= asof_group.LeftChunks();
	const auto &gsink = gsource.op.sink_state->Cast<AsOfGlobalSinkState>();
	auto &hashed_sort = *gsink.hashed_sorts[child];
	auto &hashed_source = *gsource.hashed_sources[child];

	auto unused = make_uniq<LocalSourceState>();
	OperatorSourceInput hsource {hashed_source, *unused, source.interrupt_state};
	hashed_sort.MaterializeSortedRun(context, task_local.group_idx, hsource);

	//	Mark this range as done
	task->begin_idx = task->end_idx;
}

void AsOfLocalSourceState::ExecuteGetTask(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &source) {
	auto &asof_group = *gsource.asof_groups[task_local.group_idx];

	const auto &gsink = gsource.op.sink_state->Cast<AsOfGlobalSinkState>();
	auto unused = make_uniq<LocalSourceState>();

	for (idx_t child = 0; child < gsink.hashed_sorts.size(); ++child) {
		//	Don't get children that don't exist
		if (child) {
			if (!asof_group.RightChunks()) {
				continue;
			}
		} else {
			if (!asof_group.LeftChunks()) {
				continue;
			}
		}

		auto &hashed_sort = *gsink.hashed_sorts[child];
		auto &hashed_source = *gsource.hashed_sources[child];
		OperatorSourceInput hsource {hashed_source, *unused, source.interrupt_state};

		auto group = hashed_sort.GetSortedRun(context.client, task_local.group_idx, hsource);
		if (group) {
			if (child) {
				asof_group.right_group = std::move(group);
			} else {
				asof_group.left_group = std::move(group);
			}
		}
	}

	//	Mark this range as done
	task->begin_idx = task->end_idx;
}

void AsOfLocalSourceState::ExecuteLeftTask(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &source) {
	while (probe_buffer.HasMoreData()) {
		probe_buffer.GetData(context, chunk);
		if (chunk.size()) {
			return;
		}
	}
}

SourceResultType PhysicalAsOfJoin::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	auto &gsource = input.global_state.Cast<AsOfGlobalSourceState>();
	auto &lsource = input.local_state.Cast<AsOfLocalSourceState>();

	// Any call to GetData must produce tuples, otherwise the pipeline executor thinks that we're done
	// Therefore, we loop until we've produced tuples, or until the operator is actually done
	while (gsource.HasUnfinishedTasks() && chunk.size() == 0) {
		if (!lsource.TaskFinished() || lsource.TryAssignTask()) {
			try {
				lsource.ExecuteTask(context, chunk, input);
			} catch (...) {
				gsource.stopped = true;
				throw;
			}
		} else {
			auto guard = gsource.Lock();
			if (!gsource.HasMoreTasks()) {
				gsource.UnblockTasks(guard);
			} else {
				// there are more tasks available, but we can't execute them yet
				// block the source
				return gsource.BlockSource(guard, input.interrupt_state);
			}
		}
	}

	return chunk.size() > 0 ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
}

void AsOfLocalSourceState::ExecuteRightTask(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) {
	while (task->begin_idx < task->end_idx) {
		const auto rhs_position = scanner->Scanned();
		scanner->Scan(rhs_chunk);
		++task->begin_idx;

		// figure out which tuples didn't find a match in the RHS
		const auto count = rhs_chunk.size();
		idx_t result_count = 0;
		for (idx_t i = 0; i < count; i++) {
			if (!rhs_matches[rhs_position + i]) {
				rsel.set_index(result_count++, i);
			}
		}
		if (!result_count) {
			continue;
		}

		// if there were any tuples that didn't find a match, output them
		const auto &op = gsource.op;
		const idx_t left_column_count = op.children[0].get().GetTypes().size();
		for (idx_t col_idx = 0; col_idx < left_column_count; ++col_idx) {
			chunk.data[col_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(chunk.data[col_idx], true);
		}
		for (idx_t col_idx = 0; col_idx < op.right_projection_map.size(); ++col_idx) {
			const auto rhs_idx = op.right_projection_map[col_idx];
			chunk.data[left_column_count + col_idx].Slice(rhs_chunk.data[rhs_idx], rsel, result_count);
		}
		chunk.SetCardinality(result_count);
		return;
	}

	//	Exhausted the task data
	scanner.reset();
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
