#include "duckdb/execution/operator/persistent/physical_create_bf.hpp"

#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/column/partitioned_column_data.hpp"

namespace duckdb {

PhysicalCreateBF::PhysicalCreateBF(vector<LogicalType> types, const vector<shared_ptr<FilterPlan>> &bf_plans,
                                   idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CREATE_BF, std::move(types), estimated_cardinality) {
	for (auto &plan : bf_plans) {
		auto BF = BuildBloomFilter(*plan);
		this->bf_to_create.emplace_back(BF);
	}
}

shared_ptr<BloomFilter> PhysicalCreateBF::BuildBloomFilter(FilterPlan &bf_plan) {
	auto BF = make_shared_ptr<BloomFilter>();
	for (auto &apply_col : bf_plan.apply) {
		BF->column_bindings_applied_.emplace_back(apply_col->Copy());
	}
	for (auto &build_col : bf_plan.build) {
		BF->column_bindings_built_.emplace_back(build_col->Copy());
	}
	// BF->BoundColsApply will be updated in the related PhysicalUseBF
	BF->BoundColsBuilt = bf_plan.bound_cols_build;
	return BF;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CreateBFGlobalSinkState : public GlobalSinkState {
public:
	CreateBFGlobalSinkState(ClientContext &context, const PhysicalCreateBF &op)
	    : context(context), op(op),
	      num_threads(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads())) {
		data_collection = make_uniq<ColumnDataCollection>(context, op.types);

		if (op.filter_pushdown) {
			global_filter_state = op.filter_pushdown->GetGlobalState(context, op);
		}
	}

	void ScheduleFinalize(Pipeline &pipeline, Event &event);

public:
	ClientContext &context;
	const PhysicalCreateBF &op;

	const idx_t num_threads;
	unique_ptr<ColumnDataCollection> data_collection;
	vector<unique_ptr<ColumnDataCollection>> local_data_collections;

	//! min-max filters
	unique_ptr<JoinFilterGlobalState> global_filter_state;
};

class CreateBFLocalSinkState : public LocalSinkState {
public:
	CreateBFLocalSinkState(ClientContext &context, const PhysicalCreateBF &op)
	    : client_context(context), keys_executor(context) {
		local_data = make_uniq<ColumnDataCollection>(context, op.types);

		if (op.filter_pushdown) {
			auto &gstate = op.sink_state->Cast<CreateBFGlobalSinkState>();
			auto &allocator = BufferAllocator::Get(context);

			vector<LogicalType> types;
			for (auto &cond : op.bf_to_create[0]->column_bindings_built_) {
				auto &column = cond->Cast<BoundColumnRefExpression>();
				keys_executor.AddExpression(column);
				types.push_back(column.return_type);
			}
			join_keys.Initialize(allocator, types);
			local_filter_state = op.filter_pushdown->GetLocalState(*gstate.global_filter_state);
		}
	}

	ClientContext &client_context;
	unique_ptr<ColumnDataCollection> local_data;

	DataChunk join_keys;
	ExpressionExecutor keys_executor;
	unique_ptr<JoinFilterLocalState> local_filter_state;
};

SinkResultType PhysicalCreateBF::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &state = input.local_state.Cast<CreateBFLocalSinkState>();
	state.local_data->Append(chunk);

	if (filter_pushdown) {
		state.join_keys.Reset();
		state.keys_executor.Execute(chunk, state.join_keys);

		filter_pushdown->Sink(state.join_keys, *state.local_filter_state);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCreateBF::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<CreateBFGlobalSinkState>();
	auto &state = input.local_state.Cast<CreateBFLocalSinkState>();

	auto guard = gstate.Lock();
	gstate.local_data_collections.push_back(std::move(state.local_data));

	if (filter_pushdown) {
		filter_pushdown->Combine(*gstate.global_filter_state, *state.local_filter_state);
	}
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
//! If we have only one thread, always finalize single-threaded.
static bool FinalizeSingleThreaded(const CreateBFGlobalSinkState &sink) {
	// if only one thread, finalize single-threaded
	const auto num_threads = NumericCast<idx_t>(sink.num_threads);
	if (num_threads == 1) {
		return true;
	}

	// if we want to verify parallelism, finalize parallel
	if (sink.context.config.verify_parallelism) {
		return false;
	}

	if (sink.data_collection->Count() < 1048576) {
		return true;
	}

	return false;
}

class CreateBFFinalizeTask : public ExecutorTask {
public:
	CreateBFFinalizeTask(shared_ptr<Event> event_p, ClientContext &context, CreateBFGlobalSinkState &sink_p,
	                     idx_t chunk_idx_from_p, idx_t chunk_idx_to_p)
	    : ExecutorTask(context, std::move(event_p), sink_p.op), sink(sink_p), chunk_idx_from(chunk_idx_from_p),
	      chunk_idx_to(chunk_idx_to_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		DataChunk chunk;
		sink.data_collection->InitializeScanChunk(chunk);
		for (idx_t i = chunk_idx_from; i < chunk_idx_to; i++) {
			sink.data_collection->FetchChunk(i, chunk);
			for (auto &bf : sink.op.bf_to_create) {
				bf->Insert(chunk);
			}
		}
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	CreateBFGlobalSinkState &sink;
	idx_t chunk_idx_from;
	idx_t chunk_idx_to;
};

class CreateBFFinalizeEvent : public BasePipelineEvent {
public:
	CreateBFFinalizeEvent(Pipeline &pipeline_p, CreateBFGlobalSinkState &sink)
	    : BasePipelineEvent(pipeline_p), sink(sink) {
	}

	CreateBFGlobalSinkState &sink;

public:
	void Schedule() override {
		auto &context = pipeline->GetClientContext();

		vector<shared_ptr<Task>> finalize_tasks;
		auto &collection = sink.data_collection;
		const auto chunk_count = collection->ChunkCount();

		if (FinalizeSingleThreaded(sink)) {
			// Single-threaded finalize
			finalize_tasks.push_back(
			    make_uniq<CreateBFFinalizeTask>(shared_from_this(), context, sink, 0U, chunk_count));
		} else {
			// Parallel finalize
			const idx_t chunks_per_task = context.config.verify_parallelism ? 1 : CHUNKS_PER_TASK;
			for (idx_t chunk_idx = 0; chunk_idx < chunk_count; chunk_idx += chunks_per_task) {
				auto chunk_idx_to = MinValue<idx_t>(chunk_idx + chunks_per_task, chunk_count);
				finalize_tasks.push_back(
				    make_uniq<CreateBFFinalizeTask>(shared_from_this(), context, sink, chunk_idx, chunk_idx_to));
			}
		}
		SetTasks(std::move(finalize_tasks));
	}

	void FinishEvent() override {
		for (auto &bf : sink.op.bf_to_create) {
			bf->finalized_ = true;
		}
	}

	static constexpr idx_t CHUNKS_PER_TASK = 256;
};

void CreateBFGlobalSinkState::ScheduleFinalize(Pipeline &pipeline, Event &event) {
	auto new_event = make_shared_ptr<CreateBFFinalizeEvent>(pipeline, *this);
	event.InsertEvent(std::move(new_event));
}

SinkFinalizeType PhysicalCreateBF::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	auto &sink = input.global_state.Cast<CreateBFGlobalSinkState>();

	// collect local data
	for (auto &local_data : sink.local_data_collections) {
		sink.data_collection->Combine(*local_data);
	}
	sink.local_data_collections.clear();

	// initialize the bloom filter
	for (auto &filter : bf_to_create) {
		filter->Initialize(context, static_cast<uint32_t>(sink.data_collection->Count()));
	}

	sink.ScheduleFinalize(pipeline, event);

	if (filter_pushdown) {
		filter_pushdown->Finalize(context, *sink.global_filter_state, *this);
	}
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> PhysicalCreateBF::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<CreateBFGlobalSinkState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalCreateBF::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<CreateBFLocalSinkState>(context.client, *this);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateBFGlobalSourceState : public GlobalSourceState {
public:
	explicit CreateBFGlobalSourceState(const ColumnDataCollection &collection)
	    : max_threads(MaxValue<idx_t>(collection.ChunkCount(), 1)), data_collection(collection) {
		collection.InitializeScan(global_scan_state);
	}

	const idx_t max_threads;
	const ColumnDataCollection &data_collection;
	ColumnDataParallelScanState global_scan_state;
};

class CreateBFLocalSourceState : public LocalSourceState {
public:
	ColumnDataLocalScanState local_scan_state;
};

InsertionOrderPreservingMap<string> PhysicalCreateBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;

	result["BF Number"] = std::to_string(bf_to_create.size());
	result["ID"] = "0x" + std::to_string(reinterpret_cast<size_t>(this));

	return result;
}

unique_ptr<GlobalSourceState> PhysicalCreateBF::GetGlobalSourceState(ClientContext &context) const {
	auto &gstate = sink_state->Cast<CreateBFGlobalSinkState>();
	return make_uniq<CreateBFGlobalSourceState>(*gstate.data_collection);
}

unique_ptr<LocalSourceState> PhysicalCreateBF::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	return make_uniq<CreateBFLocalSourceState>();
}

SourceResultType PhysicalCreateBF::GetData(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<CreateBFGlobalSourceState>();
	auto &lstate = input.local_state.Cast<CreateBFLocalSourceState>();
	gstate.data_collection.Scan(gstate.global_scan_state, lstate.local_scan_state, chunk);
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

void PhysicalCreateBF::BuildPipelinesFromRelated(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	// operator is a sink, build a pipeline
	D_ASSERT(children.size() == 1);

	if (this_pipeline == nullptr) {
		// we create a new pipeline starting from the child
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
		this_pipeline = child_meta_pipeline.GetBasePipeline();
		child_meta_pipeline.Build(children[0]);
	} else {
		current.AddDependency(this_pipeline);
	}
}

void PhysicalCreateBF::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	auto &state = meta_pipeline.GetState();
	// operator is a sink, build a pipeline
	sink_state.reset();
	D_ASSERT(children.size() == 1);

	// single operator: the operator becomes the data source of the current pipeline
	state.SetPipelineSource(current, *this);
	if (this_pipeline == nullptr) {
		// we create a new pipeline starting from the child
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
		this_pipeline = child_meta_pipeline.GetBasePipeline();
		child_meta_pipeline.Build(children[0]);
	} else {
		current.AddDependency(this_pipeline);
	}
}
} // namespace duckdb
