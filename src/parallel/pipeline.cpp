#include "duckdb/parallel/pipeline.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/tree_renderer/text_tree_renderer.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/pipeline_event.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

static shared_ptr<GlobalSourceState> ToSharedSourceState(unique_ptr<GlobalSourceState> state) {
	return shared_ptr<GlobalSourceState>(std::move(state));
}

PipelineTask::PipelineTask(Pipeline &pipeline_p, shared_ptr<Event> event_p)
    : ExecutorTask(pipeline_p.executor, std::move(event_p)), pipeline(pipeline_p) {
}

bool PipelineTask::TaskBlockedOnResult() const {
	// If this returns true, it means the pipeline this task belongs to has a cached chunk
	// that was the result of the Sink method returning BLOCKED
	return pipeline_executor->RemainingSinkChunk();
}

const PipelineExecutor &PipelineTask::GetPipelineExecutor() const {
	return *pipeline_executor;
}

TaskExecutionResult PipelineTask::ExecuteTask(TaskExecutionMode mode) {
	if (!pipeline_executor) {
		pipeline_executor = make_uniq<PipelineExecutor>(pipeline.GetClientContext(), pipeline);
	}

	pipeline_executor->SetTaskForInterrupts(shared_from_this());

	if (mode == TaskExecutionMode::PROCESS_PARTIAL) {
		auto res = pipeline_executor->Execute(PARTIAL_CHUNK_COUNT);

		switch (res) {
		case PipelineExecuteResult::NOT_FINISHED:
			return TaskExecutionResult::TASK_NOT_FINISHED;
		case PipelineExecuteResult::INTERRUPTED:
			return TaskExecutionResult::TASK_BLOCKED;
		case PipelineExecuteResult::FINISHED:
			break;
		}
	} else {
		auto res = pipeline_executor->Execute();
		switch (res) {
		case PipelineExecuteResult::NOT_FINISHED:
			throw InternalException("Execute without limit should not return NOT_FINISHED");
		case PipelineExecuteResult::INTERRUPTED:
			return TaskExecutionResult::TASK_BLOCKED;
		case PipelineExecuteResult::FINISHED:
			break;
		}
	}

	event->FinishTask();
	pipeline_executor.reset();
	return TaskExecutionResult::TASK_FINISHED;
}

Pipeline::Pipeline(Executor &executor_p)
    : executor(executor_p), ready(false), initialized(false), source(nullptr), sink(nullptr) {
}

ClientContext &Pipeline::GetClientContext() {
	return executor.context;
}

bool Pipeline::GetProgress(ProgressData &progress) {
	D_ASSERT(source);
	idx_t source_cardinality = MinValue<idx_t>(source->estimated_cardinality, 1ULL << 48ULL);
	if (source_cardinality < 1) {
		source_cardinality = 1;
	}
	if (!initialized) {
		progress.done = 0;
		progress.total = double(source_cardinality);
		return true;
	}
	auto &client = executor.context;

	auto state = GetSourceState();
	if (state) {
		progress = source->GetProgress(client, *state);
	} else {
		progress.done = 0;
		progress.total = double(source_cardinality);
	}
	progress.Normalize(double(source_cardinality));
	if (sink) {
		lock_guard<mutex> guard(sink->lock);
		if (sink->sink_state) {
			progress = sink->GetSinkProgress(client, *sink->sink_state, progress);
		}
	}
	return progress.IsValid();
}

void Pipeline::ScheduleSequentialTask(shared_ptr<Event> &event) {
	vector<shared_ptr<Task>> tasks;
	tasks.push_back(make_uniq<PipelineTask>(*this, event));
	event->SetTasks(std::move(tasks));
}

bool Pipeline::TryGetMaxThreads(idx_t &max_threads) {
	// check if the sink, source and all intermediate operators support parallelism
	if (!sink->ParallelSink()) {
		return false;
	}
	if (!source->ParallelSource()) {
		return false;
	}
	auto source_state = GetSourceState();
	D_ASSERT(source_state);
	max_threads = source_state->MaxThreads();

	for (auto &op_ref : operators) {
		auto &op = op_ref.get();
		if (!op.ParallelOperator()) {
			return false;
		}
		lock_guard<mutex> guard(op.lock);
		if (op.op_state) {
			max_threads = MinValue<idx_t>(max_threads, op.op_state->MaxThreads(max_threads));
		}
	}

	auto &scheduler = TaskScheduler::GetScheduler(executor.context);
	auto active_threads = scheduler.NumberOfThreads();
	if (max_threads > active_threads) {
		max_threads = active_threads;
	}
	if (sink) {
		lock_guard<mutex> guard(sink->lock);
		if (sink->sink_state) {
			max_threads = sink->sink_state->MaxThreads(max_threads);
		}
	}
	if (max_threads > active_threads) {
		max_threads = active_threads;
	}

	return true;
}

bool Pipeline::ScheduleParallel(shared_ptr<Event> &event) {
	idx_t max_threads;

	if (!TryGetMaxThreads(max_threads)) {
		return false;
	}

	// Handle partition requirements specific to scheduling
	auto partition_info = sink->RequiredPartitionInfo();
	if (partition_info.batch_index) {
		if (!source->SupportsPartitioning(OperatorPartitionInfo::BatchIndex())) {
			throw InternalException(
			    "Attempting to schedule a pipeline where the sink requires batch index but source does not support it");
		}
	}

	return LaunchScanTasks(event, max_threads);
}

idx_t Pipeline::GetMaxThreads() {
	idx_t max_threads;

	if (!TryGetMaxThreads(max_threads)) {
		return 1; // Fallback for unsupported parallelism
	}

	return max_threads;
}

bool Pipeline::IsOrderDependent() const {
	if (source) {
		auto source_order = source->SourceOrder();
		if (source_order == OrderPreservationType::FIXED_ORDER) {
			return true;
		}
		if (source_order == OrderPreservationType::NO_ORDER) {
			return false;
		}
	}
	for (auto &op_ref : operators) {
		auto &op = op_ref.get();
		if (op.OperatorOrder() == OrderPreservationType::NO_ORDER) {
			return false;
		}
		if (op.OperatorOrder() == OrderPreservationType::FIXED_ORDER) {
			return true;
		}
	}
	if (!Settings::Get<PreserveInsertionOrderSetting>(executor.context)) {
		return false;
	}
	if (sink && sink->SinkOrderDependent()) {
		return true;
	}
	return false;
}

void Pipeline::SetExternalInput() {
	input_mode = PipelineInputMode::EXTERNAL_INPUT;
	annotated_lock_guard<annotated_mutex> guard(external_input_lock);
	external_input_event.reset();
	external_input_event_state = ExternalInputEventState::EXTERNAL_INPUT_UNSET;
}

bool Pipeline::CanUseExternalInput() const {
	if (!sink || !sink->ParallelSink() || sink->SinkOrderDependent()) {
		return false;
	}
	if (sink->GetExternalInputSupport() != PipelineExternalInputSupport::SUPPORTED) {
		return false;
	}
	if (sink->RequiredPartitionInfo().AnyRequired()) {
		return false;
	}
	for (auto &op_ref : operators) {
		auto &op = op_ref.get();
		if (op.GetExternalInputSupport() != PipelineExternalInputSupport::SUPPORTED) {
			return false;
		}
		if (!op.ParallelOperator()) {
			return false;
		}
		if (op.OperatorOrder() == OrderPreservationType::FIXED_ORDER) {
			return false;
		}
	}
	return true;
}

bool Pipeline::CanStopSourceEarly() const {
	// Used by CTE fanout selection to keep streaming only when the consumer may finish early.
	if (sink && sink->GetSourceConsumption() == PipelineSourceConsumption::MAY_STOP_EARLY) {
		return true;
	}
	for (auto &op_ref : operators) {
		if (op_ref.get().GetSourceConsumption() == PipelineSourceConsumption::MAY_STOP_EARLY) {
			return true;
		}
	}
	return false;
}

void Pipeline::Schedule(shared_ptr<Event> &event) {
	D_ASSERT(ready);
	D_ASSERT(sink);
	if (IsExternalInput()) {
		ScheduleExternalInputEvent(event);
		return;
	}
	Reset();
	if (!ScheduleParallel(event)) {
		// could not parallelize this pipeline: push a sequential task instead
		ScheduleSequentialTask(event);
	}
}

bool Pipeline::LaunchScanTasks(shared_ptr<Event> &event, idx_t max_threads) {
	// split the scan up into parts and schedule the parts
	if (max_threads <= 1) {
		// too small to parallelize
		return false;
	}

	// launch a task for every thread
	vector<shared_ptr<Task>> tasks;
	for (idx_t i = 0; i < max_threads; i++) {
		tasks.push_back(make_uniq<PipelineTask>(*this, event));
	}
	event->SetTasks(std::move(tasks));
	return true;
}

void Pipeline::ResetSink() {
	if (sink) {
		if (!sink->IsSink()) {
			throw InternalException("Sink of pipeline does not have IsSink set");
		}
		lock_guard<mutex> guard(sink->lock);
		if (!sink->sink_state) {
			sink->sink_state = sink->GetGlobalSinkState(GetClientContext());
		}
	}
}

void Pipeline::ResetSinkForReschedule() {
	if (!sink) {
		return;
	}
	if (!sink->IsSink()) {
		throw InternalException("Sink of pipeline does not have IsSink set");
	}
	lock_guard<mutex> guard(sink->lock);
	auto &client = GetClientContext();
	auto allow_reuse = Settings::Get<EnableCachingOperatorsSetting>(client);
	if (allow_reuse && sink->sink_state && sink->sink_state->SupportsReuse()) {
		sink->sink_state->Reset(client);
		return;
	}
	sink->sink_state = sink->GetGlobalSinkState(client);
}

void Pipeline::PrepareFinalize() {
	if (sink) {
		if (!sink->IsSink()) {
			throw InternalException("Sink of pipeline does not have IsSink set");
		}
		lock_guard<mutex> guard(sink->lock);
		if (!sink->sink_state) {
			throw InternalException("Sink of pipeline does not have sink state");
		}
		sink->PrepareFinalize(GetClientContext(), *sink->sink_state);
	}
}

void Pipeline::ResetSinkAndOperators() {
	ResetSink();
	for (auto &op_ref : operators) {
		auto &op = op_ref.get();
		lock_guard<mutex> guard(op.lock);
		if (!op.op_state) {
			op.op_state = op.GetGlobalOperatorState(GetClientContext());
		}
	}
}

void Pipeline::Reset() {
	ResetSinkAndOperators();
	ResetSource(false);
	initialized = true;
}

void Pipeline::ResetForReschedule(bool reset_sink) {
	if (reset_sink) {
		ResetSinkForReschedule();
	}
	auto &client = GetClientContext();
	auto allow_reuse = Settings::Get<EnableCachingOperatorsSetting>(client);
	for (auto &op_ref : operators) {
		auto &op = op_ref.get();
		lock_guard<mutex> guard(op.lock);
		if (allow_reuse && op.op_state && op.ResetGlobalOperatorState(client, *op.op_state)) {
			continue;
		}
		op.op_state = op.GetGlobalOperatorState(client);
	}
	if (source && !source->IsSource()) {
		throw InternalException("Source of pipeline does not have IsSource set");
	}
	auto source_state = GetSourceState();
	if (allow_reuse && source_state && source_state->SupportsReuse()) {
		source_state->Reset(client);
	} else {
		SetSourceState(ToSharedSourceState(source->GetGlobalSourceState(client)));
	}
	initialized = true;
}

void Pipeline::ResetSource(bool force) {
	if (source && !source->IsSource()) {
		throw InternalException("Source of pipeline does not have IsSource set");
	}
	auto source_state = GetSourceState();
	if (force || !source_state) {
		SetSourceState(ToSharedSourceState(source->GetGlobalSourceState(GetClientContext())));
	}
}

void Pipeline::PrepareExternalInput() {
	if (!IsExternalInput()) {
		throw InternalException("PrepareExternalInput called for a pipeline that is not externally fed");
	}
	if (initialized) {
		return;
	}
	annotated_lock_guard<annotated_mutex> guard(external_input_lock);
	if (initialized) {
		return;
	}
	ResetSinkAndOperators();
	initialized = true;
}

shared_ptr<GlobalSourceState> Pipeline::GetSourceState() {
	annotated_lock_guard<annotated_mutex> guard(source_state_lock);
	return source_state;
}

void Pipeline::SetSourceState(shared_ptr<GlobalSourceState> state) {
	annotated_lock_guard<annotated_mutex> guard(source_state_lock);
	source_state = std::move(state);
}

void Pipeline::FinishSourceAndPreventBlocking(ClientContext &context) {
	if (IsExternalInput() || !source) {
		return;
	}
	auto source_state = GetSourceState();
	if (!source_state) {
		return;
	}
	source->SourceFinished(context, *source_state);
	annotated_lock_guard<annotated_mutex> state_guard(source_state->lock);
	source_state->PreventBlocking();
	source_state->UnblockTasks();
}

void Pipeline::PreventSourceBlocking() {
	auto source_state = GetSourceState();
	if (!source_state) {
		return;
	}
	annotated_lock_guard<annotated_mutex> state_guard(source_state->lock);
	source_state->PreventBlocking();
	source_state->UnblockTasks();
}

void Pipeline::PreventSinkBlocking() {
	auto sink = GetSink();
	if (!sink) {
		return;
	}
	lock_guard<mutex> sink_guard(sink->lock);
	if (!sink->sink_state) {
		return;
	}
	annotated_lock_guard<annotated_mutex> state_guard(sink->sink_state->lock);
	sink->sink_state->PreventBlocking();
	sink->sink_state->UnblockTasks();
}

void Pipeline::PreventBlocking() {
	PreventSourceBlocking();
	PreventSinkBlocking();
}

void Pipeline::SetExternalInputEvent(const shared_ptr<Event> &event) {
	if (!IsExternalInput()) {
		throw InternalException("SetExternalInputEvent called for a pipeline that is not externally fed");
	}
	if (!event) {
		throw InternalException("SetExternalInputEvent called with a null event");
	}
	annotated_lock_guard<annotated_mutex> guard(external_input_lock);
	auto existing_event = external_input_event.lock();
	if (existing_event && existing_event.get() != event.get()) {
		throw InternalException("External input pipeline event was registered more than once");
	}
	external_input_event = event;
	external_input_event_state = ExternalInputEventState::EXTERNAL_INPUT_REGISTERED;
}

void Pipeline::ScheduleExternalInputEvent(shared_ptr<Event> event) {
	shared_ptr<Event> event_to_finish;
	{
		annotated_lock_guard<annotated_mutex> guard(external_input_lock);
		if (!IsExternalInput()) {
			throw InternalException("ScheduleExternalInputEvent called for a pipeline that is not externally fed");
		}
		if (!event) {
			throw InternalException("ScheduleExternalInputEvent called with a null event");
		}
		auto existing_event = external_input_event.lock();
		if (existing_event && existing_event.get() != event.get()) {
			throw InternalException("External input pipeline event was registered more than once");
		}
		external_input_event = event;
		switch (external_input_event_state) {
		case ExternalInputEventState::EXTERNAL_INPUT_COMPLETED_BEFORE_SCHEDULE:
			external_input_event_state = ExternalInputEventState::EXTERNAL_INPUT_COMPLETED;
			event_to_finish = std::move(event);
			break;
		case ExternalInputEventState::EXTERNAL_INPUT_COMPLETED:
			event_to_finish = std::move(event);
			break;
		case ExternalInputEventState::EXTERNAL_INPUT_UNSET:
		case ExternalInputEventState::EXTERNAL_INPUT_REGISTERED:
		case ExternalInputEventState::EXTERNAL_INPUT_EVENT_SCHEDULED:
			external_input_event_state = ExternalInputEventState::EXTERNAL_INPUT_EVENT_SCHEDULED;
			break;
		}
	}
	if (event_to_finish && !event_to_finish->IsFinished()) {
		event_to_finish->Finish();
	}
}

void Pipeline::CompleteExternalInput() {
	shared_ptr<Event> event;
	{
		annotated_lock_guard<annotated_mutex> guard(external_input_lock);
		if (!IsExternalInput()) {
			throw InternalException("CompleteExternalInput called for a pipeline that is not externally fed");
		}
		if (external_input_event_state == ExternalInputEventState::EXTERNAL_INPUT_COMPLETED ||
		    external_input_event_state == ExternalInputEventState::EXTERNAL_INPUT_COMPLETED_BEFORE_SCHEDULE) {
			return;
		}
		event = external_input_event.lock();
		if (!event) {
			throw InternalException("Completing external input pipeline before its event was scheduled");
		}
		if (external_input_event_state == ExternalInputEventState::EXTERNAL_INPUT_REGISTERED) {
			external_input_event_state = ExternalInputEventState::EXTERNAL_INPUT_COMPLETED_BEFORE_SCHEDULE;
			return;
		}
		D_ASSERT(external_input_event_state == ExternalInputEventState::EXTERNAL_INPUT_EVENT_SCHEDULED);
		external_input_event_state = ExternalInputEventState::EXTERNAL_INPUT_COMPLETED;
	}
	if (!event->IsFinished()) {
		event->Finish();
	}
}

void Pipeline::Ready() {
	if (ready) {
		return;
	}
	ready = true;
	std::reverse(operators.begin(), operators.end());
}

void Pipeline::AddDependency(shared_ptr<Pipeline> &pipeline) {
	D_ASSERT(pipeline);
	dependencies.push_back(weak_ptr<Pipeline>(pipeline));
	pipeline->parents.push_back(weak_ptr<Pipeline>(shared_from_this()));
}

void Pipeline::AddDataflowDependency(shared_ptr<Pipeline> &pipeline) {
	D_ASSERT(pipeline);
	dataflow_dependencies.push_back(weak_ptr<Pipeline>(pipeline));
	pipeline->parents.push_back(weak_ptr<Pipeline>(shared_from_this()));
}

void Pipeline::AddExternalFinishDependency(shared_ptr<Pipeline> &pipeline) {
	D_ASSERT(pipeline);
	external_finish_dependencies.push_back(weak_ptr<Pipeline>(pipeline));
	pipeline->parents.push_back(weak_ptr<Pipeline>(shared_from_this()));
}

vector<weak_ptr<Pipeline>> Pipeline::GetDependencies() const {
	return dependencies;
}

string Pipeline::ToString() const {
	TextTreeRenderer renderer;
	return renderer.ToString(*this);
}

void Pipeline::Print() const {
	Printer::Print(ToString());
}

void Pipeline::PrintDependencies() const {
	for (auto &dep : dependencies) {
		shared_ptr<Pipeline>(dep)->Print();
	}
}

vector<reference<PhysicalOperator>> Pipeline::GetOperators() {
	vector<reference<PhysicalOperator>> result;
	D_ASSERT(source);
	result.push_back(*source);
	for (auto &op : operators) {
		result.push_back(op.get());
	}
	if (sink) {
		result.push_back(*sink);
	}
	return result;
}

vector<const_reference<PhysicalOperator>> Pipeline::GetOperators() const {
	vector<const_reference<PhysicalOperator>> result;
	D_ASSERT(source);
	result.push_back(*source);
	for (auto &op : operators) {
		result.push_back(op.get());
	}
	if (sink) {
		result.push_back(*sink);
	}
	return result;
}

const vector<reference<PhysicalOperator>> &Pipeline::GetIntermediateOperators() const {
	return operators;
}

void Pipeline::ClearSource() {
	annotated_lock_guard<annotated_mutex> source_guard(source_state_lock);
	source_state.reset();
	batch_indexes.clear();
}

idx_t Pipeline::RegisterNewBatchIndex() {
	lock_guard<mutex> l(batch_lock);
	idx_t minimum = batch_indexes.empty() ? base_batch_index : *batch_indexes.begin();
	batch_indexes.insert(minimum);
	return minimum;
}

idx_t Pipeline::UpdateBatchIndex(idx_t old_index, idx_t new_index) {
	lock_guard<mutex> l(batch_lock);
	if (new_index < *batch_indexes.begin()) {
		throw InternalException("Processing batch index %llu, but previous min batch index was %llu", new_index,
		                        *batch_indexes.begin());
	}
	auto entry = batch_indexes.find(old_index);
	if (entry == batch_indexes.end()) {
		throw InternalException("Batch index %llu was not found in set of active batch indexes", old_index);
	}
	batch_indexes.erase(entry);
	batch_indexes.insert(new_index);
	return *batch_indexes.begin();
}
//===--------------------------------------------------------------------===//
// Pipeline Build State
//===--------------------------------------------------------------------===//
void PipelineBuildState::SetPipelineSource(Pipeline &pipeline, PhysicalOperator &op) {
	pipeline.source = &op;
}

void PipelineBuildState::SetPipelineSink(Pipeline &pipeline, optional_ptr<PhysicalOperator> op,
                                         idx_t sink_pipeline_count) {
	pipeline.sink = op;
	// set the base batch index of this pipeline based on how many other pipelines have this node as their sink
	pipeline.base_batch_index = BATCH_INCREMENT * sink_pipeline_count;
}

void PipelineBuildState::AddPipelineOperator(Pipeline &pipeline, PhysicalOperator &op) {
	pipeline.operators.push_back(op);
}

optional_ptr<PhysicalOperator> PipelineBuildState::GetPipelineSource(Pipeline &pipeline) {
	return pipeline.source;
}

optional_ptr<PhysicalOperator> PipelineBuildState::GetPipelineSink(Pipeline &pipeline) {
	return pipeline.sink;
}

void PipelineBuildState::SetPipelineOperators(Pipeline &pipeline, vector<reference<PhysicalOperator>> operators) {
	pipeline.operators = std::move(operators);
}

shared_ptr<Pipeline> PipelineBuildState::CreateChildPipeline(Executor &executor, Pipeline &pipeline,
                                                             PhysicalOperator &op) {
	return executor.CreateChildPipeline(pipeline, op);
}

vector<reference<PhysicalOperator>> PipelineBuildState::GetPipelineOperators(Pipeline &pipeline) {
	return pipeline.operators;
}

} // namespace duckdb
