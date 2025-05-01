#include "duckdb/execution/operator/persistent/physical_create_bf.hpp"

#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/column/partitioned_column_data.hpp"
#include "duckdb/execution/operator/aggregate/ungrouped_aggregate_state.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

namespace duckdb {

PhysicalCreateBF::PhysicalCreateBF(vector<LogicalType> types, const vector<shared_ptr<FilterPlan>> &filter_plans,
                                   vector<shared_ptr<DynamicTableFilterSet>> dynamic_filter_sets,
                                   vector<vector<ColumnBinding>> &dynamic_filter_cols, idx_t estimated_cardinality,
                                   bool is_probing_side)
    : PhysicalOperator(PhysicalOperatorType::CREATE_BF, std::move(types), estimated_cardinality),
      is_probing_side(is_probing_side), is_successful(true), filter_plans(filter_plans),
      min_max_applied_cols(std::move(dynamic_filter_cols)), min_max_to_create(std::move(dynamic_filter_sets)) {
	// TODO: we may unify the creation of BFs if they are built on the same columns.
	for (size_t i = 0; i < filter_plans.size(); ++i) {
		bf_to_create.emplace_back(make_shared_ptr<BloomFilter>());
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CreateBFGlobalSinkState : public GlobalSinkState {
public:
	CreateBFGlobalSinkState(ClientContext &context, const PhysicalCreateBF &op)
	    : context(context), op(op),
	      num_threads(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads())),
	      temporary_memory_state(TemporaryMemoryManager::Get(context).Register(context)), is_selectivity_checked(false),
	      num_input_rows(0) {
		data_collection = make_uniq<ColumnDataCollection>(context, op.types);

		// init min max aggregation
		vector<AggregateFunction> aggr_functions;
		aggr_functions.push_back(MinFunction::GetFunction());
		aggr_functions.push_back(MaxFunction::GetFunction());

		for (idx_t i = 0; i < op.filter_plans.size(); ++i) {
			if (op.min_max_to_create[i] == nullptr) {
				continue;
			}

			auto &plan = op.filter_plans[i];
			for (idx_t j = 0; j < plan->build.size(); ++j) {
				auto &expr = plan->build[j];
				auto &bound_col_idx = plan->bound_cols_build[j];
				if (col_to_expr.count(bound_col_idx)) {
					continue;
				}
				col_to_expr[bound_col_idx] = min_max_aggregates.size();

				for (auto &aggr : aggr_functions) {
					FunctionBinder function_binder(context);
					vector<unique_ptr<Expression>> aggr_children;
					aggr_children.push_back(expr->Copy());
					auto aggr_expr = function_binder.BindAggregateFunction(aggr, std::move(aggr_children), nullptr,
					                                                       AggregateType::NON_DISTINCT);
					min_max_aggregates.push_back(std::move(aggr_expr));
				}
			}
		}

		global_aggregate_state =
		    make_uniq<GlobalUngroupedAggregateState>(BufferAllocator::Get(context), min_max_aggregates);
	}

	void FinalizeMinMax() {
		vector<LogicalType> min_max_types;
		for (auto &aggr_expr : min_max_aggregates) {
			min_max_types.push_back(aggr_expr->return_type);
		}
		auto final_min_max = make_uniq<DataChunk>();
		final_min_max->Initialize(Allocator::DefaultAllocator(), min_max_types);
		global_aggregate_state->Finalize(*final_min_max);

		// create a filter for each of the aggregates
		for (idx_t idx = 0; idx < op.filter_plans.size(); idx++) {
			auto &filter_plan = op.filter_plans[idx];
			auto &dynamic_filters = op.min_max_to_create[idx];
			auto &applied_bindings = op.min_max_applied_cols[idx];
			if (dynamic_filters == nullptr) {
				continue;
			}

			for (idx_t expr_idx = 0; expr_idx < filter_plan->build.size(); expr_idx++) {
				idx_t apply_col_index = applied_bindings[expr_idx].column_index;
				idx_t build_col_index = filter_plan->bound_cols_build[expr_idx];
				idx_t agg_idx = col_to_expr[build_col_index];

				auto min_val = final_min_max->data[agg_idx].GetValue(0);
				auto max_val = final_min_max->data[agg_idx + 1].GetValue(0);

				if (min_val.IsNull() || max_val.IsNull()) {
					// it means that no rows can pass the min-max filter...
					min_val = Value::MaximumValue(min_val.type());
					max_val = Value::MinimumValue(max_val.type());
				}

				if (Value::NotDistinctFrom(min_val, max_val)) {
					// min = max - generate an equality filter
					auto constant_filter = make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, std::move(min_val));
					dynamic_filters->PushFilter(op, apply_col_index, std::move(constant_filter));
				} else {
					// min != max - generate a range filter
					auto greater_equals =
					    make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, std::move(min_val));
					dynamic_filters->PushFilter(op, apply_col_index, std::move(greater_equals));
					auto less_equals =
					    make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHANOREQUALTO, std::move(max_val));
					dynamic_filters->PushFilter(op, apply_col_index, std::move(less_equals));
				}
			}
		}
	}

	void ScheduleFinalize(Pipeline &pipeline, Event &event);

public:
	ClientContext &context;
	const PhysicalCreateBF &op;

	const idx_t num_threads;
	unique_ptr<ColumnDataCollection> data_collection;
	vector<unique_ptr<ColumnDataCollection>> local_data_collections;

	//! Global Min/Max aggregates for filter pushdown
	unordered_map<idx_t, idx_t> col_to_expr;
	unique_ptr<GlobalUngroupedAggregateState> global_aggregate_state;
	vector<unique_ptr<Expression>> min_max_aggregates;

	//! If memory is not enough, give up creating BFs
	unique_ptr<TemporaryMemoryState> temporary_memory_state;

	// runtime statistics
	atomic<bool> is_selectivity_checked;
	atomic<int64_t> num_input_rows;
};

class CreateBFLocalSinkState : public LocalSinkState {
public:
	CreateBFLocalSinkState(ClientContext &context, const PhysicalCreateBF &op) : client_context(context) {
		auto &gstate = op.sink_state->Cast<CreateBFGlobalSinkState>();
		local_data = make_uniq<ColumnDataCollection>(context, op.types);
		col_to_expr = gstate.col_to_expr;
		local_aggregate_state = make_uniq<LocalUngroupedAggregateState>(*gstate.global_aggregate_state);

		// How much is the memory that used to materialize the table and build BFs?
		temporary_memory_state = TemporaryMemoryManager::Get(context).Register(context);
		temporary_memory_state->SetRemainingSizeAndUpdateReservation(context, op.GetMaxThreadMemory(context));
	}

	void SinkMinMaxFilter(DataChunk &chunk) const {
		for (auto &pair : col_to_expr) {
			idx_t col_idx = pair.first;
			idx_t expr_idx = pair.second;

			// min and max
			local_aggregate_state->Sink(chunk, col_idx, expr_idx);
			local_aggregate_state->Sink(chunk, col_idx, expr_idx + 1);
		}
	}

	ClientContext &client_context;
	unique_ptr<ColumnDataCollection> local_data;

	//! Local Min/Max aggregates for filter pushdown
	unordered_map<idx_t, idx_t> col_to_expr;
	unique_ptr<LocalUngroupedAggregateState> local_aggregate_state;

	//! If memory is not enough, give up creating BFs
	unique_ptr<TemporaryMemoryState> temporary_memory_state;
};

bool PhysicalCreateBF::GiveUpBFCreation(const DataChunk &chunk, OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<CreateBFLocalSinkState>();
	auto &gstate = input.global_state.Cast<CreateBFGlobalSinkState>();

	// Early Stop: OOM
	if (lstate.local_data->AllocationSize() + chunk.GetAllocationSize() >=
	    lstate.temporary_memory_state->GetReservation()) {
		is_successful = false;
		return true;
	}

	// Early Stop: Unfiltered Table or estimated OOM
	if (!gstate.is_selectivity_checked) {
		gstate.num_input_rows += static_cast<int64_t>(chunk.size());

		if (this_pipeline->num_source_chunks > 32) {
			gstate.is_selectivity_checked = true;

			bool has_logical_filter = true;
			switch (this_pipeline->GetSource()->type) {
			case PhysicalOperatorType::TABLE_SCAN: {
				auto &source = this_pipeline->GetSource()->Cast<PhysicalTableScan>();
				has_logical_filter = source.table_filters && !source.table_filters->filters.empty();
				break;
			}
			default:
				break;
			}

			// Check the selectivity
			double input_rows = static_cast<double>(gstate.num_input_rows);
			double source_rows = has_logical_filter
			                         ? static_cast<double>(this_pipeline->num_source_chunks * STANDARD_VECTOR_SIZE)
			                         : static_cast<double>(this_pipeline->num_source_rows);
			double selectivity = input_rows / source_rows;

			// Such a high selectivity means that the base table is not filtered. It is not beneficial to build a BF on
			// a full table.
			if (selectivity > 0.95) {
				is_successful = false;
				return true;
			}

			// Estimate the lower bound of required memory, which is used to materialize this table. If it is very
			// large, give up creating BF.
			ProgressData progress;
			this_pipeline->GetProgress(progress);
			double percent = progress.done / progress.total;
			double estimated_num_rows = static_cast<double>(gstate.num_input_rows) / percent;
			idx_t per_tuple_size = chunk.GetAllocationSize() / chunk.size();
			idx_t estimated_required_memory = static_cast<idx_t>(estimated_num_rows) * per_tuple_size;
			if (estimated_required_memory >= lstate.temporary_memory_state->GetReservation()) {
				is_successful = false;
				return true;
			}
		}
	}

	return false;
}

SinkResultType PhysicalCreateBF::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &state = input.local_state.Cast<CreateBFLocalSinkState>();

	if (!is_successful || GiveUpBFCreation(chunk, input)) {
		return SinkResultType::FINISHED;
	}

	// Bloomfilter
	state.local_data->Append(chunk);

	// min-max
	for (auto &pair : state.col_to_expr) {
		idx_t col_idx = pair.first;
		idx_t expr_idx = pair.second;

		state.local_aggregate_state->Sink(chunk, col_idx, expr_idx);
		state.local_aggregate_state->Sink(chunk, col_idx, expr_idx + 1);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCreateBF::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<CreateBFGlobalSinkState>();
	auto &state = input.local_state.Cast<CreateBFLocalSinkState>();

	// give up creating filters
	if (!is_successful) {
		return SinkCombineResultType::FINISHED;
	}

	// min-max aggregation
	gstate.global_aggregate_state->Combine(*state.local_aggregate_state);

	auto guard = gstate.Lock();
	size_t global_size =
	    gstate.temporary_memory_state->GetMinimumReservation() + state.temporary_memory_state->GetMinimumReservation();
	gstate.temporary_memory_state->SetMinimumReservation(global_size);
	gstate.temporary_memory_state->SetRemainingSizeAndUpdateReservation(context.client, global_size);

	gstate.local_data_collections.push_back(std::move(state.local_data));
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

		// Because null values should not be filtered in many cases (e.g., mark join), we insert null into each bloom
		// filter.
		DataChunk chunk;
		sink.data_collection->InitializeScanChunk(chunk);
		chunk.SetCardinality(1);
		for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
			chunk.SetValue(i, 0, Value());
		}
		for (auto &bf : sink.op.bf_to_create) {
			bf->Insert(chunk);
		}
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

	// Give up creating filters
	if (!is_successful) {
		return SinkFinalizeType::READY;
	}

	// Finalize min-max
	sink.FinalizeMinMax();

	// Collect local data
	for (auto &local_data : sink.local_data_collections) {
		sink.data_collection->Combine(*local_data);
	}
	sink.local_data_collections.clear();

	// Initialize the bloom filter
	uint32_t num_rows = static_cast<uint32_t>(sink.data_collection->Count());
	for (size_t i = 0; i < filter_plans.size(); i++) {
		auto &plan = filter_plans[i];
		auto &filter = bf_to_create[i];
		filter->Initialize(context, num_rows, plan->bound_cols_apply, plan->bound_cols_build);
	}

	sink.ScheduleFinalize(pipeline, event);
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

	string min_max_filter;
	for (auto &filter : min_max_applied_cols) {
		for (auto &v : filter) {
			min_max_filter += std::to_string(v.table_index) + "." + std::to_string(v.column_index) + " ";
		}
	}
	result["Min-Max Filter"] = min_max_filter;

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

		// set pipeline flag
		this_pipeline->is_building_bf = true;
		this_pipeline->is_probing_side = is_probing_side;

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

		// set pipeline flag
		this_pipeline->is_building_bf = true;
		this_pipeline->is_probing_side = is_probing_side;

		child_meta_pipeline.Build(children[0]);
	} else {
		current.AddDependency(this_pipeline);
	}
}
} // namespace duckdb
