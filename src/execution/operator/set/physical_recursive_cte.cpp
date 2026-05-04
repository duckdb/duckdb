#include "duckdb/execution/operator/set/physical_recursive_cte_state.hpp"

#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/main/settings.hpp"

namespace duckdb {

PhysicalRecursiveCTE::PhysicalRecursiveCTE(PhysicalPlan &physical_plan, string ctename, TableIndex table_index,
                                           vector<LogicalType> types, bool union_all, PhysicalOperator &top,
                                           PhysicalOperator &bottom, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::RECURSIVE_CTE, std::move(types), estimated_cardinality),
      ctename(std::move(ctename)), table_index(table_index), union_all(union_all),
      shared_executor_pool(make_shared_ptr<RecursiveExecutorPool>()) {
	children.push_back(top);
	children.push_back(bottom);
}

PhysicalRecursiveCTE::~PhysicalRecursiveCTE() {
}

//===--------------------------------------------------------------------===//
// Sink State
//===--------------------------------------------------------------------===//
RecursiveCTEState::RecursiveCTEState(ClientContext &context, const PhysicalRecursiveCTE &op)
    : op(op), executor(context), allow_executor_reuse(Settings::Get<EnableCachingOperatorsSetting>(context)),
      executor_pool(op.shared_executor_pool),
      intermediate_table(context, op.using_key ? op.internal_types : op.GetTypes()), new_groups(STANDARD_VECTOR_SIZE),
      dummy_addresses(LogicalType::POINTER) {
	vector<LogicalType> aggr_input_types;
	vector<AggregateObject> payload_aggregates;
	for (idx_t i = 0; i < op.payload_aggregates.size(); i++) {
		D_ASSERT(op.payload_aggregates[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
		auto &bound_aggr_expr = op.payload_aggregates[i]->Cast<BoundAggregateExpression>();
		for (auto &child_expr : bound_aggr_expr.children) {
			executor.AddExpression(*child_expr);
			aggr_input_types.push_back(child_expr->GetReturnType());
		}
		payload_aggregates.emplace_back(bound_aggr_expr);
	}

	payload_rows.Initialize(Allocator::Get(context), aggr_input_types);

	ht = make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), op.distinct_types,
	                                          op.payload_types, std::move(payload_aggregates));
	if (op.using_key) {
		distinct_rows.Initialize(Allocator::DefaultAllocator(), op.distinct_types);
		source_distinct_rows.Initialize(Allocator::DefaultAllocator(), op.distinct_types);
		source_payload_rows.Initialize(Allocator::DefaultAllocator(), op.payload_types);
	}
	source_result.Initialize(Allocator::DefaultAllocator(), op.GetTypes());
	InitializeIntermediateAppend();
	op.working_table->InitializeAppend(working_append_state);
	if (op.recurring_table) {
		op.recurring_table->InitializeAppend(recurring_append_state);
	}
}

RecursiveCTEState::~RecursiveCTEState() {
	ClearCachedExecutors();
}

void RecursiveCTEState::InitializeIntermediateAppend() {
	intermediate_table.InitializeAppend(intermediate_append_state);
}

void RecursiveCTEState::ResetRecurringTable() {
	D_ASSERT(op.recurring_table);
	op.recurring_table->Reset();
	op.recurring_table->InitializeAppend(recurring_append_state);
}

ColumnDataCollection &RecursiveCTEState::CurrentOutputTable() {
	if (op.using_key || !output_is_working) {
		return intermediate_table;
	}
	D_ASSERT(op.working_table);
	return *op.working_table;
}

ColumnDataCollection &RecursiveCTEState::CurrentInputTable() {
	if (op.using_key) {
		D_ASSERT(op.working_table);
		return *op.working_table;
	}
	if (output_is_working) {
		return intermediate_table;
	}
	D_ASSERT(op.working_table);
	return *op.working_table;
}

const ColumnDataCollection &RecursiveCTEState::CurrentInputTable() const {
	if (op.using_key) {
		D_ASSERT(op.working_table);
		return *op.working_table;
	}
	if (output_is_working) {
		return intermediate_table;
	}
	D_ASSERT(op.working_table);
	return *op.working_table;
}

ColumnDataAppendState &RecursiveCTEState::CurrentOutputAppendState() {
	if (op.using_key || !output_is_working) {
		return intermediate_append_state;
	}
	return working_append_state;
}

void RecursiveCTEState::AdvanceIterationBuffers() {
	if (!op.using_key) {
		output_is_working = !output_is_working;
	}
}

void RecursiveCTEState::ResetCurrentOutputTableForReuse() {
	auto &output = CurrentOutputTable();
	output.ResetForReuse();
	if (op.using_key || !output_is_working) {
		InitializeIntermediateAppend();
	} else {
		D_ASSERT(op.working_table);
		op.working_table->InitializeAppend(working_append_state);
	}
}

void RecursiveCTEState::RebindRecursiveScans() {
	if (op.using_key) {
		return;
	}
	auto &input_table = CurrentInputTable();
	for (auto &scan_ref : op.recursive_scans) {
		auto &scan = scan_ref.get();
		scan.collection = input_table;
	}
}

vector<unique_ptr<PipelineExecutor>> &RecursiveCTEState::GetCachedExecutors(Pipeline &pipeline, idx_t max_threads) {
	// Ordinary pipelines build PipelineExecutors once and discard them after the query finishes.
	// Recursive CTEs re-enter the same pipelines many times, and correlated recursive invocations can
	// construct several RecursiveCTEState objects for the same physical plan. Keep a state-local cache
	// for cheap per-iteration reuse, and spill back into a shared pool so later states can recycle the
	// already-initialized executors instead of reconstructing them from scratch.
	lock_guard<mutex> guard(cached_executor_lock);
	auto entry = cached_executors.find(pipeline);
	if (entry == cached_executors.end()) {
		entry = cached_executors.emplace(reference<Pipeline>(pipeline), vector<unique_ptr<PipelineExecutor>>()).first;
	}
	auto &executors = entry->second;
	if (!allow_executor_reuse) {
		while (executors.size() < max_threads) {
			executors.push_back(make_uniq<PipelineExecutor>(pipeline.GetClientContext(), pipeline));
		}
		return executors;
	}
	D_ASSERT(executor_pool);
	// Lock order: cached_executor_lock -> executor_pool->lock.
	lock_guard<mutex> pool_guard(executor_pool->lock);
	auto pool_entry = executor_pool->executors.find(pipeline);
	if (pool_entry == executor_pool->executors.end()) {
		pool_entry =
		    executor_pool->executors.emplace(reference<Pipeline>(pipeline), vector<unique_ptr<PipelineExecutor>>())
		        .first;
	}
	auto &shared_executors = pool_entry->second;
	while (executors.size() < max_threads) {
		if (!shared_executors.empty()) {
			executors.push_back(std::move(shared_executors.back()));
			shared_executors.pop_back();
		} else {
			executors.push_back(make_uniq<PipelineExecutor>(pipeline.GetClientContext(), pipeline));
		}
	}
	return executors;
}

void RecursiveCTEState::ClearCachedExecutors() {
	lock_guard<mutex> guard(cached_executor_lock);
	if (cached_executors.empty()) {
		return;
	}
	if (!allow_executor_reuse) {
		cached_executors.clear();
		return;
	}
	D_ASSERT(executor_pool);
	// Lock order: cached_executor_lock -> executor_pool->lock.
	lock_guard<mutex> pool_guard(executor_pool->lock);
	for (auto &entry : cached_executors) {
		auto pool_entry = executor_pool->executors.find(entry.first.get());
		if (pool_entry == executor_pool->executors.end()) {
			pool_entry = executor_pool->executors.emplace(entry.first, vector<unique_ptr<PipelineExecutor>>()).first;
		}
		auto &shared_executors = pool_entry->second;
		for (auto &executor : entry.second) {
			shared_executors.push_back(std::move(executor));
		}
	}
	cached_executors.clear();
}

unique_ptr<GlobalSinkState> PhysicalRecursiveCTE::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<RecursiveCTEState>(context, *this);
}

idx_t PhysicalRecursiveCTE::ProbeHT(DataChunk &chunk, RecursiveCTEState &state) const {
	// Use the HT to eliminate duplicate rows
	idx_t new_group_count = state.ht->FindOrCreateGroups(chunk, state.dummy_addresses, state.new_groups);

	// we only return entries we have not seen before (i.e. new groups)
	chunk.Slice(state.new_groups, new_group_count);

	return new_group_count;
}

static void GatherChunk(DataChunk &output_chunk, DataChunk &input_chunk, const vector<idx_t> &idx_set) {
	idx_t chunk_index = 0;
	for (auto &group_idx : idx_set) {
		output_chunk.data[chunk_index++].Reference(input_chunk.data[group_idx]);
	}
	output_chunk.SetCardinality(input_chunk.size());
}

static void ScatterChunk(DataChunk &output_chunk, DataChunk &input_chunk, const vector<idx_t> &idx_set) {
	idx_t chunk_index = 0;
	for (auto &group_idx : idx_set) {
		output_chunk.data[group_idx].Reference(input_chunk.data[chunk_index++]);
	}
	output_chunk.SetCardinality(input_chunk.size());
}

SinkResultType PhysicalRecursiveCTE::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<RecursiveCTEState>();

	lock_guard<mutex> guard(gstate.intermediate_table_lock);
	if (!using_key) {
		auto &output = gstate.CurrentOutputTable();
		auto &append_state = gstate.CurrentOutputAppendState();
		if (!union_all) {
			idx_t match_count = ProbeHT(chunk, gstate);
			if (match_count > 0) {
				output.Append(append_state, chunk);
			}
		} else {
			output.Append(append_state, chunk);
		}
	} else {
		// Split incoming DataChunk into payload and keys using the cached distinct_rows chunk
		gstate.distinct_rows.Reset();
		GatherChunk(gstate.distinct_rows, chunk, distinct_idx);

		// Add result of recursive anchor to intermediate table
		gstate.intermediate_table.Append(gstate.intermediate_append_state, chunk);

		// Execute aggregate expressions on chunk if any
		if (!gstate.executor.expressions.empty()) {
			gstate.payload_rows.Reset();
			gstate.executor.Execute(chunk, gstate.payload_rows);
		}

		// Add the result of the executed expressions to the hash table
		gstate.ht->AddChunk(gstate.distinct_rows, gstate.payload_rows, AggregateType::NON_DISTINCT);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalRecursiveCTE::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<RecursiveCTEState>();
	if (!gstate.initialized) {
		if (!using_key) {
			gstate.CurrentOutputTable().InitializeScan(gstate.scan_state);
		} else {
			gstate.ht->InitializeScan(gstate.ht_scan_state);
			recurring_table->InitializeScan(gstate.scan_state);
		}
		gstate.finished_scan = false;
		gstate.initialized = true;
	}
	while (chunk.size() == 0) {
		if (!gstate.finished_scan) {
			if (!using_key) {
				// scan any chunks we have collected so far
				gstate.CurrentOutputTable().Scan(gstate.scan_state, chunk);
			}
			if (chunk.size() == 0) {
				gstate.finished_scan = true;
			} else {
				break;
			}
		} else {
			// we have run out of chunks
			// now we need to recurse
			// we set up the working table as the data we gathered in this iteration of the recursion
			auto &current_output = gstate.CurrentOutputTable();

			// After an iteration, we reset the recurring table
			// and fill it up with the new hash table rows for the next iteration.
			if (using_key && ref_recurring && current_output.Count() != 0) {
				gstate.ResetRecurringTable();
				AggregateHTScanState scan_state;
				gstate.ht->InitializeScan(scan_state);
				auto &result = gstate.source_result;
				auto &payload_rows = gstate.source_payload_rows;
				auto &distinct_rows = gstate.source_distinct_rows;

				while (gstate.ht->Scan(scan_state, distinct_rows, payload_rows)) {
					result.Reset();
					// Populate the result DataChunk with the keys and the payload.
					ScatterChunk(result, distinct_rows, distinct_idx);
					ScatterChunk(result, payload_rows, payload_idx);
					// Append the result to the recurring table.
					recurring_table->Append(gstate.recurring_append_state, result);
				}
			} else if (ref_recurring && current_output.Count() != 0) {
				// we need to populate the recurring table from the intermediate table
				// careful: we can not just use Combine here, because this destroys the intermediate table
				// instead we need to scan and append to create a copy
				// Note: as we are in the "normal" recursion case here, not the USING KEY case,
				// we can just scan the intermediate table directly, instead of going through the HT
				ColumnDataScanState scan_state;
				current_output.InitializeScan(scan_state);
				while (current_output.Scan(scan_state, gstate.source_result)) {
					recurring_table->Append(gstate.recurring_append_state, gstate.source_result);
				}
			}

			gstate.finished_scan = false;
			if (!using_key) {
				gstate.AdvanceIterationBuffers();
				gstate.ResetCurrentOutputTableForReuse();
				gstate.RebindRecursiveScans();
			} else {
				working_table->Reset();
				working_table->Combine(gstate.intermediate_table);
				gstate.InitializeIntermediateAppend();
			}

			// Pre-grow the dedup HT to avoid costly Resize + ReinsertTuples during the next Sink phase.
			// current_output.Count() is the count of rows output in the previous iteration — an upper bound
			// on the number of new unique rows the next iteration can add (since the recursion is converging).
			if (!union_all) {
				const idx_t expected_new = current_output.Count();
				if (expected_new > 0) {
					const idx_t desired_capacity =
					    GroupedAggregateHashTable::GetCapacityForCount(gstate.ht->Count() + expected_new);
					if (desired_capacity > gstate.ht->Capacity()) {
						gstate.ht->Resize(desired_capacity);
					}
				}
			}

			// now we need to re-execute all of the pipelines that depend on the recursion
			ExecuteRecursivePipelines(context);

			// check if we obtained any results
			// if not, we are done
			if (gstate.CurrentOutputTable().Count() == 0) {
				gstate.finished_scan = true;
				if (using_key) {
					auto &payload_rows = gstate.source_payload_rows;
					auto &distinct_rows = gstate.source_distinct_rows;
					distinct_rows.Reset();
					payload_rows.Reset();
					gstate.ht->Scan(gstate.ht_scan_state, distinct_rows, payload_rows);
					ScatterChunk(chunk, distinct_rows, distinct_idx);
					ScatterChunk(chunk, payload_rows, payload_idx);
				}
				break;
			}
			if (!using_key) {
				// set up the scan again
				gstate.CurrentOutputTable().InitializeScan(gstate.scan_state);
			}
		}
	}

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

vector<const_reference<PhysicalOperator>> PhysicalRecursiveCTE::GetSources() const {
	return {*this};
}

InsertionOrderPreservingMap<string> PhysicalRecursiveCTE::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Name"] = ctename;
	result["Table Index"] = StringUtil::Format("%llu", table_index.index);
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
