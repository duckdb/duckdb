#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <utility>

namespace duckdb {

PhysicalRecursiveCTE::PhysicalRecursiveCTE(PhysicalPlan &physical_plan, string ctename, idx_t table_index,
                                           vector<LogicalType> types, bool union_all, PhysicalOperator &top,
                                           PhysicalOperator &bottom, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::RECURSIVE_CTE, std::move(types), estimated_cardinality),
      ctename(std::move(ctename)), table_index(table_index), union_all(union_all) {
	children.push_back(top);
	children.push_back(bottom);
}

PhysicalRecursiveCTE::~PhysicalRecursiveCTE() {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class RecursiveCTEState : public GlobalSinkState {
public:
	explicit RecursiveCTEState(ClientContext &context, const PhysicalRecursiveCTE &op)
	    : intermediate_table(context, op.GetTypes()), new_groups(STANDARD_VECTOR_SIZE) {
		vector<BoundAggregateExpression *> payload_aggregates_ptr;
		for (idx_t i = 0; i < op.payload_aggregates.size(); i++) {
			auto &dat = op.payload_aggregates[i];
			payload_aggregates_ptr.push_back(dat.get());
		}

		ht = make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), op.distinct_types,
		                                          op.payload_types, payload_aggregates_ptr);
	}

	unique_ptr<GroupedAggregateHashTable> ht;

	mutex intermediate_table_lock;
	ColumnDataCollection intermediate_table;
	ColumnDataScanState scan_state;
	bool initialized = false;
	bool finished_scan = false;
	SelectionVector new_groups;
	AggregateHTScanState ht_scan_state;
};

unique_ptr<GlobalSinkState> PhysicalRecursiveCTE::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<RecursiveCTEState>(context, *this);
}

idx_t PhysicalRecursiveCTE::ProbeHT(DataChunk &chunk, RecursiveCTEState &state) const {
	Vector dummy_addresses(LogicalType::POINTER);

	// Use the HT to eliminate duplicate rows
	idx_t new_group_count = state.ht->FindOrCreateGroups(chunk, dummy_addresses, state.new_groups);

	// we only return entries we have not seen before (i.e. new groups)
	chunk.Slice(state.new_groups, new_group_count);

	return new_group_count;
}

void PopulateChunk(DataChunk &group_chunk, DataChunk &input_chunk, const vector<idx_t> &idx_set, bool reference) {
	idx_t chunk_index = 0;
	// Populate the group_chunk
	for (auto &group_idx : idx_set) {
		if (reference) {
			// Reference from input_chunk[chunk_index] -> group_chunk[group_idx]
			group_chunk.data[chunk_index++].Reference(input_chunk.data[group_idx]);
		} else {
			// Reference from input_chunk[group.index] -> group_chunk[chunk_index]
			group_chunk.data[group_idx].Reference(input_chunk.data[chunk_index++]);
		}
	}
	group_chunk.SetCardinality(input_chunk.size());
}

SinkResultType PhysicalRecursiveCTE::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<RecursiveCTEState>();

	lock_guard<mutex> guard(gstate.intermediate_table_lock);
	if (!using_key) {
		if (!union_all) {
			idx_t match_count = ProbeHT(chunk, gstate);
			if (match_count > 0) {
				gstate.intermediate_table.Append(chunk);
			}
		} else {
			gstate.intermediate_table.Append(chunk);
		}
	} else {
		// Split incoming DataChunk into payload and keys
		DataChunk distinct_rows;
		distinct_rows.Initialize(Allocator::DefaultAllocator(), distinct_types);
		PopulateChunk(distinct_rows, chunk, distinct_idx, true);
		DataChunk payload_rows;
		if (!payload_types.empty()) {
			payload_rows.Initialize(Allocator::DefaultAllocator(), payload_types);
		}
		PopulateChunk(payload_rows, chunk, payload_idx, true);

		// Add the chunk to the hash table and append it to the intermediate table
		gstate.ht->AddChunk(distinct_rows, payload_rows, AggregateType::NON_DISTINCT);
		gstate.intermediate_table.Append(chunk);
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
			gstate.intermediate_table.InitializeScan(gstate.scan_state);
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
				gstate.intermediate_table.Scan(gstate.scan_state, chunk);
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

			// After an iteration, we reset the recurring table
			// and fill it up with the new hash table rows for the next iteration.
			if (using_key && ref_recurring && gstate.intermediate_table.Count() != 0) {
				recurring_table->Reset();
				AggregateHTScanState scan_state;
				gstate.ht->InitializeScan(scan_state);

				// Initialise the DataChunks to read the resulting rows.
				// One DataChunk for the payload, one for the keys.
				// Create a new DataChunk to store the result.
				DataChunk result;
				DataChunk payload_rows;
				DataChunk distinct_rows;
				distinct_rows.Initialize(Allocator::DefaultAllocator(), distinct_types);
				if (!payload_types.empty()) {
					payload_rows.Initialize(Allocator::DefaultAllocator(), payload_types);
				}
				result.Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());

				while (gstate.ht->Scan(scan_state, distinct_rows, payload_rows)) {
					// Populate the result DataChunk with the keys and the payload.
					PopulateChunk(result, distinct_rows, distinct_idx, false);
					PopulateChunk(result, payload_rows, payload_idx, false);
					// Append the result to the recurring table.
					recurring_table->Append(result);
				}
			}

			working_table->Reset();
			working_table->Combine(gstate.intermediate_table);
			// and we clear the intermediate table
			gstate.finished_scan = false;
			gstate.intermediate_table.Reset();
			// now we need to re-execute all of the pipelines that depend on the recursion
			ExecuteRecursivePipelines(context);

			// check if we obtained any results
			// if not, we are done
			if (gstate.intermediate_table.Count() == 0) {
				gstate.finished_scan = true;
				if (using_key) {
					// Initialise the DataChunks to read the ht.
					// One DataChunk for payload, one for keys.
					DataChunk payload_rows;
					DataChunk distinct_rows;
					distinct_rows.Initialize(Allocator::DefaultAllocator(), distinct_types);
					if (!payload_types.empty()) {
						payload_rows.Initialize(Allocator::DefaultAllocator(), payload_types);
					}

					gstate.ht->Scan(gstate.ht_scan_state, distinct_rows, payload_rows);
					PopulateChunk(chunk, distinct_rows, distinct_idx, false);
					PopulateChunk(chunk, payload_rows, payload_idx, false);
				}
				break;
			}
			if (!using_key) {
				// set up the scan again
				gstate.intermediate_table.InitializeScan(gstate.scan_state);
			}
		}
	}

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

void PhysicalRecursiveCTE::ExecuteRecursivePipelines(ExecutionContext &context) const {
	if (!recursive_meta_pipeline) {
		throw InternalException("Missing meta pipeline for recursive CTE");
	}
	D_ASSERT(recursive_meta_pipeline->HasRecursiveCTE());

	// get and reset pipelines
	vector<shared_ptr<Pipeline>> pipelines;
	recursive_meta_pipeline->GetPipelines(pipelines, true);
	for (auto &pipeline : pipelines) {
		auto sink = pipeline->GetSink();
		if (sink.get() != this) {
			sink->sink_state.reset();
		}
		for (auto &op_ref : pipeline->GetOperators()) {
			auto &op = op_ref.get();
			op.op_state.reset();
		}
		pipeline->ClearSource();
	}

	// get the MetaPipelines in the recursive_meta_pipeline and reschedule them
	vector<shared_ptr<MetaPipeline>> meta_pipelines;
	recursive_meta_pipeline->GetMetaPipelines(meta_pipelines, true, false);
	auto &executor = recursive_meta_pipeline->GetExecutor();
	vector<shared_ptr<Event>> events;
	executor.ReschedulePipelines(meta_pipelines, events);

	while (true) {
		executor.WorkOnTasks();
		if (executor.HasError()) {
			executor.ThrowException();
		}
		bool finished = true;
		for (auto &event : events) {
			if (!event->IsFinished()) {
				finished = false;
				break;
			}
		}
		if (finished) {
			// all pipelines finished: done!
			break;
		}
	}
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//

static void GatherColumnDataScans(const PhysicalOperator &op, vector<const_reference<PhysicalOperator>> &delim_scans) {
	if (op.type == PhysicalOperatorType::DELIM_SCAN || op.type == PhysicalOperatorType::CTE_SCAN) {
		delim_scans.push_back(op);
	}
	for (auto &child : op.children) {
		GatherColumnDataScans(child, delim_scans);
	}
}

void PhysicalRecursiveCTE::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	sink_state.reset();
	recursive_meta_pipeline.reset();

	auto &state = meta_pipeline.GetState();
	state.SetPipelineSource(current, *this);

	auto &executor = meta_pipeline.GetExecutor();
	executor.AddRecursiveCTE(*this);

	// the LHS of the recursive CTE is our initial state
	auto &initial_state_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	initial_state_pipeline.Build(children[0]);

	// the RHS is the recursive pipeline
	recursive_meta_pipeline = make_shared_ptr<MetaPipeline>(executor, state, this);
	recursive_meta_pipeline->SetRecursiveCTE();
	recursive_meta_pipeline->Build(children[1]);

	vector<const_reference<PhysicalOperator>> ops;
	GatherColumnDataScans(children[1], ops);

	for (auto op : ops) {
		auto entry = state.cte_dependencies.find(op);
		if (entry == state.cte_dependencies.end()) {
			continue;
		}
		// this chunk scan introduces a dependency to the current pipeline
		// namely a dependency on the CTE pipeline to finish
		auto cte_dependency = entry->second.get().shared_from_this();
		current.AddDependency(cte_dependency);
	}
}

vector<const_reference<PhysicalOperator>> PhysicalRecursiveCTE::GetSources() const {
	return {*this};
}

InsertionOrderPreservingMap<string> PhysicalRecursiveCTE::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Name"] = ctename;
	result["Table Index"] = StringUtil::Format("%llu", table_index);
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
