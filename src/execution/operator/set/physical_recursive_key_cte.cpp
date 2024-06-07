#include "duckdb/execution/operator/set/physical_recursive_key_cte.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/perfect_aggregate_hashtable.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "iostream"

namespace duckdb {

PhysicalRecursiveKeyCTE::PhysicalRecursiveKeyCTE(string ctename, idx_t table_index, vector<LogicalType> types,
                                                 bool union_all, unique_ptr<PhysicalOperator> top,
                                                 unique_ptr<PhysicalOperator> bottom, idx_t estimated_cardinality)
    : PhysicalRecursiveCTE(std::move(ctename), table_index, std::move(types), union_all, std::move(top),
                           std::move(bottom), estimated_cardinality) {
}

PhysicalRecursiveKeyCTE::~PhysicalRecursiveKeyCTE() {
}

class RecursiveKeyCTEState : public GlobalSinkState {
public:
	explicit RecursiveKeyCTEState(ClientContext &context, const PhysicalRecursiveKeyCTE &op)
	    : intermediate_table(context, op.GetTypes()), new_groups(STANDARD_VECTOR_SIZE) {

		vector<BoundAggregateExpression*> payload_aggregates_ptr;
		for (idx_t i = 0; i < op.payload_aggregates.size(); i++) {
			auto& dat = op.payload_aggregates[i];
			payload_aggregates_ptr.push_back(dat.get());
		}
		// We need to add the payload types
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
};

unique_ptr<GlobalSinkState> PhysicalRecursiveKeyCTE::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<RecursiveKeyCTEState>(context, *this);
}

void PopulateGroupChunk(DataChunk &group_chunk, DataChunk &input_chunk, vector<idx_t> idx_set) {
	idx_t chunk_index = 0;
	// Populate the group_chunk
	for (auto &group_idx : idx_set) {
		// Reference from input_chunk[group.index] -> group_chunk[chunk_index]
		group_chunk.data[chunk_index++].Reference(input_chunk.data[group_idx]);
	}
	group_chunk.SetCardinality(input_chunk.size());
}

SinkResultType PhysicalRecursiveKeyCTE::Sink(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSinkInput &input) const {

	auto &gstate = input.global_state.Cast<RecursiveKeyCTEState>();
 	lock_guard<mutex> guard(gstate.intermediate_table_lock);

	// Split incoming DataChunk into payload and keys
	DataChunk distinct_rows;
	distinct_rows.Initialize(Allocator::DefaultAllocator(), distinct_types);
	PopulateGroupChunk(distinct_rows, chunk, distinct_idx);
	DataChunk payload_rows;
	payload_rows.Initialize(Allocator::DefaultAllocator(), payload_types);
	PopulateGroupChunk(payload_rows, chunk, payload_idx);

	// add new rows and update payload
	gstate.ht->AddChunk(distinct_rows, payload_rows, AggregateType::NON_DISTINCT);
	gstate.intermediate_table.Append(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

void PopulateResultChunk(DataChunk &group_chunk, DataChunk &input_chunk, vector<idx_t> idx_set) {
	idx_t chunk_index = 0;
	// Populate the group_chunk
	for (auto &group_idx : idx_set) {
		// Reference from input_chunk[group.index] -> group_chunk[chunk_index]
		group_chunk.data[group_idx].Reference(input_chunk.data[chunk_index++]);
	}
	group_chunk.SetCardinality(input_chunk.size());
}

SourceResultType PhysicalRecursiveKeyCTE::GetData(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {

	auto &gstate = sink_state->Cast<RecursiveKeyCTEState>();
	if (!gstate.initialized) {
		recurring_table->InitializeScan(gstate.scan_state);
		gstate.finished_scan = false;
		gstate.initialized = true;
	}
	while (chunk.size() == 0) {
		if (!gstate.finished_scan) {
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
			if (gstate.intermediate_table.Count() != 0) {
				recurring_table->Reset();

				// btodo: find better solution
				idx_t size = gstate.ht->Count() > STANDARD_VECTOR_SIZE ? gstate.ht->Count() : STANDARD_VECTOR_SIZE;
				// Initialise the DataChunks to read the resulting rows.
				// One DataChunk for the payload, one for the keys.
				DataChunk payload_rows;
				DataChunk distinct_rows;
				distinct_rows.Initialize(Allocator::DefaultAllocator(), distinct_types, size);
				payload_rows.Initialize(Allocator::DefaultAllocator(), payload_types, size);

				// Collect all currently available keys and their payload.
				gstate.ht->FetchAll(distinct_rows, payload_rows);

				DataChunk result;
				result.Initialize(Allocator::DefaultAllocator(), chunk.GetTypes(), size);
				PopulateResultChunk(result, payload_rows, payload_idx);
				PopulateResultChunk(result, distinct_rows, distinct_idx);

				recurring_table->Append(result);
			}

			// filling working table
			working_table->Reset();
			working_table->Combine(gstate.intermediate_table);
			gstate.finished_scan = false;

			// and we clear the intermediate table
			gstate.intermediate_table.Reset();
			// now we need to re-execute all of the pipelines that depend on the recursion
			ExecuteRecursivePipelines(context);
			// check if we obtained any results
			// if not, we are done

			if (gstate.intermediate_table.Count() == 0) {
				gstate.finished_scan = true;
				recurring_table->Scan(gstate.scan_state, chunk);
				break;
			}
		}
	}
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
