#include "duckdb/execution/operator/set/physical_recursive_key_cte.hpp"

#include "duckdb/execution/aggregate_hashtable.hpp"
#include "iostream"

namespace duckdb {

PhysicalRecursiveKeyCTE::PhysicalRecursiveKeyCTE(string ctename, idx_t table_index, vector<LogicalType> types,
                                           bool union_all, vector<idx_t > key_columns,
                                           unique_ptr<PhysicalOperator> top, unique_ptr<PhysicalOperator> bottom,
                                           idx_t estimated_cardinality)
    : PhysicalRecursiveCTE(ctename, table_index, types, union_all, std::move(top), std::move(bottom),
                           estimated_cardinality), key_columns(std::move(key_columns)) {
}

PhysicalRecursiveKeyCTE::~PhysicalRecursiveKeyCTE() {
}

class RecursiveKeyCTEState : public GlobalSinkState {
public:
	explicit RecursiveKeyCTEState(ClientContext &context, const PhysicalRecursiveKeyCTE &op)
	    : intermediate_table(context, op.GetTypes()), new_groups(STANDARD_VECTOR_SIZE) {
		ht = make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), op.types,
		                                          vector<LogicalType>(), vector<BoundAggregateExpression *>());
		recurring_ht = make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), op.types,
		                                                    vector<LogicalType>(), vector<BoundAggregateExpression *>());
	}

	unique_ptr<GroupedAggregateHashTable> ht;
	// btodo: bad naming change later
	unique_ptr<GroupedAggregateHashTable> recurring_ht;

	bool intermediate_empty = true;
	ColumnDataCollection intermediate_table;
	ColumnDataScanState scan_state;
	bool initialized = false;
	bool finished_scan = false;
	SelectionVector new_groups;
};

unique_ptr<GlobalSinkState> PhysicalRecursiveKeyCTE::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<RecursiveKeyCTEState>(context, *this);
}

idx_t PhysicalRecursiveKeyCTE::ProbeHT(DataChunk &chunk, RecursiveKeyCTEState &state) const {
	Vector dummy_addresses(LogicalType::POINTER);

	// Adds incoming rows to the recurring ht
	auto new_group_count =
	    state.recurring_ht->FindOrCreateGroupsWithKey(chunk, dummy_addresses, state.new_groups, key_columns);

	// Unlike normal recCTE, which only returns unseen rows,
	// we return all new computed rows
	chunk.Slice(state.new_groups, new_group_count);
	return new_group_count;
}

SinkResultType PhysicalRecursiveKeyCTE::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<RecursiveKeyCTEState>();
	if (!union_all) {
		idx_t match_count = ProbeHT(chunk, gstate);
		if (match_count > 0) {
			gstate.intermediate_table.Append(chunk);
		}
	} else {
		gstate.intermediate_table.Append(chunk);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalRecursiveKeyCTE::GetData(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<RecursiveKeyCTEState>();
	if (!gstate.initialized) {
		gstate.intermediate_table.InitializeScan(gstate.scan_state);
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

			// The recurring hash table contains all the rows calculated in this iteration,
			// we can now add all the old rows.
			gstate.recurring_ht->Combine(*gstate.ht, key_columns);
			gstate.ht->Reset();
			gstate.recurring_ht.swap(gstate.ht);

			// After an iteration, we reset the recurring table
			// and fill it up with the new hash table rows for the next iteration.
			recurring_table->Reset();
			DataChunk all_rows;
			all_rows.InitializeEmpty(chunk.GetTypes());
			gstate.ht->FetchAll(all_rows);
			recurring_table->Append(all_rows);

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
				recurring_table->Scan(gstate.scan_state, chunk);
				break;
			}
			// set up the scan again
			gstate.intermediate_table.InitializeScan(gstate.scan_state);
		}
	}
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
