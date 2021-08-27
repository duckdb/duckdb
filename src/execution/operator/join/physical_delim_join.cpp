#include "duckdb/execution/operator/join/physical_delim_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/task_context.hpp"

namespace duckdb {

// class PhysicalDelimJoinState : public OperatorState {
// public:
// 	PhysicalDelimJoinState(PhysicalOperator &op, PhysicalOperator *left) : OperatorState(op, left) {
// 	}

// 	unique_ptr<OperatorState> join_state;
// };

PhysicalDelimJoin::PhysicalDelimJoin(vector<LogicalType> types, unique_ptr<PhysicalOperator> original_join,
                                     vector<PhysicalOperator *> delim_scans, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::DELIM_JOIN, move(types), estimated_cardinality), join(move(original_join)),
      delim_scans(move(delim_scans)) {
	D_ASSERT(join->children.size() == 2);
	// now for the original join
	// we take its left child, this is the side that we will duplicate eliminate
	children.push_back(move(join->children[0]));

	// we replace it with a PhysicalChunkCollectionScan, that scans the ChunkCollection that we keep cached
	// the actual chunk collection to scan will be created in the DelimJoinGlobalState
	auto cached_chunk_scan = make_unique<PhysicalChunkScan>(children[0]->GetTypes(), PhysicalOperatorType::CHUNK_SCAN,
	                                                        estimated_cardinality);
	join->children[0] = move(cached_chunk_scan);
	throw InternalException("FIXME: delim join");
}

class DelimJoinGlobalState : public GlobalSinkState {
public:
	explicit DelimJoinGlobalState(const PhysicalDelimJoin *delim_join) {
		D_ASSERT(delim_join->delim_scans.size() > 0);
		// for any duplicate eliminated scans in the RHS, point them to the duplicate eliminated chunk that we create
		// here
		for (auto op : delim_join->delim_scans) {
			D_ASSERT(op->type == PhysicalOperatorType::DELIM_SCAN);
			auto scan = (PhysicalChunkScan *)op;
			scan->collection = &delim_data;
		}
		// set up the delim join chunk to scan in the original join
		auto &cached_chunk_scan = (PhysicalChunkScan &)*delim_join->join->children[0];
		cached_chunk_scan.collection = &lhs_data;
	}

	ChunkCollection lhs_data;
	ChunkCollection delim_data;
	unique_ptr<GlobalSinkState> distinct_state;
};

unique_ptr<GlobalSinkState> PhysicalDelimJoin::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_unique<DelimJoinGlobalState>(this);
	state->distinct_state = distinct->GetGlobalSinkState(context);
	return move(state);
}

unique_ptr<LocalSinkState> PhysicalDelimJoin::GetLocalSinkState(ExecutionContext &context) const {
	return distinct->GetLocalSinkState(context);
}

void PhysicalDelimJoin::Sink(ExecutionContext &context, GlobalSinkState &state_p, LocalSinkState &lstate,
                             DataChunk &input) const {
	auto &state = (DelimJoinGlobalState &)state_p;
	state.lhs_data.Append(input);
	distinct->Sink(context, *state.distinct_state, lstate, input);
}

bool PhysicalDelimJoin::Finalize(Pipeline &pipeline, ClientContext &client, unique_ptr<GlobalSinkState> state) {
	// auto &dstate = (DelimJoinGlobalState &)*state;
	// // finalize the distinct HT
	// D_ASSERT(distinct);
	// distinct->FinalizeImmediate(client, move(dstate.distinct_state));
	// // materialize the distinct collection
	// DataChunk delim_chunk;
	// distinct->InitializeChunk(delim_chunk);
	// auto distinct_state = distinct->GetOperatorState();
	// ThreadContext thread(client);
	// TaskContext task;
	// ExecutionContext context(client, thread, task);
	// while (true) {
	// 	distinct->GetChunk(context, delim_chunk, distinct_state.get());
	// 	if (delim_chunk.size() == 0) {
	// 		break;
	// 	}
	// 	dstate.delim_data.Append(delim_chunk);
	// }
	// PhysicalOperator::Finalize(pipeline, client, move(state));
	// return true;
	return true;
}

void PhysicalDelimJoin::Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const {
	auto &dstate = (DelimJoinGlobalState &)state;
	distinct->Combine(context, *dstate.distinct_state, lstate);
}

// void PhysicalDelimJoin::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
//                                          OperatorState *state_p) const {
// 	auto state = reinterpret_cast<PhysicalDelimJoinState *>(state_p);
// 	if (!state->join_state) {
// 		// create the state of the underlying join
// 		state->join_state = join->GetOperatorState();
// 	}
// 	// now pull from the RHS from the underlying join
// 	join->GetChunk(context, chunk, state->join_state.get());
// }

// unique_ptr<OperatorState> PhysicalDelimJoin::GetOperatorState() {
// 	return make_unique<PhysicalDelimJoinState>(*this, children[0].get());
// }

string PhysicalDelimJoin::ParamsToString() const {
	return join->ParamsToString();
}

} // namespace duckdb
