#include "duckdb/execution/operator/join/physical_delim_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"

using namespace std;

namespace duckdb {

class PhysicalDelimJoinState : public PhysicalOperatorState {
public:
	PhysicalDelimJoinState(PhysicalOperator *left) : PhysicalOperatorState(left) {
	}

	unique_ptr<PhysicalOperatorState> join_state;
};

PhysicalDelimJoin::PhysicalDelimJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> original_join,
                                     vector<PhysicalOperator *> delim_scans)
    : PhysicalSink(PhysicalOperatorType::DELIM_JOIN, op.types), join(move(original_join)) {
	assert(delim_scans.size() > 0);
	assert(join->children.size() == 2);
	// for any duplicate eliminated scans in the RHS, point them to the duplicate eliminated chunk that we create here
	for (auto op : delim_scans) {
		assert(op->type == PhysicalOperatorType::DELIM_SCAN);
		auto scan = (PhysicalChunkScan *)op;
		scan->collection = &delim_data;
	}
	// now for the original join
	// we take its left child, this is the side that we will duplicate eliminate
	children.push_back(move(join->children[0]));
	// we replace it with a PhysicalChunkCollectionScan, that scans the ChunkCollection that we keep cached
	auto cached_chunk_scan = make_unique<PhysicalChunkScan>(children[0]->GetTypes(), PhysicalOperatorType::CHUNK_SCAN);
	cached_chunk_scan->collection = &lhs_data;
	join->children[0] = move(cached_chunk_scan);
}

unique_ptr<GlobalOperatorState> PhysicalDelimJoin::GetGlobalState(ClientContext &context) {
	return distinct->GetGlobalState(context);
}

unique_ptr<LocalSinkState> PhysicalDelimJoin::GetLocalSinkState(ClientContext &context) {
	return distinct->GetLocalSinkState(context);
}

void PhysicalDelimJoin::Sink(ClientContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                             DataChunk &input) {
	lhs_data.Append(input);
	distinct->Sink(context, state, lstate, input);
}

void PhysicalDelimJoin::Finalize(ClientContext &context, unique_ptr<GlobalOperatorState> state) {
	// finalize the distinct HT
	distinct->Finalize(context, move(state));
	// materialize the distinct collection
	DataChunk delim_chunk;
	distinct->InitializeChunk(delim_chunk);
	auto distinct_state = distinct->GetOperatorState();
	while (true) {
		distinct->GetChunk(context, delim_chunk, distinct_state.get());
		if (delim_chunk.size() == 0) {
			break;
		}
		delim_data.Append(delim_chunk);
	}
}

void PhysicalDelimJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalDelimJoinState *>(state_);
	if (!state->join_state) {
		// create the state of the underlying join
		state->join_state = join->GetOperatorState();
	}
	// now pull from the RHS from the underlying join
	join->GetChunk(context, chunk, state->join_state.get());
}

unique_ptr<PhysicalOperatorState> PhysicalDelimJoin::GetOperatorState() {
	return make_unique<PhysicalDelimJoinState>(children[0].get());
}

string PhysicalDelimJoin::ExtraRenderInformation() const {
	return join->ExtraRenderInformation();
}

} // namespace duckdb
