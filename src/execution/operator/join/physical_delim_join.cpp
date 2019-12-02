#include "duckdb/execution/operator/join/physical_delim_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"

using namespace duckdb;
using namespace std;

class PhysicalDelimJoinState : public PhysicalOperatorState {
public:
	PhysicalDelimJoinState(PhysicalOperator *left) : PhysicalOperatorState(left) {
	}

	unique_ptr<PhysicalOperatorState> join_state;
};

PhysicalDelimJoin::PhysicalDelimJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> original_join,
                                     vector<PhysicalOperator *> delim_scans)
    : PhysicalOperator(PhysicalOperatorType::DELIM_JOIN, op.types), join(move(original_join)) {
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

void PhysicalDelimJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalDelimJoinState *>(state_);
	assert(distinct);
	if (!state->join_state) {
		// first run: fully materialize the LHS
		ChunkCollection &big_data = lhs_data;
		do {
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			big_data.Append(state->child_chunk);
		} while (state->child_chunk.size() != 0);
		// now create the duplicate eliminated chunk by pulling from the DISTINCT aggregate
		DataChunk delim_chunk;
		distinct->InitializeChunk(delim_chunk);
		auto distinct_state = distinct->GetOperatorState();
		do {
			delim_chunk.Reset();
			distinct->GetChunkInternal(context, delim_chunk, distinct_state.get());
			delim_data.Append(delim_chunk);
		} while (delim_chunk.size() != 0);
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
