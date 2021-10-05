#include "duckdb/execution/operator/join/physical_delim_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/parallel/thread_context.hpp"

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
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class DelimJoinGlobalState : public GlobalSinkState {
public:
	explicit DelimJoinGlobalState(const PhysicalDelimJoin *delim_join) {
		D_ASSERT(delim_join->delim_scans.size() > 0);
		// set up the delim join chunk to scan in the original join
		auto &cached_chunk_scan = (PhysicalChunkScan &)*delim_join->join->children[0];
		cached_chunk_scan.collection = &lhs_data;
	}

	ChunkCollection lhs_data;
};

unique_ptr<GlobalSinkState> PhysicalDelimJoin::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_unique<DelimJoinGlobalState>(this);
	distinct->sink_state = distinct->GetGlobalSinkState(context);
	if (delim_scans.size() > 1) {
		PhysicalHashAggregate::SetMultiScan(*distinct->sink_state);
	}
	return move(state);
}

unique_ptr<LocalSinkState> PhysicalDelimJoin::GetLocalSinkState(ExecutionContext &context) const {
	return distinct->GetLocalSinkState(context);
}

SinkResultType PhysicalDelimJoin::Sink(ExecutionContext &context, GlobalSinkState &state_p, LocalSinkState &lstate,
                                       DataChunk &input) const {
	auto &state = (DelimJoinGlobalState &)state_p;
	state.lhs_data.Append(input);
	distinct->Sink(context, *distinct->sink_state, lstate, input);
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalDelimJoin::Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const {
	distinct->Combine(context, *distinct->sink_state, lstate);
}

void PhysicalDelimJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &client,
                                 GlobalSinkState &gstate) const {
	// finalize the distinct HT
	D_ASSERT(distinct);
	distinct->Finalize(pipeline, event, client, *distinct->sink_state);
}

string PhysicalDelimJoin::ParamsToString() const {
	return join->ParamsToString();
}

} // namespace duckdb
