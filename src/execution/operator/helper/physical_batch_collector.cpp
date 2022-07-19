#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"
#include "duckdb/common/types/batched_chunk_collection.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

PhysicalBatchCollector::PhysicalBatchCollector(PreparedStatementData &data) : PhysicalResultCollector(data) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BatchCollectorGlobalState : public GlobalSinkState {
public:
	explicit BatchCollectorGlobalState(Allocator &allocator) : data(allocator) {
	}

	mutex glock;
	BatchedChunkCollection data;
	unique_ptr<MaterializedQueryResult> result;
};

class BatchCollectorLocalState : public LocalSinkState {
public:
	explicit BatchCollectorLocalState(Allocator &allocator) : data(allocator) {
	}

	BatchedChunkCollection data;
};

SinkResultType PhysicalBatchCollector::Sink(ExecutionContext &context, GlobalSinkState &gstate,
                                            LocalSinkState &lstate_p, DataChunk &input) const {
	auto &state = (BatchCollectorLocalState &)lstate_p;
	state.data.Append(input, state.batch_index);
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalBatchCollector::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                     LocalSinkState &lstate_p) const {
	auto &gstate = (BatchCollectorGlobalState &)gstate_p;
	auto &state = (BatchCollectorLocalState &)lstate_p;

	lock_guard<mutex> lock(gstate.glock);
	gstate.data.Merge(state.data);
}

SinkFinalizeType PhysicalBatchCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  GlobalSinkState &gstate_p) const {
	auto &gstate = (BatchCollectorGlobalState &)gstate_p;
	auto result =
	    make_unique<MaterializedQueryResult>(statement_type, properties, types, names, context.shared_from_this());
	DataChunk output;
	output.Initialize(BufferAllocator::Get(context), types);

	BatchedChunkScanState state;
	gstate.data.InitializeScan(state);
	while (true) {
		output.Reset();
		gstate.data.Scan(state, output);
		if (output.size() == 0) {
			break;
		}
		result->collection.Append(output);
	}

	gstate.result = move(result);
	return SinkFinalizeType::READY;
}

unique_ptr<LocalSinkState> PhysicalBatchCollector::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<BatchCollectorLocalState>(Allocator::DefaultAllocator());
}

unique_ptr<GlobalSinkState> PhysicalBatchCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<BatchCollectorGlobalState>(Allocator::DefaultAllocator());
}

unique_ptr<QueryResult> PhysicalBatchCollector::GetResult(GlobalSinkState &state) {
	auto &gstate = (BatchCollectorGlobalState &)state;
	D_ASSERT(gstate.result);
	return move(gstate.result);
}

} // namespace duckdb
