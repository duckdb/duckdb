#include "duckdb/execution/operator/order/physical_top_n.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class TopNGlobalState : public GlobalOperatorState {
public:
	mutex lock;
	ChunkCollection big_data;
	unique_ptr<idx_t[]> heap;
	idx_t heap_size;
};

class TopNLocalState : public LocalSinkState {
public:
	ChunkCollection big_data;
};

unique_ptr<LocalSinkState> PhysicalTopN::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<TopNLocalState>();
}

unique_ptr<GlobalOperatorState> PhysicalTopN::GetGlobalState(ClientContext &context) {
	return make_unique<TopNGlobalState>();
}

void PhysicalTopN::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                        DataChunk &input) {
	// append to the local sink state
	auto &sink = (TopNLocalState &)lstate;
	sink.big_data.Append(input);
}

unique_ptr<idx_t[]> PhysicalTopN::ComputeTopN(ChunkCollection &big_data, idx_t &heap_size) {
	// now perform the actual ordering of the data
	// compute the sorting columns from the input data
	ExpressionExecutor executor;
	vector<LogicalType> sort_types;
	vector<OrderType> order_types;
	vector<OrderByNullType> null_order_types;
	for (idx_t i = 0; i < orders.size(); i++) {
		auto &expr = orders[i].expression;
		sort_types.push_back(expr->return_type);
		order_types.push_back(orders[i].type);
		null_order_types.push_back(orders[i].null_order);
		executor.AddExpression(*expr);
	}

	heap_size = (big_data.Count() > offset) ? MinValue<idx_t>(limit + offset, big_data.Count()) : 0;
	if (heap_size == 0) {
		return nullptr;
	}

	ChunkCollection heap_collection;
	for (idx_t i = 0; i < big_data.ChunkCount(); i++) {
		DataChunk heap_chunk;
		heap_chunk.Initialize(sort_types);

		executor.Execute(big_data.GetChunk(i), heap_chunk);
		heap_collection.Append(heap_chunk);
	}

	D_ASSERT(heap_collection.Count() == big_data.Count());

	// create and use the heap
	auto heap = unique_ptr<idx_t[]>(new idx_t[heap_size]);
	heap_collection.Heap(order_types, null_order_types, heap.get(), heap_size);
	return heap;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
void PhysicalTopN::Combine(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_p) {
	auto &gstate = (TopNGlobalState &)state;
	auto &lstate = (TopNLocalState &)lstate_p;

	// first construct the top n of the local sink state
	idx_t local_heap_size;
	auto local_heap = ComputeTopN(lstate.big_data, local_heap_size);
	if (!local_heap) {
		return;
	}

	// now scan the local top N and add it to the global heap
	lock_guard<mutex> glock(gstate.lock);
	idx_t position = 0;
	DataChunk chunk;
	chunk.Initialize(types);
	while (position < local_heap_size) {
		position = lstate.big_data.MaterializeHeapChunk(chunk, local_heap.get(), position, local_heap_size);
		gstate.big_data.Append(chunk);
	}
	gstate.heap_size += local_heap_size;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
void PhysicalTopN::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state) {
	auto &gstate = (TopNGlobalState &)*state;
	// global finalize: compute the final top N
	gstate.heap = ComputeTopN(gstate.big_data, gstate.heap_size);

	PhysicalSink::Finalize(pipeline, context, move(state));
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalTopNOperatorState : public PhysicalOperatorState {
public:
	PhysicalTopNOperatorState(PhysicalOperator &op, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), position(0) {
	}

	idx_t position;
};

void PhysicalTopN::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) {
	auto &state = (PhysicalTopNOperatorState &)*state_p;
	auto &gstate = (TopNGlobalState &)*sink_state;

	if (state.position >= gstate.heap_size) {
		return;
	} else if (state.position < offset) {
		state.position = offset;
	}

	state.position = gstate.big_data.MaterializeHeapChunk(chunk, gstate.heap.get(), state.position, gstate.heap_size);
}

unique_ptr<PhysicalOperatorState> PhysicalTopN::GetOperatorState() {
	return make_unique<PhysicalTopNOperatorState>(*this, children[0].get());
}

string PhysicalTopN::ParamsToString() const {
	string result;
	result += "Top " + to_string(limit);
	if (offset > 0) {
		result += "\n";
		result += "Offset " + to_string(offset);
	}
	result += "\n[INFOSEPARATOR]";
	for (idx_t i = 0; i < orders.size(); i++) {
		result += "\n";
		result += orders[i].expression->ToString() + " ";
		result += orders[i].type == OrderType::DESCENDING ? "DESC" : "ASC";
	}
	return result;
}

} // namespace duckdb
