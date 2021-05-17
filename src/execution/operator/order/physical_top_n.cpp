#include "duckdb/execution/operator/order/physical_top_n.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Heaps
//===--------------------------------------------------------------------===//
class TopNHeap {
public:
	static OrderByNullType FlipNullOrder(OrderByNullType order) {
		if (order == OrderByNullType::NULLS_FIRST) {
			return OrderByNullType::NULLS_LAST;
		} else if (order == OrderByNullType::NULLS_LAST) {
			return OrderByNullType::NULLS_FIRST;
		}
		return order;
	}

	TopNHeap(const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset)
	    : limit(limit), offset(offset), heap_size(0) {
		for (auto &order : orders) {
			auto &expr = order.expression;
			sort_types.push_back(expr->return_type);
			order_types.push_back(order.type);
			null_order_types.push_back(order.type == OrderType::DESCENDING ? FlipNullOrder(order.null_order)
			                                                               : order.null_order);
			executor.AddExpression(*expr);
		}
		// preallocate the heap
		heap = unique_ptr<idx_t[]>(new idx_t[limit + offset]);
	}

	void Append(DataChunk &top_chunk, DataChunk &heap_chunk) {
		top_data.Append(top_chunk);
		heap_data.Append(heap_chunk);
		D_ASSERT(heap_data.Count() == top_data.Count());
	}

	void Sink(DataChunk &input);
	void Combine(TopNHeap &other);
	void Reduce();

	idx_t MaterializeTopChunk(DataChunk &top_chunk, idx_t position) {
		return top_data.MaterializeHeapChunk(top_chunk, heap.get(), position, heap_size);
	}

	idx_t limit;
	idx_t offset;
	idx_t heap_size;
	ExpressionExecutor executor;
	vector<LogicalType> sort_types;
	vector<OrderType> order_types;
	vector<OrderByNullType> null_order_types;
	ChunkCollection top_data;
	ChunkCollection heap_data;
	unique_ptr<idx_t[]> heap;
};

void TopNHeap::Sink(DataChunk &input) {
	// compute the ordering values for the new chunk
	DataChunk heap_chunk;
	heap_chunk.Initialize(sort_types);

	executor.Execute(input, heap_chunk);

	// append the new chunk to what we have already
	Append(input, heap_chunk);
}

void TopNHeap::Combine(TopNHeap &other) {
	for (idx_t i = 0; i < other.top_data.ChunkCount(); ++i) {
		auto &top_chunk = other.top_data.GetChunk(i);
		auto &heap_chunk = other.heap_data.GetChunk(i);
		Append(top_chunk, heap_chunk);
	}
}

void TopNHeap::Reduce() {
	heap_size = (heap_data.Count() > offset) ? MinValue(limit + offset, heap_data.Count()) : 0;
	if (heap_size == 0) {
		return;
	}

	// create the heap
	heap_data.Heap(order_types, null_order_types, heap.get(), heap_size);

	// extract the top rows into new collections
	ChunkCollection new_top;
	ChunkCollection new_heap;
	DataChunk top_chunk;
	top_chunk.Initialize(top_data.Types());
	DataChunk heap_chunk;
	heap_chunk.Initialize(heap_data.Types());
	for (idx_t position = 0; position < heap_size;) {
		(void)top_data.MaterializeHeapChunk(top_chunk, heap.get(), position, heap_size);
		position = heap_data.MaterializeHeapChunk(heap_chunk, heap.get(), position, heap_size);
		new_top.Append(top_chunk);
		new_heap.Append(heap_chunk);
	}

	// replace the old data
	std::swap(top_data, new_top);
	std::swap(heap_data, new_heap);
}

class TopNGlobalState : public GlobalOperatorState {
public:
	TopNGlobalState(const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset) : heap(orders, limit, offset) {
	}
	mutex lock;
	TopNHeap heap;
};

class TopNLocalState : public LocalSinkState {
public:
	TopNLocalState(const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset) : heap(orders, limit, offset) {
	}
	TopNHeap heap;
};

unique_ptr<LocalSinkState> PhysicalTopN::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<TopNLocalState>(orders, limit, offset);
}

unique_ptr<GlobalOperatorState> PhysicalTopN::GetGlobalState(ClientContext &context) {
	return make_unique<TopNGlobalState>(orders, limit, offset);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
void PhysicalTopN::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                        DataChunk &input) const {
	// append to the local sink state
	auto &sink = (TopNLocalState &)lstate;
	sink.heap.Sink(input);
	sink.heap.Reduce();
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
void PhysicalTopN::Combine(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_p) {
	auto &gstate = (TopNGlobalState &)state;
	auto &lstate = (TopNLocalState &)lstate_p;

	// scan the local top N and append it to the global heap
	lock_guard<mutex> glock(gstate.lock);
	gstate.heap.Combine(lstate.heap);
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
bool PhysicalTopN::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state) {
	auto &gstate = (TopNGlobalState &)*state;
	// global finalize: compute the final top N
	gstate.heap.Reduce();

	PhysicalSink::Finalize(pipeline, context, move(state));
	return true;
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

void PhysicalTopN::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) const {
	auto &state = (PhysicalTopNOperatorState &)*state_p;
	auto &gstate = (TopNGlobalState &)*sink_state;

	if (state.position >= gstate.heap.heap_size) {
		return;
	} else if (state.position < offset) {
		state.position = offset;
	}

	state.position = gstate.heap.MaterializeTopChunk(chunk, state.position);
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
