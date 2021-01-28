#include "duckdb/execution/operator/order/physical_order.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/data_table.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/row_chunk.hpp"

namespace duckdb {

PhysicalOrder::PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders)
    : PhysicalSink(PhysicalOperatorType::ORDER_BY, move(types)), orders(move(orders)) {
    // compute the sorting columns from the input data
    for (idx_t i = 0; i < orders.size(); i++) {
        auto &expr = orders[i].expression;
        sort_types.push_back(expr->return_type);
        order_types.push_back(orders[i].type);
        null_order_types.push_back(orders[i].null_order);
        executor.AddExpression(*expr);
    }
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class OrderByGlobalState : public GlobalOperatorState {
public:
	OrderByGlobalState(BufferManager &buffer_manager) : sort_cols(buffer_manager) {
	}
	// TODO: old
	//! The lock for updating the global aggregate state
	mutex lock;
	//! The sorted data
	ChunkCollection sorted_data;
	//! The sorted vector
	unique_ptr<idx_t[]> sorted_vector;

	// TODO: new
	//! Columns to be sorted
	RowChunk sort_cols;
};

class OrderByLocalState : public LocalSinkState{
public:
    OrderByLocalState() {
	}
};

unique_ptr<GlobalOperatorState> PhysicalOrder::GetGlobalState(ClientContext &context) {
    auto state = make_unique<OrderByGlobalState>(BufferManager::GetBufferManager(context));
    for (auto type : sort_types) {
		state->sort_cols.entry_size += GetTypeIdSize(type.InternalType());
	}
    state->sort_cols.block_capacity = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, (Storage::BLOCK_ALLOC_SIZE / state->sort_cols.entry_size) + 1);
	return state;
}

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &context) {
    return make_unique<OrderByLocalState>();
}

void PhysicalOrder::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                         DataChunk &input) {
	// concatenate all the data of the child chunks
	auto &gstate = (OrderByGlobalState &)state;

    // TODO: old
	lock_guard<mutex> glock(gstate.lock);
	gstate.sorted_data.Append(input);

    // TODO: new

}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
void PhysicalOrder::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state) {
	// finalize: perform the actual sorting
	auto &sink = (OrderByGlobalState &)*state;
	ChunkCollection &big_data = sink.sorted_data;

	ChunkCollection sort_collection;
	for (idx_t i = 0; i < big_data.ChunkCount(); i++) {
		DataChunk sort_chunk;
		sort_chunk.Initialize(sort_types);

		executor.Execute(big_data.GetChunk(i), sort_chunk);
		sort_collection.Append(sort_chunk);
	}

	D_ASSERT(sort_collection.Count() == big_data.Count());

	// now perform the actual sort
	sink.sorted_vector = unique_ptr<idx_t[]>(new idx_t[sort_collection.Count()]);
	sort_collection.Sort(order_types, null_order_types, sink.sorted_vector.get());

	PhysicalSink::Finalize(pipeline, context, move(state));
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalOrderOperatorState : public PhysicalOperatorState {
public:
    PhysicalOrderOperatorState(PhysicalOperator &op, PhysicalOperator *child)
        : PhysicalOperatorState(op, child), position(0) {
    }

    idx_t position;
};

unique_ptr<PhysicalOperatorState> PhysicalOrder::GetOperatorState() {
    return make_unique<PhysicalOrderOperatorState>(*this, children[0].get());
}

void PhysicalOrder::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalOrderOperatorState *>(state_);
	auto &sink = (OrderByGlobalState &)*this->sink_state;
	ChunkCollection &big_data = sink.sorted_data;
	if (state->position >= big_data.Count()) {
		return;
	}

	big_data.MaterializeSortedChunk(chunk, sink.sorted_vector.get(), state->position);
	state->position += STANDARD_VECTOR_SIZE;
}

string PhysicalOrder::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < orders.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += orders[i].expression->ToString() + " ";
		result += orders[i].type == OrderType::DESCENDING ? "DESC" : "ASC";
	}
	return result;
}

} // namespace duckdb
