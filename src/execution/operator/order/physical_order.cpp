#include "duckdb/execution/operator/order/physical_order.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

class PhysicalOrderOperatorState : public PhysicalOperatorState {
public:
	PhysicalOrderOperatorState(PhysicalOperator &op, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), position(0) {
	}

	idx_t position;
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class OrderByGlobalOperatorState : public GlobalOperatorState {
public:
	//! The lock for updating the global aggregate state
	mutex lock;
	//! The sorted data
	ChunkCollection sorted_data;
	//! The sorted vector
	unique_ptr<idx_t[]> sorted_vector;
};

unique_ptr<GlobalOperatorState> PhysicalOrder::GetGlobalState(ClientContext &context) {
	return make_unique<OrderByGlobalOperatorState>();
}

void PhysicalOrder::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                         DataChunk &input) {
	// concatenate all the data of the child chunks
	auto &gstate = (OrderByGlobalOperatorState &)state;
	lock_guard<mutex> glock(gstate.lock);
	gstate.sorted_data.Append(input);
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
void PhysicalOrder::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state) {
	// finalize: perform the actual sorting
	auto &sink = (OrderByGlobalOperatorState &)*state;
	ChunkCollection &big_data = sink.sorted_data;

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
void PhysicalOrder::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalOrderOperatorState *>(state_);
	auto &sink = (OrderByGlobalOperatorState &)*this->sink_state;
	ChunkCollection &big_data = sink.sorted_data;
	if (state->position >= big_data.Count()) {
		return;
	}

	big_data.MaterializeSortedChunk(chunk, sink.sorted_vector.get(), state->position);
	state->position += STANDARD_VECTOR_SIZE;
}

unique_ptr<PhysicalOperatorState> PhysicalOrder::GetOperatorState() {
	return make_unique<PhysicalOrderOperatorState>(*this, children[0].get());
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
