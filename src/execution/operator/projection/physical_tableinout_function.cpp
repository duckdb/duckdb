#include "duckdb/execution/operator/projection/physical_tableinout_function.hpp"

namespace duckdb {

class PhysicalTableInOutFunctionState : public PhysicalOperatorState {
public:
	PhysicalTableInOutFunctionState(PhysicalOperator &op, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), initialized(false) {
		D_ASSERT(child);
	}

	unique_ptr<PhysicalOperatorState> child_state;
	DataChunk child_chunk;
	unique_ptr<FunctionOperatorData> operator_data;
	bool initialized = false;
};

// this implements a sorted window functions variant
PhysicalTableInOutFunction::PhysicalTableInOutFunction(vector<LogicalType> types, TableFunction function_p,
                                                       unique_ptr<FunctionData> bind_data_p,
                                                       vector<column_t> column_ids_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::INOUT_FUNCTION, move(types), estimated_cardinality),
      function(move(function_p)), bind_data(move(bind_data_p)), column_ids(move(column_ids_p)) {
}

void PhysicalTableInOutFunction::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                                  PhysicalOperatorState *state_p) const {
	auto &state = (PhysicalTableInOutFunctionState &)*state_p;

	if (!state.initialized) {
		if (function.init) {
			state.operator_data = function.init(context.client, bind_data.get(), column_ids, nullptr);
		}
		state.initialized = true;
	}

	D_ASSERT(children.size() == 1);
	state.child_chunk.Reset();
	children[0]->GetChunkInternal(context, state.child_chunk, state.child_state.get());
	function.function(context.client, bind_data.get(), state.operator_data.get(), &state.child_chunk, chunk);
}

unique_ptr<PhysicalOperatorState> PhysicalTableInOutFunction::GetOperatorState() {
	auto state = make_unique<PhysicalTableInOutFunctionState>(*this, children[0].get());
	state->child_chunk.Initialize(children[0]->GetTypes());
	state->child_state = children[0]->GetOperatorState();

	return move(state);
}

} // namespace duckdb
