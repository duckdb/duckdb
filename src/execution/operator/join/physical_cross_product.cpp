#include "duckdb/execution/operator/join/physical_cross_product.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"

namespace duckdb {

PhysicalCrossProduct::PhysicalCrossProduct(vector<LogicalType> types, unique_ptr<PhysicalOperator> left,
                                           unique_ptr<PhysicalOperator> right, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CROSS_PRODUCT, move(types), estimated_cardinality) {
	children.push_back(move(left));
	children.push_back(move(right));
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CrossProductGlobalState : public GlobalSinkState {
public:
	explicit CrossProductGlobalState(ClientContext &context) : rhs_materialized(BufferAllocator::Get(context)) {
	}

	ChunkCollection rhs_materialized;
	mutex rhs_lock;
};

unique_ptr<GlobalSinkState> PhysicalCrossProduct::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<CrossProductGlobalState>(context);
}

SinkResultType PhysicalCrossProduct::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p,
                                          DataChunk &input) const {
	auto &sink = (CrossProductGlobalState &)state;
	lock_guard<mutex> client_guard(sink.rhs_lock);
	sink.rhs_materialized.Append(input);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class CrossProductOperatorState : public OperatorState {
public:
	CrossProductOperatorState() : right_position(0) {
	}

	idx_t right_position;
};

unique_ptr<OperatorState> PhysicalCrossProduct::GetOperatorState(ExecutionContext &context) const {
	return make_unique<CrossProductOperatorState>();
}

OperatorResultType PhysicalCrossProduct::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                 GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = (CrossProductOperatorState &)state_p;
	auto &sink = (CrossProductGlobalState &)*sink_state;
	auto &right_collection = sink.rhs_materialized;

	if (sink.rhs_materialized.Count() == 0) {
		// no RHS: empty result
		return OperatorResultType::FINISHED;
	}
	if (state.right_position >= right_collection.Count()) {
		// ran out of entries on the RHS
		// reset the RHS and move to the next chunk on the LHS
		state.right_position = 0;
		return OperatorResultType::NEED_MORE_INPUT;
	}

	auto &left_chunk = input;
	// now match the current vector of the left relation with the current row
	// from the right relation
	chunk.SetCardinality(left_chunk.size());
	// create a reference to the vectors of the left column
	for (idx_t i = 0; i < left_chunk.ColumnCount(); i++) {
		chunk.data[i].Reference(left_chunk.data[i]);
	}
	// duplicate the values on the right side
	auto &right_chunk = right_collection.GetChunkForRow(state.right_position);
	auto row_in_chunk = state.right_position % STANDARD_VECTOR_SIZE;
	for (idx_t i = 0; i < right_collection.ColumnCount(); i++) {
		ConstantVector::Reference(chunk.data[left_chunk.ColumnCount() + i], right_chunk.data[i], row_in_chunk,
		                          right_chunk.size());
	}

	// for the next iteration, move to the next position on the right side
	state.right_position++;
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalCrossProduct::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	PhysicalJoin::BuildJoinPipelines(executor, current, state, *this);
}

vector<const PhysicalOperator *> PhysicalCrossProduct::GetSources() const {
	return children[0]->GetSources();
}

} // namespace duckdb
