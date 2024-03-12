#include "duckdb/execution/operator/helper/physical_verify_vector.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

PhysicalVerifyVector::PhysicalVerifyVector(unique_ptr<PhysicalOperator> child)
    : PhysicalOperator(PhysicalOperatorType::VERIFY_VECTOR, child->types, child->estimated_cardinality) {
	children.push_back(std::move(child));
}

class VerifyVectorState : public OperatorState {
public:
	explicit VerifyVectorState() : const_idx(0) {
	}

	idx_t const_idx;
};

OperatorResultType VerifyEmitConstantVectors(DataChunk &input, DataChunk &chunk, OperatorState &state_p) {
	auto &state = state_p.Cast<VerifyVectorState>();
	D_ASSERT(state.const_idx < input.size());

	// emit constant vectors at the current index
	for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
		ConstantVector::Reference(chunk.data[c], input.data[c], state.const_idx, 1);
	}
	chunk.SetCardinality(1);
	chunk.Flatten();
	state.const_idx++;
	if (state.const_idx >= input.size()) {
		state.const_idx = 0;
		return OperatorResultType::NEED_MORE_INPUT;
	}
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

OperatorResultType VerifyEmitDictionaryVectors(DataChunk &input, DataChunk &chunk, OperatorState &state) {
	chunk.Reference(input);
	for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
		Vector::DebugTransformToDictionary(chunk.data[c], chunk.size());
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<OperatorState> PhysicalVerifyVector::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<VerifyVectorState>();
}

OperatorResultType PhysicalVerifyVector::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                 GlobalOperatorState &gstate, OperatorState &state) const {
	return VerifyEmitConstantVectors(input, chunk, state);
}

} // namespace duckdb
