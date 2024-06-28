#include "duckdb/execution/operator/helper/physical_streaming_limit.hpp"
#include "duckdb/execution/operator/helper/physical_limit.hpp"

namespace duckdb {

PhysicalStreamingLimit::PhysicalStreamingLimit(vector<LogicalType> types, BoundLimitNode limit_val_p,
                                               BoundLimitNode offset_val_p, idx_t estimated_cardinality, bool parallel)
    : PhysicalOperator(PhysicalOperatorType::STREAMING_LIMIT, std::move(types), estimated_cardinality),
      limit_val(std::move(limit_val_p)), offset_val(std::move(offset_val_p)), parallel(parallel) {
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class StreamingLimitOperatorState : public OperatorState {
public:
	explicit StreamingLimitOperatorState(const PhysicalStreamingLimit &op) {
		PhysicalLimit::SetInitialLimits(op.limit_val, op.offset_val, limit, offset);
	}

	optional_idx limit;
	optional_idx offset;
};

class StreamingLimitGlobalState : public GlobalOperatorState {
public:
	StreamingLimitGlobalState() : current_offset(0) {
	}

	std::atomic<idx_t> current_offset;
};

unique_ptr<OperatorState> PhysicalStreamingLimit::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<StreamingLimitOperatorState>(*this);
}

unique_ptr<GlobalOperatorState> PhysicalStreamingLimit::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<StreamingLimitGlobalState>();
}

OperatorResultType PhysicalStreamingLimit::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = gstate_p.Cast<StreamingLimitGlobalState>();
	auto &state = state_p.Cast<StreamingLimitOperatorState>();
	auto &limit = state.limit;
	auto &offset = state.offset;
	idx_t current_offset = gstate.current_offset.fetch_add(input.size());
	idx_t max_element;
	if (!PhysicalLimit::ComputeOffset(context, input, limit, offset, current_offset, max_element, limit_val,
	                                  offset_val)) {
		return OperatorResultType::FINISHED;
	}
	if (PhysicalLimit::HandleOffset(input, current_offset, offset.GetIndex(), limit.GetIndex())) {
		chunk.Reference(input);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

OrderPreservationType PhysicalStreamingLimit::OperatorOrder() const {
	return OrderPreservationType::FIXED_ORDER;
}

bool PhysicalStreamingLimit::ParallelOperator() const {
	return parallel;
}

} // namespace duckdb
