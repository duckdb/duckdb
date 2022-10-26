#include "duckdb/execution/operator/helper/physical_streaming_limit.hpp"
#include "duckdb/execution/operator/helper/physical_limit.hpp"

namespace duckdb {

PhysicalStreamingLimit::PhysicalStreamingLimit(vector<LogicalType> types, idx_t limit, idx_t offset,
                                               unique_ptr<Expression> limit_expression,
                                               unique_ptr<Expression> offset_expression, idx_t estimated_cardinality,
                                               bool parallel)
    : PhysicalOperator(PhysicalOperatorType::STREAMING_LIMIT, move(types), estimated_cardinality), limit_value(limit),
      offset_value(offset), limit_expression(move(limit_expression)), offset_expression(move(offset_expression)),
      parallel(parallel) {
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class StreamingLimitOperatorState : public OperatorState {
public:
	explicit StreamingLimitOperatorState(const PhysicalStreamingLimit &op) {
		this->limit = op.limit_expression ? DConstants::INVALID_INDEX : op.limit_value;
		this->offset = op.offset_expression ? DConstants::INVALID_INDEX : op.offset_value;
	}

	idx_t limit;
	idx_t offset;
};

class StreamingLimitGlobalState : public GlobalOperatorState {
public:
	StreamingLimitGlobalState() : current_offset(0) {
	}

	std::atomic<idx_t> current_offset;
};

unique_ptr<OperatorState> PhysicalStreamingLimit::GetOperatorState(ExecutionContext &context) const {
	return make_unique<StreamingLimitOperatorState>(*this);
}

unique_ptr<GlobalOperatorState> PhysicalStreamingLimit::GetGlobalOperatorState(ClientContext &context) const {
	return make_unique<StreamingLimitGlobalState>();
}

OperatorResultType PhysicalStreamingLimit::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = (StreamingLimitGlobalState &)gstate_p;
	auto &state = (StreamingLimitOperatorState &)state_p;
	auto &limit = state.limit;
	auto &offset = state.offset;
	idx_t current_offset = gstate.current_offset.fetch_add(input.size());
	idx_t max_element;
	if (!PhysicalLimit::ComputeOffset(context, input, limit, offset, current_offset, max_element,
	                                  limit_expression.get(), offset_expression.get())) {
		return OperatorResultType::FINISHED;
	}
	if (PhysicalLimit::HandleOffset(input, current_offset, offset, limit)) {
		chunk.Reference(input);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

bool PhysicalStreamingLimit::IsOrderDependent() const {
	return !parallel;
}

bool PhysicalStreamingLimit::ParallelOperator() const {
	return parallel;
}

} // namespace duckdb
