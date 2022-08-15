#include "duckdb/planner/plan_serialization.hpp"
#include "duckdb/planner/expression/bound_parameter_data.hpp"

namespace duckdb {

PlanDeserializationState::PlanDeserializationState(ClientContext &context) : context(context) {
}
PlanDeserializationState::~PlanDeserializationState() {
}

LogicalDeserializationState::LogicalDeserializationState(PlanDeserializationState &gstate, LogicalOperatorType type,
                                                         vector<unique_ptr<LogicalOperator>> &children)
    : gstate(gstate), type(type), children(children) {
}

ExpressionDeserializationState::ExpressionDeserializationState(PlanDeserializationState &gstate, ExpressionType type)
    : gstate(gstate), type(type) {
}

} // namespace duckdb
