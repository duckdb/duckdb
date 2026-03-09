#include "duckdb/planner/operator/logical_explain.hpp"

namespace duckdb {

LogicalExplain::LogicalExplain(unique_ptr<LogicalOperator> plan, ExplainType explain_type, ExplainFormat explain_format)
    : LogicalOperator(LogicalOperatorType::LOGICAL_EXPLAIN), explain_type(explain_type),
      explain_format(explain_format) {
	children.push_back(std::move(plan));
}

idx_t LogicalExplain::EstimateCardinality(ClientContext &context) {
	return 3;
}

bool LogicalExplain::SupportSerialization() const {
	//! Skips the serialization check in VerifyPlan
	return false;
}

void LogicalExplain::ResolveTypes() {
	types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
}
vector<ColumnBinding> LogicalExplain::GetColumnBindings() {
	return {ColumnBinding(TableIndex(0), ProjectionIndex(0)), ColumnBinding(TableIndex(0), ProjectionIndex(1))};
}

} // namespace duckdb
