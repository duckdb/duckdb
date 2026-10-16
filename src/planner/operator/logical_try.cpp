#include "duckdb/planner/operator/logical_try.hpp"

namespace duckdb {

LogicalTry::LogicalTry() : LogicalOperator(LogicalOperatorType::LOGICAL_TRY) {
}

LogicalTry::LogicalTry(unique_ptr<LogicalOperator> child) : LogicalOperator(LogicalOperatorType::LOGICAL_TRY) {
	children.push_back(std::move(child));
}

vector<ColumnBinding> LogicalTry::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

idx_t LogicalTry::EstimateCardinality(ClientContext &context) {
	// the boundary always produces exactly one row
	return 1;
}

void LogicalTry::ResolveTypes() {
	types = children[0]->types;
}

} // namespace duckdb
