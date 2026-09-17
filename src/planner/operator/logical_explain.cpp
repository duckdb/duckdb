#include "duckdb/planner/operator/logical_explain.hpp"

namespace duckdb {

LogicalExplain::LogicalExplain(unique_ptr<LogicalOperator> plan, ExplainType explain_type,
                               const ProfilerPrintFormat &format)
    : LogicalOperator(LogicalOperatorType::LOGICAL_EXPLAIN), explain_type(explain_type), format(format) {
	children.push_back(std::move(plan));
}

vector<TableIndex> LogicalExplain::GetTableIndex() const {
	return {table_index};
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
	vector<ColumnBinding> result;
	for (auto explain_col_idx : ProjectionIndex::GetIndexes(2)) {
		result.emplace_back(table_index, explain_col_idx);
	}
	return result;
}

} // namespace duckdb
