#include "duckdb/planner/operator/logical_create_bf.hpp"

#include <utility>

namespace duckdb {

LogicalCreateBF::LogicalCreateBF(vector<shared_ptr<BloomFilterPlan>> bloom_filters)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_BF), bf_to_create_plans(std::move(bloom_filters)) {
}

InsertionOrderPreservingMap<string> LogicalCreateBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;

	result["BF Number"] = std::to_string(bf_to_create_plans.size());
	string bfs;
	for (auto &bf_plan : bf_to_create_plans) {
		bfs += "0x" + std::to_string(reinterpret_cast<uint64_t>(bf_plan.get())) + "\n";
		bfs += "Build: ";
		for (auto &expr : bf_plan->build) {
			auto &v = expr->Cast<BoundColumnRefExpression>().binding;
			bfs += std::to_string(v.table_index) + "." + std::to_string(v.column_index) + " ";
		}
		bfs += " Apply: ";
		for (auto &expr : bf_plan->apply) {
			auto &v = expr->Cast<BoundColumnRefExpression>().binding;
			bfs += std::to_string(v.table_index) + "." + std::to_string(v.column_index) + " ";
		}
		bfs += "\n";
	}
	result["BloomFilters"] = bfs;
	return result;
}

vector<ColumnBinding> LogicalCreateBF::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalCreateBF::ResolveTypes() {
	types = children[0]->types;
}
} // namespace duckdb
