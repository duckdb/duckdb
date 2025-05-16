#include <utility>

#include "duckdb/planner/operator/logical_use_bf.hpp"

namespace duckdb {

LogicalUseBF::LogicalUseBF(shared_ptr<FilterPlan> filter_plans)
    : LogicalOperator(LogicalOperatorType::LOGICAL_USE_BF), filter_plan({std::move(filter_plans)}) {
}

InsertionOrderPreservingMap<string> LogicalUseBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;

	result["BF Number"] = std::to_string(1);
	string bfs;
	auto &bf_plan = filter_plan;
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

	result["BloomFilters"] = bfs;
	return result;
}

vector<ColumnBinding> LogicalUseBF::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalUseBF::ResolveTypes() {
	types = children[0]->types;
}
} // namespace duckdb
