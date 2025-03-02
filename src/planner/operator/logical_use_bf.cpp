#include <utility>

#include "duckdb/planner/operator/logical_use_bf.hpp"

namespace duckdb {

LogicalUseBF::LogicalUseBF(vector<shared_ptr<FilterPlan>> bloom_filter_plans)
    : LogicalOperator(LogicalOperatorType::LOGICAL_USE_BF), bf_to_use_plans(std::move(bloom_filter_plans)) {
}
LogicalUseBF::LogicalUseBF(shared_ptr<FilterPlan> bloom_filter)
    : LogicalOperator(LogicalOperatorType::LOGICAL_USE_BF), bf_to_use_plans({std::move(bloom_filter)}) {
}

InsertionOrderPreservingMap<string> LogicalUseBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;

	result["BF Number"] = std::to_string(bf_to_use_plans.size());
	string bfs;
	for (auto &bf_plan : bf_to_use_plans) {
		bfs += "0x" + std::to_string(reinterpret_cast<uint64_t>(bf_plan.get())) + "\n";
		bfs += "Build: ";
		for (auto &v : bf_plan->build) {
			bfs += std::to_string(v.table_index) + "." + std::to_string(v.column_index) + " ";
		}
		bfs += " Apply: ";
		for (auto &v : bf_plan->apply) {
			bfs += std::to_string(v.table_index) + "." + std::to_string(v.column_index) + " ";
		}
		bfs += "\n";
	}
	result["BloomFilters"] = bfs;
	return result;
}

void LogicalUseBF::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	throw InternalException("Shouldn't go here: LogicalUseBF::Serialize");
}

unique_ptr<LogicalOperator> LogicalUseBF::Deserialize(Deserializer &deserializer) {
	throw InternalException("Shouldn't go here: LogicalUseBF::Deserialize");
}

vector<ColumnBinding> LogicalUseBF::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalUseBF::AddDownStreamOperator(LogicalCreateBF *op) {
	related_create_bfs.push_back(op);
}

void LogicalUseBF::ResolveTypes() {
	types = children[0]->types;
}
} // namespace duckdb
