#include <utility>

#include "duckdb/planner/operator/logical_create_bf.hpp"

namespace duckdb {

LogicalCreateBF::LogicalCreateBF(vector<shared_ptr<FilterPlan>> bloom_filters)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_BF), bf_to_create_plans(std::move(bloom_filters)) {};

InsertionOrderPreservingMap<string> LogicalCreateBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;

	result["BF Number"] = std::to_string(bf_to_create_plans.size());
	string bfs;
	for (auto &bf_plan : bf_to_create_plans) {
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

void LogicalCreateBF::Serialize(Serializer &serializer) const {
	throw InternalException("Shouldn't go here: LogicalCreateBF::Serialize");
}

unique_ptr<LogicalOperator> LogicalCreateBF::Deserialize(Deserializer &deserializer) {
	throw InternalException("Shouldn't go here: LogicalCreateBF::Deserialize");
}

vector<ColumnBinding> LogicalCreateBF::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalCreateBF::ResolveTypes() {
	types = children[0]->types;
}
} // namespace duckdb
