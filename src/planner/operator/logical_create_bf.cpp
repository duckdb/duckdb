#include <utility>

#include "duckdb/planner/operator/logical_create_bf.hpp"

namespace duckdb {

LogicalCreateBF::LogicalCreateBF(vector<shared_ptr<BloomFilterPlan>> bloom_filters)
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

// void LogicalCreateBF::Serialize(Serializer &serializer) const {
// 	LogicalOperator::Serialize(serializer);
// 	serializer.WritePropertyWithDefault<vector<shared_ptr<FilterPlan>>>(200, "BloomFilter Plans", bf_to_create_plans);
// }
//
// unique_ptr<LogicalOperator> LogicalCreateBF::Deserialize(Deserializer &deserializer) {
// 	vector<shared_ptr<FilterPlan>> bloom_filter_plans;
// 	deserializer.ReadPropertyWithDefault<vector<shared_ptr<FilterPlan>>>(200, "BloomFilter Plans", bloom_filter_plans);
// 	auto result = make_uniq<LogicalCreateBF>(std::move(bloom_filter_plans));
// 	return std::move(result);
// }

vector<ColumnBinding> LogicalCreateBF::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalCreateBF::ResolveTypes() {
	types = children[0]->types;
}
} // namespace duckdb
