#include <utility>

#include "duckdb/planner/operator/logical_use_bf.hpp"

namespace duckdb {

LogicalUseBF::LogicalUseBF(vector<shared_ptr<BlockedBloomFilter>> bloom_filters)
    : LogicalOperator(LogicalOperatorType::LOGICAL_USE_BF), bf_to_use(std::move(bloom_filters)) {
}

LogicalUseBF::LogicalUseBF(shared_ptr<BlockedBloomFilter> bloom_filter)
    : LogicalOperator(LogicalOperatorType::LOGICAL_USE_BF), bf_to_use({std::move(bloom_filter)}) {
}

InsertionOrderPreservingMap<string> LogicalUseBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;

	result["BF Number"] = std::to_string(bf_to_use.size());
	string bfs;
	for (auto *bf : related_create_bf) {
		bfs += "0x" + std::to_string(reinterpret_cast<size_t>(bf)) + "\n";
	}
	result["BF Creators"] = bfs;
	return result;
}

vector<ColumnBinding> LogicalUseBF::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalUseBF::AddDownStreamOperator(LogicalCreateBF *op) {
	related_create_bf.emplace_back(op);
}

void LogicalUseBF::ResolveTypes() {
	types = children[0]->types;
}
} // namespace duckdb