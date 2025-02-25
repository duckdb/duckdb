#include "duckdb/planner/operator/logical_create_bf.hpp"

namespace duckdb {
LogicalCreateBF::LogicalCreateBF(vector<shared_ptr<BlockedBloomFilter>> bf)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_BF), bf_to_create(bf) {};

InsertionOrderPreservingMap<string> LogicalCreateBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;

	result["BF Number"] = std::to_string(bf_to_create.size());
	result["ID"] = "0x" + std::to_string(reinterpret_cast<size_t>(this));

	return result;
}

vector<ColumnBinding> LogicalCreateBF::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalCreateBF::ResolveTypes() {
	types = children[0]->types;
}
} // namespace duckdb