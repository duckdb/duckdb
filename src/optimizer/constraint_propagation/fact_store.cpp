#include "duckdb/optimizer/constraint_propagation/fact_store.hpp"

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

const ScopeFacts FactStore::kNoFacts;

ScopeFacts &FactStore::GetOrCreate(const LogicalOperator *op) {
	return facts_[op];
}

const ScopeFacts &FactStore::Get(const LogicalOperator *op) const {
	auto it = facts_.find(op);
	if (it == facts_.end()) {
		return kNoFacts;
	}
	return it->second;
}

const vector<ColumnBinding> &FactStore::OutputBindings(LogicalOperator &op) {
	auto it = output_bindings_.find(&op);
	if (it == output_bindings_.end()) {
		it = output_bindings_.emplace(&op, op.GetColumnBindings()).first;
	}
	return it->second;
}

const vector<ColumnBinding> *FactStore::FindBindings(const LogicalOperator *op) const {
	auto it = output_bindings_.find(op);
	if (it == output_bindings_.end()) {
		return nullptr;
	}
	return &it->second;
}

void FactStore::Clear() {
	facts_.clear();
	output_bindings_.clear();
}

} // namespace duckdb
