#pragma once

#include "duckdb/optimizer/constraint_propagation/constraint_facts.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class LogicalOperator;

//! Owns per-operator ScopeFacts and the output-bindings cache for one analysis pass.
class FactStore {
public:
	ScopeFacts &GetOrCreate(const LogicalOperator *op);
	const ScopeFacts &Get(const LogicalOperator *op) const;
	const ScopeFacts &Get(const LogicalOperator &op) const {
		return Get(&op);
	}

	//! Cached output bindings
	const vector<ColumnBinding> &OutputBindings(LogicalOperator &op);
	const vector<ColumnBinding> *FindBindings(const LogicalOperator *op) const;

	void Clear();

	const unordered_map<const LogicalOperator *, ScopeFacts> &All() const {
		return facts_;
	}

private:
	static const ScopeFacts kNoFacts;
	unordered_map<const LogicalOperator *, ScopeFacts> facts_;
	unordered_map<const LogicalOperator *, vector<ColumnBinding>> output_bindings_;
};

} // namespace duckdb
