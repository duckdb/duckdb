#pragma once

#include "duckdb/optimizer/constraint_propagation/constraint_facts.hpp"
#include "duckdb/optimizer/constraint_propagation/fact_store.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class LogicalOperator;
class TransferKernel;

//! Owns the FactStore (per-operator ScopeFacts) and the TransferKernel that
//! walks the plan and populates it.
class ConstraintPropagator {
public:
	ConstraintPropagator();
	~ConstraintPropagator();

	void Analyze(LogicalOperator &root);

	const FactStore &Store() const {
		return store_;
	}

	const ScopeFacts &Facts(const LogicalOperator &scope) const;

	optional_idx Position(const LogicalOperator &scope, const ColumnBinding &col) const;
	bool Positions(const LogicalOperator &scope, const vector<ColumnBinding> &cols, ColumnMask &out) const;

private:
	friend class TransferKernel;

	FactStore store_;
	unique_ptr<TransferKernel> kernel_;
};

} // namespace duckdb
