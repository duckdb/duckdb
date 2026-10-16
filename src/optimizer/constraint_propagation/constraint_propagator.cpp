#include "duckdb/optimizer/constraint_propagation/constraint_propagator.hpp"

#include "duckdb/optimizer/constraint_propagation/transfer_kernel.hpp"

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

ConstraintPropagator::ConstraintPropagator() {
	kernel_ = make_uniq<TransferKernel>(*this);
}

ConstraintPropagator::~ConstraintPropagator() = default;

void ConstraintPropagator::Analyze(LogicalOperator &root) {
	store_.Clear();
	kernel_->Walk(root);
}

const ScopeFacts &ConstraintPropagator::Facts(const LogicalOperator &scope) const {
	return store_.Get(&scope);
}

optional_idx ConstraintPropagator::Position(const LogicalOperator &scope, const ColumnBinding &col) const {
	const auto *bindings = store_.FindBindings(&scope);
	if (!bindings) {
		return optional_idx();
	}
	return PositionIn(*bindings, col);
}

bool ConstraintPropagator::Positions(const LogicalOperator &scope, const vector<ColumnBinding> &cols,
                                     ColumnMask &out) const {
	const auto *bindings = store_.FindBindings(&scope);
	if (!bindings) {
		out = ColumnMask::Empty();
		return false;
	}
	out = ColumnMask(bindings->size());
	for (auto &c : cols) {
		auto pos = PositionIn(*bindings, c);
		if (!pos.IsValid()) {
			out = ColumnMask::Empty();
			return false;
		}
		out.Set(pos.GetIndex());
	}
	return true;
}

} // namespace duckdb
