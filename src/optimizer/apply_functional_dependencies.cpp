#include "duckdb/optimizer/apply_functional_dependencies.hpp"

#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_binder/base_select_binder.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

void ApplyFunctionalDependencies::VisitLogicalOrder(LogicalOrder &order) {
	result = distinctness.GetUniqueSets(*order.children[0]);
	VisitOrderBys(order.orders);
}

void ApplyFunctionalDependencies::VisitOrderBys(vector<BoundOrderByNode> &orders) const {
	if (orders.size() < 2) {
		return;
	}

	//	Remove orderings that are FD on the prefix
	vector<reference<Expression>> refs;
	refs.emplace_back(*orders[0].expression);
	for (idx_t i = 1; i < orders.size();) {
		auto &expr = orders[i].expression;
		if (BaseSelectBinder::IsFunctionallyDependent(expr, refs)) {
			orders.erase(orders.begin() + NumericCast<int64_t>(i));
			continue;
		}
		refs.emplace_back(*expr);
		++i;
	}

	//	Remove trailing orderings that are FD on a primary key
	if (result.empty()) {
		return;
	}

	column_binding_set_t prefix;
	for (idx_t i = 0; i + 1 < orders.size(); i++) {
		auto &expr = *orders[i].expression;
		if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		prefix.insert(expr.Cast<BoundColumnRefExpression>().Binding());
		for (auto &unique_set : result) {
			bool covered = true;
			for (auto &binding : unique_set) {
				if (prefix.find(binding) == prefix.end()) {
					covered = false;
					break;
				}
			}
			if (!covered) {
				continue;
			}
			orders.erase(orders.begin() + NumericCast<int64_t>(i + 1), orders.end());
			return;
		}
	}
}

void ApplyFunctionalDependencies::VisitPartitioning(vector<unique_ptr<Expression>> &partitions) const {
	//	Remove partitioning keys that are FD on the others
	for (idx_t i = 0; i < partitions.size();) {
		vector<reference<Expression>> refs;
		for (idx_t j = 0; j < partitions.size(); ++j) {
			if (i != j) {
				refs.emplace_back(*partitions[j]);
			}
		}
		if (BaseSelectBinder::IsFunctionallyDependent(partitions[i], refs)) {
			partitions.erase(partitions.begin() + NumericCast<int64_t>(i));
		} else {
			++i;
		}
	}

	//	Extract the partitioning references
	column_binding_set_t partition_bindings;
	for (auto &expr : partitions) {
		if (expr->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		partition_bindings.insert(expr->Cast<BoundColumnRefExpression>().Binding());
	}

	//	Find the smallest unique set that is covered by the bindings
	optional_idx smallest;
	for (idx_t i = 0; i < result.size(); ++i) {
		auto &unique_set = result.at(i);
		bool covered = true;
		for (auto &binding : unique_set) {
			if (!partition_bindings.count(binding)) {
				covered = false;
				break;
			}
		}
		if (!covered) {
			continue;
		}
		if (!smallest.IsValid() || unique_set.size() < result.at(smallest.GetIndex()).size()) {
			smallest = i;
		}
	}
	if (smallest.IsValid()) {
		//	Replace the partitioning with the unique bindings
		auto &unique_set = result.at(smallest.GetIndex());
		vector<unique_ptr<Expression>> reduced;
		for (auto &expr : partitions) {
			if (expr->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}
			const auto &ref = expr->Cast<BoundColumnRefExpression>();
			if (!unique_set.count(ref.Binding())) {
				continue;
			}
			reduced.emplace_back(ref.Copy());
		}
		std::swap(partitions, reduced);
	}
}

void ApplyFunctionalDependencies::VisitWindowExpression(BoundWindowExpression &wexpr) const {
	//	Equivalence 1: Reduce partitioning to minimal functional dependency
	VisitPartitioning(wexpr.PartitionsMutable());

	//	Equivalence 2: Truncate ordering at minimal functional dependency
	VisitOrderBys(wexpr.OrderByMutable());

	//	Equivalence 3: Remove a single ORDER BY if it is FD on the partitioning
	//	Note that this implies that the ordering expression is constant on the partition,
	//	so there is only one peer group, which is the same as having no ORDER BY clause.
	auto &partition_bys = wexpr.Partitions();
	auto &order_bys = wexpr.OrderByMutable();
	if (!partition_bys.empty() && order_bys.size() == 1) {
		vector<reference<Expression>> refs;
		for (auto &arg : partition_bys) {
			refs.emplace_back(*arg);
		}
		if (BaseSelectBinder::IsFunctionallyDependent(order_bys[0].expression, refs)) {
			order_bys.clear();
		}
	}
}

void ApplyFunctionalDependencies::VisitExpression(unique_ptr<Expression> *expression) {
	switch ((*expression)->GetExpressionClass()) {
	case ExpressionClass::BOUND_WINDOW:
		VisitWindowExpression((*expression)->Cast<BoundWindowExpression>());
		break;
	default:
		break;
	}
}

void ApplyFunctionalDependencies::VisitOperator(LogicalOperator &op) {
	VisitOperatorChildren(op);
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		VisitLogicalOrder(op.Cast<LogicalOrder>());
		break;
	case LogicalOperatorType::LOGICAL_WINDOW:
		result = distinctness.GetUniqueSets(*op.children[0]);
		VisitOperatorExpressions(op);
		break;
	default:
		break;
	}
}

} // namespace duckdb
