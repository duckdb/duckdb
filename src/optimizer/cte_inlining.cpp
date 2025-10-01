#include "duckdb/optimizer/cte_inlining.hpp"

#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/logical_operator_deep_copy.hpp"
#include "duckdb/planner/operator/logical_prepare.hpp"

#include "duckdb/function/scalar/generic_functions.hpp"

namespace duckdb {

CTEInlining::CTEInlining(Optimizer &optimizer_p) : optimizer(optimizer_p) {
}

unique_ptr<LogicalOperator> CTEInlining::Optimize(unique_ptr<LogicalOperator> op) {
	TryInlining(op);
	return op;
}

static idx_t CountCTEReferences(const LogicalOperator &op, idx_t cte_index) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cte = op.Cast<LogicalCTERef>();
		if (cte.cte_index == cte_index) {
			return 1;
		}
	}
	idx_t number_of_references = 0;
	for (auto &child : op.children) {
		number_of_references += CountCTEReferences(*child, cte_index);
	}

	return number_of_references;
}

static bool ContainsLimit(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_LIMIT || op.type == LogicalOperatorType::LOGICAL_TOP_N) {
		return true;
	}
	if (op.children.size() != 1) {
		return false;
	}
	for (auto &child : op.children) {
		if (ContainsLimit(*child)) {
			return true;
		}
	}
	return false;
}

bool CTEInlining::EndsInAggregateOrDistinct(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_WINDOW:
		return true;
	default:
		break;
	}
	if (op.children.size() != 1) {
		return false;
	}
	for (auto &child : op.children) {
		if (EndsInAggregateOrDistinct(*child)) {
			return true;
		}
	}
	return false;
}

void CTEInlining::TryInlining(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_PREPARE) {
		// we are in a prepare statement, if we have to copy an operator during inlining,
		// we have to be careful to use the correct parameter data
		auto &prepare = op->Cast<LogicalPrepare>();
		parameter_data = prepare.prepared->value_map;
	}

	// traverse children first, so we can inline the deepest CTEs first
	for (auto &child : op->children) {
		TryInlining(child);
	}

	if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		auto &cte = op->Cast<LogicalMaterializedCTE>();
		auto ref_count = CountCTEReferences(*op, cte.table_index);
		if (ref_count == 0) {
			// this CTE is not referenced, we can remove it
			op = std::move(op->children[1]);
			return;
		}
		if (cte.materialize == CTEMaterialize::CTE_MATERIALIZE_ALWAYS) {
			// This CTE is always materialized, we cannot inline it
			return;
		}
		if (ref_count == 1) {
			// this CTE is only referenced once, we can inline it directly without copying
			bool success = Inline(op->children[1], *op, false);
			if (success) {
				op = std::move(op->children[1]);
			}
			return;
		}
		if (ref_count > 1) {
			if (cte.materialize == CTEMaterialize::CTE_MATERIALIZE_NEVER) {
				// this CTE is referenced multiple times, but we are not allowed to materialize it
				// we have to inline it if possible
				bool success = Inline(op->children[1], *op, true);
				if (success) {
					op = std::move(op->children[1]);
				}
				return;
			}
			// check if we can inline this CTE
			PreventInlining prevent_inlining;
			prevent_inlining.VisitOperator(*op->children[0]);

			if (prevent_inlining.prevent_inlining) {
				// we cannot inline this CTE, we have to keep it materialized
				return;
			}

			// Prevent inlining if the CTE ends in an aggregate or distinct operator
			// This mimics the behavior of the CTE materialization in the binder
			if (EndsInAggregateOrDistinct(*op->children[0])) {
				return;
			}

			// CTEs require full materialization before the CTE scans begin,
			// LIMIT and TOP_N operators cannot abort the materialization,
			// even if only a part of the CTE result is needed.
			// Therefore, we check if the CTE Scans are below the LIMIT or TOP_N operator
			// and if so, we try to inline the CTE definition.
			if (ContainsLimit(*op->children[1]) || op->children[0]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
				// this CTE is referenced multiple times and has a limit, we want to inline it
				bool success = Inline(op->children[1], *op, true);
				if (success) {
					op = std::move(op->children[1]);
				}
				return;
			}
		}
	}
}

bool CTEInlining::Inline(unique_ptr<LogicalOperator> &op, LogicalOperator &materialized_cte, bool requires_copy) {
	if (op->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cteref = op->Cast<LogicalCTERef>();
		auto &cte = materialized_cte.Cast<LogicalCTE>();
		if (cteref.cte_index == cte.table_index) {
			unique_ptr<LogicalOperator> &definition = cte.children[0];
			unique_ptr<LogicalOperator> copy;
			if (requires_copy) {
				// there are multiple references to the CTE, we need to copy it
				LogicalOperatorDeepCopy deep_copy(optimizer.binder, parameter_data);
				try {
					copy = deep_copy.DeepCopy(definition);
				} catch (NotImplementedException &ex) {
					// if we have to copy the lhs of a CTE, but we cannot copy the operator, we have to
					// stop inlining and keep the materialized CTE instead
					return false;
				}
			}
			vector<unique_ptr<Expression>> proj_expressions;
			definition->ResolveOperatorTypes();
			vector<LogicalType> types = definition->types;
			vector<ColumnBinding> bindings =
			    requires_copy ? copy->GetColumnBindings() : definition->GetColumnBindings();

			idx_t col_idx = 0;
			for (auto &col : bindings) {
				proj_expressions.push_back(make_uniq<BoundColumnRefExpression>(types[col_idx], col));
				col_idx++;
			}
			auto proj = make_uniq<LogicalProjection>(cteref.table_index, std::move(proj_expressions));

			if (requires_copy) {
				proj->children.push_back(std::move(copy));
			} else {
				proj->children.push_back(std::move(definition));
			}
			op = std::move(proj);
			return true;
		}
		return true;
	} else {
		bool success = true;
		for (auto &child : op->children) {
			success &= Inline(child, materialized_cte, requires_copy);
		}
		return success;
	}
}

void PreventInlining::VisitOperator(LogicalOperator &op) {
	VisitOperatorExpressions(op);
	// We can stop checking early if we already know that inlining is not possible
	if (!prevent_inlining) {
		VisitOperatorChildren(op);
	}
}

void PreventInlining::VisitExpression(unique_ptr<Expression> *expression) {
	auto &expr = *expression;

	if (expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &bound_function = expr->Cast<BoundFunctionExpression>();
		// if we encounter the ErrorFun function, we still want to inline
		if (bound_function.function == ErrorFun::GetFunction()) {
			return;
		}

		if (expr->IsVolatile()) {
			prevent_inlining = true;
			return;
		}
	}
	VisitExpressionChildren(**expression);
}

} // namespace duckdb
