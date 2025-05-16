#include "duckdb/optimizer/cte_inlining.hpp"

#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/logical_operator_deep_copy.hpp"
#include "duckdb/planner/operator/logical_prepare.hpp"

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
		if (cte.materialize == CTEMaterialize::CTE_MATERIALIZE_ALWAYS) {
			// This CTE is always materialized, we cannot inline it
			return;
		}
		if (CountCTEReferences(*op->children[1], cte.table_index) == 1) {
			// this CTE is only referenced once, we can inline it directly without copying
			Inline(op->children[1], *op, false);
			op = std::move(op->children[1]);
		}
	}
}

bool CTEInlining::Inline(unique_ptr<duckdb::LogicalOperator> &op, LogicalOperator &materialized_cte,
                         bool requires_copy) {
	if (op->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cteref = op->Cast<LogicalCTERef>();
		auto &cte = materialized_cte.Cast<LogicalCTE>();
		if (cteref.cte_index == cte.table_index) {
			unique_ptr<LogicalOperator> &definition = cte.children[0];
			if (requires_copy) {
				// there are multiple references to the CTE, we need to copy it
				LogicalOperatorDeepCopy deep_copy(optimizer.binder, parameter_data);
				try {
					definition = deep_copy.DeepCopy(cte.children[0]);
				} catch (NotImplementedException &ex) {
					// if we have to copy the lhs of a CTE, but we cannot copy the operator, we have to
					// stop inlining and keep the materialized CTE instead
					return false;
				}
			}
			vector<unique_ptr<Expression>> proj_expressions;
			definition->ResolveOperatorTypes();
			auto types = definition->types;
			idx_t col_idx = 0;
			for (auto &col : definition->GetColumnBindings()) {
				proj_expressions.push_back(make_uniq<BoundColumnRefExpression>(types[col_idx], col));
				col_idx++;
			}
			auto proj = make_uniq<LogicalProjection>(cteref.table_index, std::move(proj_expressions));

			proj->children.push_back(std::move(definition));
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

} // namespace duckdb
