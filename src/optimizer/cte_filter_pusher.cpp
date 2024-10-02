#include "duckdb/optimizer/cte_filter_pusher.hpp"

#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"

namespace duckdb {

CTEFilterPusher::MaterializedCTEInfo::MaterializedCTEInfo(LogicalOperator &materialized_cte_p)
    : materialized_cte(materialized_cte_p), all_cte_refs_are_filtered(true) {
}

CTEFilterPusher::CTEFilterPusher(Optimizer &optimizer_p) : optimizer(optimizer_p) {
}

unique_ptr<LogicalOperator> CTEFilterPusher::Optimize(unique_ptr<LogicalOperator> op) {
	FindCandidates(*op);
	auto ctes = std::move(cte_info_map);

	// Iterate once over all materialized CTEs
	for (auto it = ctes.rbegin(); it != ctes.rend(); it++) {
		if (!it->second->all_cte_refs_are_filtered) {
			continue;
		}

		// The cte_info_map must be reconstructed each time.
		// Changes to the plan otherwise break the non-unique_ptr references.
		cte_info_map = InsertionOrderPreservingMap<unique_ptr<MaterializedCTEInfo>>();
		FindCandidates(*op);

		PushFilterIntoCTE(*cte_info_map[it->first]);
	}
	return op;
}

void CTEFilterPusher::FindCandidates(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		// We encountered a new CTE, add it to the map
		cte_info_map.insert(to_string(op.Cast<LogicalMaterializedCTE>().table_index),
		                    make_uniq<MaterializedCTEInfo>(op));
	} else if (op.type == LogicalOperatorType::LOGICAL_FILTER &&
	           op.children[0]->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		// We encountered a filtered CTE ref, update the according CTE info
		auto &cte_ref = op.children[0]->Cast<LogicalCTERef>();
		auto it = cte_info_map.find(to_string(cte_ref.cte_index));
		if (it != cte_info_map.end()) {
			it->second->filters.push_back(op);
		}
		return;
	} else if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		// We encountered a CTE ref without a filter on top, so we can't do the optimization
		auto &cte_ref = op.Cast<LogicalCTERef>();
		auto it = cte_info_map.find(to_string(cte_ref.cte_index));
		if (it != cte_info_map.end()) {
			it->second->all_cte_refs_are_filtered = false;
		}
		return;
	}
	for (auto &child : op.children) {
		FindCandidates(*child);
	}
}

void CTEFilterPusher::PushFilterIntoCTE(MaterializedCTEInfo &info) {
	D_ASSERT(info.materialized_cte.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE);
	if (info.filters.empty()) {
		return;
	}

	// Create an OR expression with all the filters on all references of the CTE
	unique_ptr<Expression> outer_expr;
	for (auto &filter : info.filters) {
		D_ASSERT(filter.get().type == LogicalOperatorType::LOGICAL_FILTER);

		auto old_bindings = filter.get().children[0]->GetColumnBindings();
		auto new_bindings = info.materialized_cte.children[0]->GetColumnBindings();
		D_ASSERT(old_bindings.size() == new_bindings.size());

		ColumnBindingReplacer replacer;
		replacer.replacement_bindings.reserve(old_bindings.size());
		for (idx_t i = 0; i < old_bindings.size(); i++) {
			replacer.replacement_bindings.emplace_back(old_bindings[i], new_bindings[i]);
		}

		// We copy the filters and replace the CTE reference bindings with the bindings in the CTE definition
		unique_ptr<Expression> inner_expr;
		for (auto &filter_expr : filter.get().expressions) {
			auto filter_expr_copy = filter_expr->Copy();
			replacer.VisitExpression(&filter_expr_copy);
			if (inner_expr) {
				inner_expr = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
				                                                   std::move(inner_expr), std::move(filter_expr_copy));
			} else {
				inner_expr = std::move(filter_expr_copy);
			}
		}

		if (outer_expr) {
			outer_expr = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(outer_expr),
			                                                   std::move(inner_expr));
		} else {
			outer_expr = std::move(inner_expr);
		}
	}

	// Add the filter on top of the CTE definition and push it down
	auto new_cte = make_uniq_base<LogicalOperator, LogicalFilter>(std::move(outer_expr));
	new_cte->children.push_back(std::move(info.materialized_cte.children[0]));
	FilterPushdown pushdown(optimizer);
	new_cte = pushdown.Rewrite(std::move(new_cte));
	info.materialized_cte.children[0] = std::move(new_cte);
}

} // namespace duckdb
