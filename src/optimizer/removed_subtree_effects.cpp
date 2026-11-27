#include "duckdb/optimizer/removed_subtree_effects.hpp"

#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

RemovedSubtreeEffects::RemovedSubtreeEffects(LogicalOperator &plan) {
	CollectDefinitions(plan);
}

void RemovedSubtreeEffects::CollectDefinitions(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		materialized_ctes.emplace(op.Cast<LogicalCTE>().table_index, op);
	} else if (op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		recursive_ctes.insert(op.Cast<LogicalCTE>().table_index);
	}
	for (auto &child : op.children) {
		CollectDefinitions(*child);
	}
}

bool RemovedSubtreeEffects::DefinitionHasEffect(TableIndex cte_index) {
	auto known = definition_has_effect.find(cte_index);
	if (known != definition_has_effect.end()) {
		return known->second;
	}
	auto cte = materialized_ctes.find(cte_index);
	if (cte == materialized_ctes.end()) {
		// A reference to something not defined in this plan may have effects.
		return true;
	}
	// Materialized CTE definitions cannot reference themselves.
	const bool has_effect = HasEffect(*cte->second.get().children[0]);
	definition_has_effect.emplace(cte_index, has_effect);
	return has_effect;
}

bool RemovedSubtreeEffects::HasEffect(LogicalOperator &subtree) {
	bool is_volatile = false;
	LogicalOperatorVisitor::EnumerateExpressions(
	    subtree, [&](const unique_ptr<Expression> *expr) { is_volatile |= (*expr)->IsVolatile(); });
	if (is_volatile) {
		return true;
	}
	switch (subtree.type) {
	case LogicalOperatorType::LOGICAL_INSERT:
	case LogicalOperatorType::LOGICAL_DELETE:
	case LogicalOperatorType::LOGICAL_UPDATE:
	case LogicalOperatorType::LOGICAL_MERGE_INTO:
		return true;
	case LogicalOperatorType::LOGICAL_GET:
		if (subtree.Cast<LogicalGet>().function.stability == FunctionStability::VOLATILE) {
			return true;
		}
		break;
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		auto cte_index = subtree.Cast<LogicalCTERef>().cte_index;
		if (recursive_ctes.find(cte_index) == recursive_ctes.end() && DefinitionHasEffect(cte_index)) {
			return true;
		}
		break;
	}
	default:
		break;
	}
	for (auto &child : subtree.children) {
		if (HasEffect(*child)) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb
