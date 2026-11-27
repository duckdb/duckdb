//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/removed_subtree_effects.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Effects skipped when a subtree or its rows are removed, including materialized CTE producer effects.
class RemovedSubtreeEffects {
public:
	//! Registers the CTEs that are defined in the plan.
	explicit RemovedSubtreeEffects(LogicalOperator &plan);

	bool HasEffect(LogicalOperator &subtree);

private:
	void CollectDefinitions(LogicalOperator &op);
	bool DefinitionHasEffect(TableIndex cte_index);

	unordered_map<TableIndex, reference<LogicalOperator>> materialized_ctes;
	unordered_set<TableIndex> recursive_ctes;
	unordered_map<TableIndex, bool> definition_has_effect;
};

} // namespace duckdb
