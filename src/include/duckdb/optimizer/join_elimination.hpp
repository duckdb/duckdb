//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_elimination.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {
class JoinElimination;

class PipelineInfo {
public:
	//! The tables the operators above (and the conditions of the joins above) reference
	unordered_set<TableIndex> ref_table_ids;
	//! The operators above return the same result when rows of this pipeline are repeated: an aggregate for which
	//! LogicalAggregate::IgnoresDuplicates holds (DISTINCT, GROUP BY without aggregates, min / max, DISTINCT
	//! aggregates) or the right side of a SEMI / ANTI join, with nothing in between that sees how often a row occurs.
	//! It only passes through projections, filters, UNNEST and joins without a volatile expression; every other
	//! operator (LIMIT, window, set operation, ...) ends it
	bool ignores_duplicates = false;

	//! Found in this pipeline: a pushed-down filter condition (e.g. in a table scan operator)
	bool has_filter = false;

public:
	//! The state a pipeline below starts with: what is inherited from the operators above, nothing that was found.
	//! Whether the pipeline below ignores duplicates is decided by the operator that starts it
	PipelineInfo CreateChild() {
		auto result = PipelineInfo();
		result.ref_table_ids = ref_table_ids;
		return result;
	}
};

//! What a removed subtree would no longer do, beyond returning rows: evaluate a volatile expression (nextval, random),
//! modify data, or run a table function that declares itself VOLATILE. A reference to a materialized CTE counts with
//! everything in the definition of that CTE: once the reference is gone, the columns only it read are pruned from the
//! definition, expressions included.
class RemovedSubtreeEffects {
public:
	//! Registers the CTEs that are defined in the plan
	explicit RemovedSubtreeEffects(LogicalOperator &plan);

	//! Whether removing the subtree would skip an effect
	bool HasEffect(LogicalOperator &subtree);

private:
	void CollectDefinitions(LogicalOperator &op);
	//! Whether the definition of a materialized CTE has an effect; computed once per CTE
	bool DefinitionHasEffect(TableIndex cte_index);

	//! The materialized CTEs of the plan (the CTE operators; the definition is their first child)
	unordered_map<TableIndex, reference<LogicalOperator>> materialized_ctes;
	//! The recursive CTEs of the plan: a reference to one reads the working table of a recursion that is part of the
	//! plan around the reference
	unordered_set<TableIndex> recursive_ctes;
	unordered_map<TableIndex, bool> definition_has_effect;
};

class JoinElimination : public LogicalOperatorVisitor {
public:
	explicit JoinElimination() {
	}

	// A LEFT / RIGHT join is replaced by its outer side when
	// 1. nothing above the join reads a column of its inner side (the output, filters, the conditions of the joins
	//    above only read the outer side),
	// 2. neither the join condition nor the inner side has a volatile expression (nextval, random): they would not
	//    be evaluated any more. The inner side must not have any other effect either (RemovedSubtreeEffects), and
	// 3. the join cannot change how often an outer row occurs in a way that is visible. There are two separate proofs:
	//    a) the operators above ignore duplicates (PipelineInfo::ignores_duplicates): the join only repeats outer rows
	//    b) every outer row matches at most one inner row: the inner side is distinct on the columns its equality
	//       conditions compare (Distinctness: a DISTINCT / GROUP BY, a PRIMARY KEY). This holds whatever is above the
	//       join. A SINGLE join is only removed by this proof: it raises an error on a second match.
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	//! Whether Optimize removed a join
	bool RemovedJoin() const;
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;

	unique_ptr<JoinElimination> CreateChildren() {
		auto result = make_uniq<JoinElimination>();
		result->pipe_info = pipe_info.CreateChild();
		result->effects = effects;
		return result;
	}

private:
	//! Analyses the operators of this pipeline from `op` down. A join is replaced in place, through `op`, by its outer
	//! side when it can be removed; `replaceable`: whether `op` itself may be replaced (false for the root of the
	//! plan and for the children of operators that may read their columns by position)
	void OptimizeOperator(unique_ptr<LogicalOperator> &op, bool replaceable);
	//! Analyses child `child_idx` of `op` with `child_optimizer` and keeps the projection map `op` has for that child
	//! valid when a join was removed below
	void OptimizeChild(LogicalOperator &op, idx_t child_idx, JoinElimination &child_optimizer, bool replaceable);
	//! Analyses the join `op` of this pipeline and both of its sides, and removes it when it can
	void OptimizeJoin(unique_ptr<LogicalOperator> &op);
	void OptimizeJoinChild(LogicalOperator &join, idx_t child_idx, JoinElimination &child);
	//! Replaces the join `op` by its outer side when it can; returns whether it did
	bool TryEliminateJoin(unique_ptr<LogicalOperator> &op, JoinElimination &left_child, JoinElimination &right_child);

	PipelineInfo pipe_info;
	//! Shared by the optimizers of all pipelines of one plan
	shared_ptr<RemovedSubtreeEffects> effects;
	//! The number of joins removed in this pipeline and the pipelines below it
	idx_t removed_joins = 0;
};
} // namespace duckdb
