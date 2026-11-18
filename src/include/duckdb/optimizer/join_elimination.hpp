//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_elimination.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
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

class JoinElimination : public LogicalOperatorVisitor {
public:
	explicit JoinElimination() {
	}

	// A LEFT / RIGHT join is replaced by its outer side when
	// 1. nothing above the join reads a column of its inner side (the output, filters, the conditions of the joins
	//    above only read the outer side),
	// 2. neither the join condition nor the inner side has a volatile expression (nextval, random): they would not
	//    be evaluated any more, and
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
	//! The number of joins removed in this pipeline and the pipelines below it
	idx_t removed_joins = 0;
};
} // namespace duckdb
