//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_elimination.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {
class JoinElimination;
class PipelineInfo;

struct DistinctGroupRef {
	column_binding_set_t distinct_group;
	unordered_set<ProjectionIndex> ref_column_ids;
};

class PipelineInfo {
public:
	//! The tables the operators above (and the conditions of the joins above) reference
	unordered_set<TableIndex> ref_table_ids;
	//! The operators above return the same result when rows of this pipeline are repeated
	bool ignores_duplicates = false;
	//! Column sets that are distinct: the ones of the operators above, and the ones found on the way down
	unordered_map<TableIndex, column_binding_set_t> distinct_groups;

	//! Found in this pipeline: a pushed-down filter condition (e.g. in a table scan operator)
	bool has_filter = false;

public:
	//! The state a pipeline below starts with: what is inherited from the operators above, nothing that was found
	PipelineInfo CreateChild() {
		auto result = PipelineInfo();
		result.ref_table_ids = ref_table_ids;
		result.distinct_groups = distinct_groups;
		return result;
	}
};

class JoinElimination : public LogicalOperatorVisitor {
public:
	explicit JoinElimination() {
	}

	// with specific condition we can eliminate a (left/right, semi, inner) join.
	// exemplify left/right join eliminaion condition:
	// 1. output can only have outer table columns
	// 2. join result cannot filter by inner table columns(ex. in where clause/ having clause ...)
	// 3. must ensure each outer row can match at most one inner table row, such as:
	//  1) inner table join condition is unique(ex. 1. join conditions have inner table's primary key 2. inner table
	//  join condition columns contains a whole distinct group) 2) join result columns contains a whole distinct group
	// condition 3 is not needed when the operators above the join ignore duplicates (a GROUP BY with only min, max
	// and DISTINCT aggregates, the right side of a SEMI or ANTI join): the join then only repeats outer rows.
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
	bool ContainDistinctGroup(const unordered_map<TableIndex, column_binding_set_t> &distinct_groups,
	                          vector<ColumnBinding> &column_bindings);

	PipelineInfo pipe_info;
	//! The number of joins removed in this pipeline and the pipelines below it
	idx_t removed_joins = 0;
};
} // namespace duckdb
