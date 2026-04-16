#include "duckdb/planner/subquery/rewrite_cte_scan.hpp"

#include "duckdb/planner/operator/list.hpp"

#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

RewriteCTEScan::RewriteCTEScan(TableIndex table_index, const CorrelatedColumns &correlated_columns)
    : table_index(table_index), correlated_columns(correlated_columns) {
}

bool RewriteCTEScan::CollectAccessingOperators(LogicalOperator &op) {
	bool accesses_target_cte = false;
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cteref = op.Cast<LogicalCTERef>();
		accesses_target_cte = cteref.cte_index == table_index;
	}
	for (auto &child : op.children) {
		accesses_target_cte |= CollectAccessingOperators(*child);
	}
	if (accesses_target_cte) {
		accessing_operators.insert(op);
	}
	return accesses_target_cte;
}

void RewriteCTEScan::Rewrite(LogicalOperator &op, TableIndex table_index, const CorrelatedColumns &correlated_columns) {
	RewriteCTEScan rewriter(table_index, correlated_columns);
	rewriter.CollectAccessingOperators(op);
	rewriter.VisitOperator(op);
}

void RewriteCTEScan::VisitOperator(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cteref = op.Cast<LogicalCTERef>();

		if (cteref.cte_index == table_index && cteref.correlated_columns == 0) {
			for (auto &c : this->correlated_columns) {
				cteref.chunk_types.push_back(c.type);
				cteref.bound_columns.push_back(c.name);
			}
			cteref.correlated_columns += correlated_columns.size();
		}
	} else if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		// There is another dependent join below the rewritten CTE. Add the
		// CTE's correlated columns to this operator if one of its children
		// accesses the rewritten CTE.
		auto &join = op.Cast<LogicalDependentJoin>();
		bool has_cte_ref = false;
		for (auto &child : join.children) {
			if (accessing_operators.find(*child) != accessing_operators.end()) {
				has_cte_ref = true;
				break;
			}
		}
		if (!has_cte_ref) {
			VisitOperatorChildren(op);
			return;
		}

		for (auto &c : correlated_columns) {
			bool contains_binding = false;
			for (auto &col : join.correlated_columns) {
				if (col.binding == c.binding) {
					contains_binding = true;
					break;
				}
			}
			// We only add new columns
			if (!contains_binding) {
				join.correlated_columns.AddColumnToBack(c);
			}
		}
	}
	VisitOperatorChildren(op);
}

} // namespace duckdb
