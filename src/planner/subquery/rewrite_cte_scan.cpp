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

namespace duckdb {

RewriteCTEScan::RewriteCTEScan(idx_t table_index, const vector<CorrelatedColumnInfo> &correlated_columns)
    : table_index(table_index), correlated_columns(correlated_columns) {
}

void RewriteCTEScan::VisitOperator(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cteref = op.Cast<LogicalCTERef>();

		if (cteref.cte_index == table_index) {
			for (auto &c : this->correlated_columns) {
				cteref.chunk_types.push_back(c.type);
				cteref.bound_columns.push_back(c.name);
			}
			cteref.correlated_columns += correlated_columns.size();
		}
	}
	VisitOperatorChildren(op);
}

} // namespace duckdb
