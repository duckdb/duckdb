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

static bool ContainsCTERef(LogicalOperator &op, idx_t table_index) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cteref = op.Cast<LogicalCTERef>();
		if (cteref.cte_index == table_index) {
			return true;
		}
	}
	for (auto &child : op.children) {
		if (ContainsCTERef(*child, table_index)) {
			return true;
		}
	}
	return false;
}

static CorrelatedColumns ReorderCorrelatedColumns(const CorrelatedColumns &current_columns,
                                                  const CorrelatedColumns &target_columns, bool target_first) {
	vector<CorrelatedColumnInfo> cte_columns;
	cte_columns.reserve(target_columns.size());
	vector<bool> used(current_columns.size(), false);
	for (const auto &target_column : target_columns) {
		idx_t idx = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < current_columns.size(); i++) {
			if (current_columns[i].binding == target_column.binding) {
				idx = i;
				break;
			}
		}
		if (idx != DConstants::INVALID_INDEX) {
			cte_columns.push_back(current_columns[idx]);
			used[idx] = true;
		} else {
			cte_columns.push_back(target_column);
		}
	}

	vector<CorrelatedColumnInfo> other_columns;
	for (idx_t i = 0; i < current_columns.size(); i++) {
		if (!used[i]) {
			other_columns.push_back(current_columns[i]);
		}
	}

	CorrelatedColumns reordered;
	auto &first = target_first ? cte_columns : other_columns;
	auto &second = target_first ? other_columns : cte_columns;
	for (auto &column : first) {
		reordered.AddColumnToBack(std::move(column));
	}
	for (auto &column : second) {
		reordered.AddColumnToBack(std::move(column));
	}

	if (current_columns.GetDelimIndex() < current_columns.size()) {
		auto delim_binding = current_columns[current_columns.GetDelimIndex()].binding;
		for (idx_t i = 0; i < reordered.size(); i++) {
			if (reordered[i].binding == delim_binding) {
				reordered.SetDelimIndex(i);
				break;
			}
		}
	}
	return reordered;
}

RewriteCTEScan::RewriteCTEScan(idx_t table_index, const CorrelatedColumns &correlated_columns, CTEScanRewriteMode mode)
    : table_index(table_index), correlated_columns(correlated_columns), mode(mode) {
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
	} else if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN &&
	           (mode == CTEScanRewriteMode::WITH_NON_RECURSIVE_DEPENDENT_JOINS ||
	            mode == CTEScanRewriteMode::WITH_RECURSIVE_DEPENDENT_JOINS)) {
		// There is another DependentJoin below the correlated recursive CTE.
		// We have to add the correlated columns of the recursive CTE to the
		// set of columns of this operator.
		auto &join = op.Cast<LogicalDependentJoin>();
		if (mode == CTEScanRewriteMode::WITH_NON_RECURSIVE_DEPENDENT_JOINS) {
			bool has_cte_ref = false;
			for (auto &child : join.children) {
				if (ContainsCTERef(*child, table_index)) {
					has_cte_ref = true;
					break;
				}
			}
			if (!has_cte_ref) {
				VisitOperatorChildren(op);
				return;
			}
		}

		join.correlated_columns = ReorderCorrelatedColumns(join.correlated_columns, correlated_columns,
		                                                   mode == CTEScanRewriteMode::WITH_RECURSIVE_DEPENDENT_JOINS);
	} else if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN && mode != CTEScanRewriteMode::CTE_REF_ONLY) {
		throw InternalException("Unsupported CTEScanRewriteMode in RewriteCTEScan");
	}
	VisitOperatorChildren(op);
}

} // namespace duckdb
