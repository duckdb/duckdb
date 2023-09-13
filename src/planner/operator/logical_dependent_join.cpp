#include "duckdb/planner/operator/logical_dependent_join.hpp"

namespace duckdb {

LogicalDependentJoin::LogicalDependentJoin(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right,
                                           vector<CorrelatedColumnInfo> correlated_columns, JoinType type,
                                           unique_ptr<Expression> condition)
    : LogicalComparisonJoin(type, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN), join_condition(std::move(condition)),
      correlated_columns(std::move(correlated_columns)) {
	children.push_back(std::move(left));
	children.push_back(std::move(right));
}

unique_ptr<LogicalOperator> LogicalDependentJoin::Create(unique_ptr<LogicalOperator> left,
                                                         unique_ptr<LogicalOperator> right,
                                                         vector<CorrelatedColumnInfo> correlated_columns, JoinType type,
                                                         unique_ptr<Expression> condition) {
	return make_uniq<LogicalDependentJoin>(std::move(left), std::move(right), std::move(correlated_columns), type,
	                                       std::move(condition));
}

} // namespace duckdb
