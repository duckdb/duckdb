#include "duckdb/optimizer/filter_pullup.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> FilterPullup::Rewrite(unique_ptr<LogicalOperator> op)
{
	switch (op->logical_type) {
	case LogicalOperatorType::LOGICAL_FILTER:
		return PullupFilter(std::move(op));
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return PullupProjection(std::move(op));
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return PullupCrossProduct(std::move(op));
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		return PullupJoin(std::move(op));
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT:
		return PullupSetOperation(std::move(op));
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	{
		// we can just pull directly through these operations without any rewriting
		op->children[0] = Rewrite(unique_ptr<LogicalOperator>((LogicalOperator*)op->children[0].get()));
		return op;
	}
	default:
		return FinishPullup(std::move(op));
	}
}

unique_ptr<LogicalOperator> FilterPullup::PullupJoin(unique_ptr<LogicalOperator> op)
{
	D_ASSERT(op->logical_type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op->logical_type == LogicalOperatorType::LOGICAL_ASOF_JOIN || op->logical_type == LogicalOperatorType::LOGICAL_ANY_JOIN || op->logical_type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
	auto &join = op->Cast<LogicalJoin>();
	switch (join.join_type)
	{
		case JoinType::INNER:
			return PullupInnerJoin(std::move(op));
		case JoinType::LEFT:
		case JoinType::ANTI:
		case JoinType::SEMI:
		{
			return PullupFromLeft(std::move(op));
		}
		default:
			// unsupported join type: call children pull up
			return FinishPullup(std::move(op));
	}
}

unique_ptr<LogicalOperator> FilterPullup::PullupInnerJoin(unique_ptr<LogicalOperator> op)
{
	D_ASSERT(op->Cast<LogicalJoin>().join_type == JoinType::INNER);
	if (op->logical_type == LogicalOperatorType::LOGICAL_DELIM_JOIN)
	{
		return op;
	}
	return PullupBothSide(std::move(op));
}

unique_ptr<LogicalOperator> FilterPullup::PullupCrossProduct(unique_ptr<LogicalOperator> op)
{
	D_ASSERT(op->logical_type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT);
	return PullupBothSide(std::move(op));
}

unique_ptr<LogicalOperator> FilterPullup::GeneratePullupFilter(unique_ptr<LogicalOperator> child, vector<unique_ptr<Expression>> &expressions)
{
	unique_ptr<LogicalFilter> filter = make_uniq<LogicalFilter>();
	for (idx_t i = 0; i < expressions.size(); ++i)
	{
		filter->expressions.push_back(std::move(expressions[i]));
	}
	expressions.clear();
	filter->children.push_back(std::move(child));
	return std::move(filter);
}

unique_ptr<LogicalOperator> FilterPullup::FinishPullup(unique_ptr<LogicalOperator> op)
{
	// unhandled type, first perform filter pushdown in its children
	for (idx_t i = 0; i < op->children.size(); i++)
	{
		FilterPullup pullup;
		op->children[i] = pullup.Rewrite(unique_ptr<LogicalOperator>((LogicalOperator*)op->children[i].get()));
	}
	// now pull up any existing filters
	if (filters_expr_pullup.empty())
	{
		// no filters to pull up
		return op;
	}
	return GeneratePullupFilter(std::move(op), filters_expr_pullup);
}
} // namespace duckdb
