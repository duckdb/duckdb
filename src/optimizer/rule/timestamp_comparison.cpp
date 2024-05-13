
#include "duckdb/optimizer/rule/timestamp_comparison.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/optimizer/matcher/type_matcher_id.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

TimeStampComparison::TimeStampComparison(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a ComparisonExpression that is an Equality and has a VARCHAR and ENUM as its children
	auto op = make_uniq<ComparisonExpressionMatcher>();
	// Enum requires expression to be root
	op->expr_type = make_uniq<SpecificExpressionTypeMatcher>(ExpressionType::COMPARE_EQUAL);

	// one side is timestamp cast to date
	auto left = make_uniq<CastExpressionMatcher>();
	left->type = make_uniq<TypeMatcherId>(LogicalTypeId::DATE);
	left->matcher = make_uniq<ExpressionMatcher>();
	left->matcher->type = make_uniq<TypeMatcherId>(LogicalTypeId::TIMESTAMP);
	op->matchers.push_back(std::move(left));

	// other side is varchar to date?
	auto right = make_uniq<ExpressionMatcher>();
	right->type = make_uniq<TypeMatcherId>(LogicalTypeId::DATE);
	op->matchers.push_back(std::move(right));

	root = std::move(op);
}

unique_ptr<Expression> TimeStampComparison::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                  bool &changes_made, bool is_root) {

	const Expression &ts_to_date = bindings[0].get();
	// rewrite it to ts >= date && ts < date + 1 day.

	const Expression &date = bindings[1].get();
	// ok so here now I need to figure out the other stuff.
	return nullptr;
}

}
