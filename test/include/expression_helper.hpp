#include "catch.hpp"
#include "duckdb.hpp"
#include "optimizer/expression_rewriter.hpp"
#include "parser/parsed_expression.hpp"
#include "planner/parsed_expression.hpp"
#include "planner/planner.hpp"

namespace duckdb {

class ClientContext;

class ExpressionHelper {
public:
	ExpressionHelper(ClientContext &context);

	unique_ptr<ParsedExpression> ParseExpression(string expression);
	unique_ptr<Expression> BindExpression(string expression);
	unique_ptr<Expression> ApplyExpressionRule(unique_ptr<Expression> root,
	                                           LogicalOperatorType root_type = LogicalOperatorType::PROJECTION);

	unique_ptr<LogicalOperator> ParseLogicalTree(string query);

	template <class T> void AddRule() {
		rewriter.rules.push_back(make_unique<T>(rewriter));
	}

	bool VerifyRewrite(string input, string expected_output);

private:
	ClientContext &context;
	ExpressionRewriter rewriter;
};

} // namespace duckdb
