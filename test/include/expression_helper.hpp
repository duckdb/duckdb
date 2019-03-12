#include "catch.hpp"
#include "duckdb.hpp"
#include "optimizer/expression_rewriter.hpp"
#include "parser/expression.hpp"
#include "planner/planner.hpp"

namespace duckdb {

class ClientContext;

class ExpressionHelper {
public:
	ExpressionHelper(ClientContext &context);

	unique_ptr<Expression> ParseExpression(string expression);
	unique_ptr<Expression> ApplyExpressionRule(unique_ptr<Expression> root,
	                                           LogicalOperatorType root_type = LogicalOperatorType::PROJECTION);

	template <class T> void AddRule() {
		rewriter.rules.push_back(make_unique<T>(rewriter));
	}

	bool VerifyRewrite(string input, string expected_output);

private:
	ClientContext &context;
	ExpressionRewriter rewriter;
};

// unique_ptr<LogicalOperator> ApplyLogicalRule(Rewriter &rewriter, unique_ptr<LogicalOperator> op);

} // namespace duckdb
