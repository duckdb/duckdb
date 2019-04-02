#include "catch.hpp"
#include "duckdb.hpp"
#include "optimizer/expression_rewriter.hpp"
#include "parser/parsed_expression.hpp"
#include "planner/expression.hpp"
#include "planner/planner.hpp"

namespace duckdb {

class ClientContext;

class ExpressionHelper {
public:
	ExpressionHelper();

	unique_ptr<Expression> ParseExpression(string expression);
	unique_ptr<Expression> ApplyExpressionRule(unique_ptr<Expression> root);

	unique_ptr<LogicalOperator> ParseLogicalTree(string query);

	template <class T> void AddRule() {
		rewriter.rules.push_back(make_unique<T>(rewriter));
	}

	bool VerifyRewrite(string input, string expected_output);

	string AddColumns(string columns);

	DuckDB db;
	Connection con;
private:
	ExpressionRewriter rewriter;

	string from_clause;
};

} // namespace duckdb
