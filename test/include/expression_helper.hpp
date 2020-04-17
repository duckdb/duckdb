#pragma once

#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/planner.hpp"

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

	bool VerifyRewrite(string input, string expected_output, bool silent = false);

	string AddColumns(string columns);

	DuckDB db;
	Connection con;

private:
	ExpressionRewriter rewriter;

	string from_clause;
};

} // namespace duckdb
