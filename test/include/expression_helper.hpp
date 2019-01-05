// #include "optimizer/rewriter.hpp"
#include "parser/expression.hpp"
#include "planner/planner.hpp"

#include <string>

namespace duckdb {

class DuckDBConnection;

unique_ptr<Expression> ParseExpression(string expression);
// unique_ptr<Expression> ApplyExprRule(Rewriter &rewriter, unique_ptr<Expression> root);
unique_ptr<Planner> ParseLogicalPlan(DuckDBConnection &con, string query);
// unique_ptr<LogicalOperator> ApplyLogicalRule(Rewriter &rewriter, unique_ptr<LogicalOperator> op);

} // namespace duckdb
