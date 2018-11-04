
#include <string>

#include "optimizer/rewriter.hpp"
#include "parser/expression.hpp"
#include "planner/planner.hpp"

namespace duckdb {

class DuckDBConnection;

std::unique_ptr<Expression> ParseExpression(std::string expression);
std::unique_ptr<Expression> ApplyExprRule(Rewriter &rewriter,
                                          std::unique_ptr<Expression> root);
std::unique_ptr<Planner> ParseLogicalPlan(DuckDBConnection& con, std::string query);

} // namespace duckdb
