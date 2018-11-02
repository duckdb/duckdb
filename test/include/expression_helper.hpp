
#include <string>

#include "optimizer/rewriter.hpp"
#include "parser/expression.hpp"

namespace duckdb {

std::unique_ptr<Expression> ParseExpression(std::string expression);
std::unique_ptr<Expression> ApplyExprRule(Rewriter &rewriter,
                                          std::unique_ptr<Expression> root);

} // namespace duckdb
