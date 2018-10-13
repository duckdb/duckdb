
#include <string>

#include "parser/expression.hpp"

namespace duckdb {

std::unique_ptr<Expression> ParseExpression(std::string expression);

} // namespace duckdb
