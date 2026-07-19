#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

CastExpression::CastExpression(LogicalType target, unique_ptr<ParsedExpression> child, bool try_cast_p)
    : ParsedExpression(ExpressionType::OPERATOR_CAST, ExpressionClass::CAST), cast_type(std::move(target)),
      try_cast(try_cast_p) {
	D_ASSERT(child);
	this->child = std::move(child);
}

CastExpression::CastExpression() : ParsedExpression(ExpressionType::OPERATOR_CAST, ExpressionClass::CAST) {
}

string CastExpression::ToString() const {
	return ToString<CastExpression, ParsedExpression>(*this);
}

} // namespace duckdb
