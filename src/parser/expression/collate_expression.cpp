#include "duckdb/parser/expression/collate_expression.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

CollateExpression::CollateExpression(string collation_p, unique_ptr<ParsedExpression> child)
    : ParsedExpression(ExpressionType::COLLATE, ExpressionClass::COLLATE), collation(std::move(collation_p)) {
	D_ASSERT(child);
	this->child = std::move(child);
}

CollateExpression::CollateExpression() : ParsedExpression(ExpressionType::COLLATE, ExpressionClass::COLLATE) {
}

string CollateExpression::ToString() const {
	return StringUtil::Format("%s COLLATE %s", child->ToString(), SQLIdentifier(collation));
}

unique_ptr<ParsedExpression> CollateExpression::Copy() const {
	auto copy = make_uniq<CollateExpression>(collation, child->Copy());
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb
