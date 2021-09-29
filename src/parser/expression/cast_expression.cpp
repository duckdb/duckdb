#include "duckdb/parser/expression/cast_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"

namespace duckdb {

CastExpression::CastExpression(LogicalType target, unique_ptr<ParsedExpression> child, bool try_cast_p)
    : ParsedExpression(ExpressionType::OPERATOR_CAST, ExpressionClass::CAST), cast_type(move(target)),
      try_cast(try_cast_p) {
	D_ASSERT(child);
	this->child = move(child);
}

string CastExpression::ToString() const {
	return (try_cast ? "TRY_CAST(" : "CAST(") + child->ToString() + " AS " + cast_type.ToString() + ")";
}

bool CastExpression::Equals(const CastExpression *a, const CastExpression *b) {
	if (!a->child->Equals(b->child.get())) {
		return false;
	}
	if (a->cast_type != b->cast_type) {
		return false;
	}
	if (a->try_cast != b->try_cast) {
		return false;
	}
	return true;
}

unique_ptr<ParsedExpression> CastExpression::Copy() const {
	auto copy = make_unique<CastExpression>(cast_type, child->Copy(), try_cast);
	copy->CopyProperties(*this);
	return move(copy);
}

void CastExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	child->Serialize(serializer);
	cast_type.Serialize(serializer);
	serializer.Write<bool>(try_cast);
}

unique_ptr<ParsedExpression> CastExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto child = ParsedExpression::Deserialize(source);
	auto cast_type = LogicalType::Deserialize(source);
	auto try_cast = source.Read<bool>();
	return make_unique_base<ParsedExpression, CastExpression>(cast_type, move(child), try_cast);
}

} // namespace duckdb
