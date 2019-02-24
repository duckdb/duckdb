#include "parser/expression/bound_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"
#include "common/types/hash.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> BoundExpression::Copy() {
	return make_unique<BoundExpression>(return_type, index);
}

void BoundExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	serializer.Write<uint32_t>(index);
	serializer.Write<uint32_t>(depth);
}

unique_ptr<Expression> BoundExpression::Deserialize(ExpressionType type, TypeId return_type, Deserializer &source) {
	auto index = source.Read<uint32_t>();
	auto depth = source.Read<uint32_t>();
	auto expression = make_unique<BoundExpression>(return_type, index, depth);
	return move(expression);
}

uint64_t BoundExpression::Hash() const {
	return CombineHash(Expression::Hash(), duckdb::Hash<uint32_t>(index));
}

bool BoundExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (BoundExpression *)other_;
	return other->index == index;
}

string BoundExpression::ToString() const {
	return "#" + std::to_string(index);
}
