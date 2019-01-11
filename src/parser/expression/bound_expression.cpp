#include "parser/expression/bound_expression.hpp"

#include "common/exception.hpp"
#include "common/types/hash.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> BoundExpression::Copy() {
	return make_unique<BoundExpression>(return_type, index, depth);
}

void BoundExpression::Serialize(Serializer &serializer) {
	throw SerializationException("Cannot serialize a BoundExpression");
}

uint64_t BoundExpression::Hash() const {
	uint64_t result = Expression::Hash();
	result = CombineHash(result, duckdb::Hash<uint32_t>(index));
	return CombineHash(result, duckdb::Hash<uint32_t>(depth));
}

bool BoundExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (BoundExpression *)other_;
	return other->index == index && other->depth == depth;
}

string BoundExpression::ToString() const {
	return "#" + std::to_string(index);
}
