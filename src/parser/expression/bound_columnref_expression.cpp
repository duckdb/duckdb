#include "parser/expression/bound_columnref_expression.hpp"

#include "common/exception.hpp"
#include "common/types/hash.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> BoundColumnRefExpression::Copy() const{
	return make_unique<BoundColumnRefExpression>(alias, return_type, binding, depth);
}

void BoundColumnRefExpression::Serialize(Serializer &serializer) {
	throw SerializationException("Cannot serialize a BoundColumnRefExpression");
}

uint64_t BoundColumnRefExpression::Hash() const {
	auto result = Expression::Hash();
	result = CombineHash(result, duckdb::Hash<uint32_t>(binding.column_index));
	result = CombineHash(result, duckdb::Hash<uint32_t>(binding.table_index));
	return CombineHash(result, duckdb::Hash<uint32_t>(depth));
}

bool BoundColumnRefExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (BoundColumnRefExpression *)other_;
	return other->binding == binding && other->depth == depth;
}

string BoundColumnRefExpression::ToString() const {
	return !alias.empty() ? alias : "#[" + to_string(binding.table_index) + "." + to_string(binding.column_index) + "]";
}
