#include "parser/expression/groupref_expression.hpp"

#include "common/exception.hpp"
#include "common/types/hash.hpp"

using namespace duckdb;
using namespace std;

GroupRefExpression::GroupRefExpression(TypeId return_type, size_t group_index)
    : Expression(ExpressionType::GROUP_REF, return_type), group_index(group_index) {
}

unique_ptr<Expression> GroupRefExpression::Copy() {
	assert(children.size() == 0);
	auto copy = make_unique<GroupRefExpression>(return_type, group_index);
	copy->CopyProperties(*this);
	return copy;
}

bool GroupRefExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (GroupRefExpression *)other_;
	return group_index == other->group_index;
}

uint64_t GroupRefExpression::Hash() const {
	uint64_t result = Expression::Hash();
	return CombineHash(result, duckdb::Hash(group_index));
}
