
#include "parser/expression/indexref_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

void IndexRefExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	serializer.Write<int>(index);
}

unique_ptr<Expression>
IndexRefExpression::Deserialize(ExpressionDeserializeInformation *info,
                                Deserializer &source) {
	bool failed = false;
	int index = source.Read<int>(failed);
	if (failed) {
		return nullptr;
	}
	auto expression = make_unique_base<Expression, IndexRefExpression>(index);
	expression->children = move(info->children);
	return expression;
}

void IndexRefExpression::ResolveType() {
	Expression::ResolveType();
	if (return_type == TypeId::INVALID) {
		throw Exception("Type of ColumnRefExpression was not resolved!");
	}
}

bool IndexRefExpression::Equals(const Expression *other_) {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = reinterpret_cast<const IndexRefExpression *>(other_);
	if (!other) {
		return false;
	}
	return index == other->index && depth == other->depth;
}

string IndexRefExpression::ToString() const {
	return "[" + to_string(index) + "]";
}
