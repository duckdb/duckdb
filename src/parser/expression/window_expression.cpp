#include "parser/expression/window_expression.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

WindowExpression::WindowExpression(ExpressionType type, unique_ptr<Expression> child)
    : AggregateExpression(type, move(child)) {
}

unique_ptr<Expression> WindowExpression::Copy() {
	throw NotImplementedException("eek");
}

void WindowExpression::Serialize(Serializer &serializer) {
	throw NotImplementedException("eek");
}

unique_ptr<Expression> WindowExpression::Deserialize(ExpressionDeserializeInfo *info, Deserializer &source) {
	throw NotImplementedException("eek");
}

