#include "parser/expression/window_expression.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

WindowExpression::WindowExpression(ExpressionType type, unique_ptr<Expression> child)
    : AggregateExpression(type, move(child)) {
}

unique_ptr<Expression> WindowExpression::Accept(SQLNodeVisitor *v) {
	// TODO what about a return value here?
	auto res = v->Visit(*this);
	assert(res == nullptr);
	for (size_t expr_idx = 0; expr_idx < partitions.size(); expr_idx++) {
		unique_ptr<Expression> ret = partitions[expr_idx]->Accept(v);
		if (ret != nullptr) {
			partitions[expr_idx] = move(ret);
		}
	}
	for (size_t expr_idx = 0; expr_idx < ordering.orders.size(); expr_idx++) {
		unique_ptr<Expression> ret = ordering.orders[expr_idx].expression->Accept(v);
		if (ret != nullptr) {
			ordering.orders[expr_idx].expression = move(ret);
		}
	}
	return nullptr;
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

