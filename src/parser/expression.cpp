#include "parser/expression.hpp"

#include "common/serializer.hpp"
#include "common/types/hash.hpp"
#include "parser/expression/list.hpp"

using namespace duckdb;
using namespace std;

void Expression::AcceptChildren(SQLNodeVisitor *v) {
	for (size_t i = 0; i < children.size(); i++) {
		auto accept_res = children[i]->Accept(v);
		if (accept_res) {
			children[i] = move(accept_res);
		}
	}
}

bool Expression::IsAggregate() {
	bool is_aggregate = false;
	for (auto &child : children) {
		is_aggregate |= child->IsAggregate();
	}
	return is_aggregate;
}

bool Expression::IsWindow() {
	bool is_window = false;
	for (auto &child : children) {
		is_window |= child->IsWindow();
	}
	return is_window;
}

bool Expression::IsScalar() {
	bool is_scalar = true;
	for (auto &child : children) {
		is_scalar &= child->IsScalar();
	}
	return is_scalar;
}

bool Expression::HasSubquery() {
	for (auto &child : children) {
		if (child->HasSubquery()) {
			return true;
		}
	}
	return false;
}

bool Expression::Equals(const Expression *other) const {
	if (!other) {
		return false;
	}
	if (this->type != other->type) {
		return false;
	}
	if (children.size() != other->children.size()) {
		return false;
	}
	for (size_t i = 0; i < children.size(); i++) {
		if (!children[i]->Equals(other->children[i].get())) {
			return false;
		}
	}
	return true;
}

string Expression::ToString() const {
	auto op = ExpressionTypeToOperator(type);
	if (!op.empty()) {
		if (children.size() == 0) {
			return "(" + op + ")";
		} else if (children.size() == 1) {
			return "(" + op + children[0]->ToString() + ")";
		} else if (children.size() == 2) {
			return "(" + children[0]->ToString() + " " + op + " " + children[1]->ToString() + ")";
		}
	}
	string result = ExpressionTypeToString(type);
	if (children.size() > 0) {
		result += "(";
		for (size_t i = 0; i < children.size(); i++) {
			auto &child = children[i];
			result += child->ToString();
			if (i < children.size() - 1) {
				result += ", ";
			}
		}
		result += ")";
	}
	return result;
}

void Expression::Serialize(Serializer &serializer) {
	serializer.Write<ExpressionClass>(GetExpressionClass());
	serializer.Write<ExpressionType>(type);
	serializer.Write<TypeId>(return_type);
	serializer.WriteString(alias);
	serializer.WriteList<Expression>(children);
}

unique_ptr<Expression> Expression::Deserialize(Deserializer &source) {
	ExpressionDeserializeInfo info;
	auto expression_class = source.Read<ExpressionClass>();
	info.type = source.Read<ExpressionType>();
	info.return_type = source.Read<TypeId>();
	auto alias = source.Read<string>();
	source.ReadList<Expression>(info.children);
	unique_ptr<Expression> result;
	switch (expression_class) {
	case ExpressionClass::AGGREGATE:
		result = AggregateExpression::Deserialize(&info, source);
		break;
	case ExpressionClass::CASE:
		result = CaseExpression::Deserialize(&info, source);
		break;
	case ExpressionClass::CAST:
		result = CastExpression::Deserialize(&info, source);
		break;
	case ExpressionClass::COLUMN_REF:
		result = ColumnRefExpression::Deserialize(&info, source);
		break;
	case ExpressionClass::COMPARISON:
		result = ComparisonExpression::Deserialize(&info, source);
		break;
	case ExpressionClass::CONJUNCTION:
		result = ConjunctionExpression::Deserialize(&info, source);
		break;
	case ExpressionClass::CONSTANT:
		result = ConstantExpression::Deserialize(&info, source);
		break;
	case ExpressionClass::DEFAULT:
		result = DefaultExpression::Deserialize(&info, source);
		break;
	case ExpressionClass::FUNCTION:
		result = FunctionExpression::Deserialize(&info, source);
		break;
	case ExpressionClass::OPERATOR:
		result = OperatorExpression::Deserialize(&info, source);
		break;
	case ExpressionClass::STAR:
		result = StarExpression::Deserialize(&info, source);
		break;
	case ExpressionClass::SUBQUERY:
		result = SubqueryExpression::Deserialize(&info, source);
		break;
	default:
		throw SerializationException("Unsupported type for aggregation deserialization!");
	}
	result->return_type = info.return_type;
	result->alias = alias;
	return result;
}

uint64_t Expression::Hash() const {
	uint64_t hash = duckdb::Hash<uint32_t>((uint32_t)type);
	for (auto &child : children) {
		hash = CombineHash(child->Hash(), hash);
	}
	return hash;
}

void Expression::EnumerateExpressions(
    unique_ptr<Expression> *parent, ExpressionType type,
    std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback) {
	for (size_t i = 0; i < (*parent)->children.size(); i++) {
		EnumerateExpressions(&(*parent)->children[i], type, callback);
	}
	if ((*parent)->type == type) {
		*parent = callback(move(*parent));
	}
}
