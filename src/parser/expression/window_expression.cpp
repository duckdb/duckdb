#include "parser/expression/window_expression.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

WindowExpression::WindowExpression(ExpressionType type, unique_ptr<Expression> child) : Expression(type) {
	switch (type) {
	case ExpressionType::WINDOW_SUM:
	case ExpressionType::WINDOW_ROW_NUMBER:
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_LAST_VALUE:

		break;
	default:
		throw NotImplementedException("Window aggregate type %s not supported", ExpressionTypeToString(type).c_str());
	}
	if (child) {
		AddChild(move(child));
	}
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

//! Resolve the type of the aggregate
void WindowExpression::ResolveType() {
	Expression::ResolveType();
	switch (type) {

	// TODO: this is copied pretty verbatim from aggregate_expression.cpp, avoid duplication
	case ExpressionType::WINDOW_SUM:
		if (children[0]->IsScalar()) {
			stats.has_stats = false;
			switch (children[0]->return_type) {
			case TypeId::BOOLEAN:
			case TypeId::TINYINT:
			case TypeId::SMALLINT:
			case TypeId::INTEGER:
			case TypeId::BIGINT:
				return_type = TypeId::BIGINT;
				break;
			default:
				return_type = children[0]->return_type;
			}
		} else {
			ExpressionStatistics::Count(children[0]->stats, stats);
			ExpressionStatistics::Sum(children[0]->stats, stats);
			return_type = max(children[0]->return_type, stats.MinimalType());
		}

		break;
	case ExpressionType::WINDOW_ROW_NUMBER:
	case ExpressionType::WINDOW_RANK:
		return_type = TypeId::BIGINT;
		break;
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_LAST_VALUE:
		if (children.size() != 1) {
			throw Exception("Window functions FIRST_VALUE and LAST_VALUE need an expression");
		}
		return_type = children[0]->return_type;
		break;
	default:
		throw NotImplementedException("Unsupported window type!");
	}
}
