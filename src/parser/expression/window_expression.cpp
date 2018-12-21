#include "parser/expression/window_expression.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

WindowExpression::WindowExpression(ExpressionType type, unique_ptr<Expression> child) : Expression(type) {
	switch (type) {
	case ExpressionType::WINDOW_SUM:
	case ExpressionType::WINDOW_COUNT_STAR:
	case ExpressionType::WINDOW_MIN:
	case ExpressionType::WINDOW_MAX:
	case ExpressionType::WINDOW_AVG:
	case ExpressionType::WINDOW_ROW_NUMBER:
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_LAST_VALUE:
	case ExpressionType::WINDOW_RANK:
	case ExpressionType::WINDOW_RANK_DENSE:

		break;
	default:
		throw NotImplementedException("Window aggregate type %s not supported", ExpressionTypeToString(type).c_str());
	}
	if (child) {
		AddChild(move(child));
	}
}

bool WindowExpression::IsWindow() {
	return true;
}

unique_ptr<Expression> WindowExpression::Copy() {
	if (children.size() > 1) {
		assert(0);
		return nullptr;
	}

	auto child = children.size() == 1 ? children[0]->Copy() : nullptr;
	auto new_window = make_unique<WindowExpression>(type, move(child));
	new_window->CopyProperties(*this);

	for (auto &e : partitions) {
		new_window->partitions.push_back(e->Copy());
	}

	for (auto &o : ordering.orders) {
		OrderByNode node;
		node.type = o.type;
		node.expression = o.expression->Copy();
		new_window->ordering.orders.push_back(move(node));
	}

	new_window->window_type = window_type;

	new_window->start = start;
	new_window->end = end;
	new_window->start_expr = start_expr ? start_expr->Copy() : nullptr;
	new_window->end_expr = end_expr ? end_expr->Copy() : nullptr;

	return new_window;
}

void WindowExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	serializer.WriteList(partitions);
	//	auto order_count = source.Read<uint32_t>();
	serializer.Write<uint32_t>(ordering.orders.size());
	for (auto &order : ordering.orders) {
		serializer.Write<OrderType>(order.type);
		order.expression->Serialize(serializer);
	}
	serializer.Write<uint8_t>(window_type);
	serializer.Write<uint8_t>(start);
	serializer.Write<uint8_t>(end);

	serializer.WriteOptional(start_expr);
	serializer.WriteOptional(end_expr);
}

unique_ptr<Expression> WindowExpression::Deserialize(ExpressionDeserializeInfo *info, Deserializer &source) {
	auto child = info->children.size() == 0 ? nullptr : move(info->children[0]);
	auto expr = make_unique<WindowExpression>(info->type, move(child));
	source.ReadList<Expression>(expr->partitions);

	auto order_count = source.Read<uint32_t>();
	for (size_t i = 0; i < order_count; i++) {
		auto order_type = source.Read<OrderType>();
		auto expression = Expression::Deserialize(source);
		expr->ordering.orders.push_back(OrderByNode(order_type, move(expression)));
	}
	expr->window_type = (WindowType)source.Read<uint8_t>();
	expr->start = (WindowBoundary)source.Read<uint8_t>();
	expr->end = (WindowBoundary)source.Read<uint8_t>();

	expr->start_expr = source.ReadOptional<Expression>();
	expr->end_expr = source.ReadOptional<Expression>();
	return expr;
}

//! Resolve the type of the window function
void WindowExpression::ResolveType() {
	Expression::ResolveType();

	if (start_expr) {
		start_expr->ResolveType();
	}
	if (end_expr) {
		end_expr->ResolveType();
	}

	for (auto &order : ordering.orders) {
		order.expression->ResolveType();
	}

	for (auto &part : partitions) {
		part->ResolveType();
	}

	switch (type) {

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
	case ExpressionType::WINDOW_AVG:
		return_type = TypeId::DECIMAL;
		break;
	case ExpressionType::WINDOW_ROW_NUMBER:
	case ExpressionType::WINDOW_COUNT_STAR:
	case ExpressionType::WINDOW_RANK:
	case ExpressionType::WINDOW_RANK_DENSE:
		return_type = TypeId::BIGINT;
		break;
	case ExpressionType::WINDOW_MIN:
	case ExpressionType::WINDOW_MAX:
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
