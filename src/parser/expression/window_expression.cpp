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
	case ExpressionType::WINDOW_PERCENT_RANK:
	case ExpressionType::WINDOW_CUME_DIST:
	case ExpressionType::WINDOW_LEAD:
	case ExpressionType::WINDOW_LAG:
	case ExpressionType::WINDOW_NTILE:

		break;
	default:
		throw NotImplementedException("Window aggregate type %s not supported", ExpressionTypeToString(type).c_str());
	}
	if (child) {
		this->child = move(child);
	}
}

unique_ptr<Expression> WindowExpression::Copy() {
	auto child_copy = child ? child->Copy() : nullptr;
	auto new_window = make_unique<WindowExpression>(type, move(child_copy));
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

	new_window->start = start;
	new_window->end = end;
	new_window->start_expr = start_expr ? start_expr->Copy() : nullptr;
	new_window->end_expr = end_expr ? end_expr->Copy() : nullptr;
	new_window->offset_expr = offset_expr ? offset_expr->Copy() : nullptr;
	new_window->default_expr = default_expr ? default_expr->Copy() : nullptr;

	return new_window;
}

size_t WindowExpression::ChildCount() const {
	size_t count = partitions.size() + ordering.orders.size();
	if (child) {
		count++;
	}
	if (offset_expr) {
		count++;
	}
	if (default_expr) {
		count++;
	}
	return count;
}

Expression *WindowExpression::GetChild(size_t index) const {
	if (index < partitions.size()) {
		return partitions[index].get();
	}
	index -= partitions.size();
	if (index < ordering.orders.size()) {
		return ordering.orders[index].expression.get();
	}
	index -= ordering.orders.size();
	if (child) {
		if (index == 0) {
			return child.get();
		} else {
			index--;
		}
	}
	if (offset_expr) {
		if (index == 0) {
			return offset_expr.get();
		} else {
			index--;
		}
	}
	assert(index == 0 && default_expr);
	return default_expr.get();
}

void WindowExpression::ReplaceChild(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
                                    size_t index) {
	if (index < partitions.size()) {
		partitions[index] = callback(move(partitions[index]));
		return;
	}
	index -= partitions.size();
	if (index < ordering.orders.size()) {
		ordering.orders[index].expression = callback(move(ordering.orders[index].expression));
		return;
	}
	index -= ordering.orders.size();
	if (child) {
		if (index == 0) {
			child = callback(move(child));
			return;
		} else {
			index--;
		}
	}
	if (offset_expr) {
		if (index == 0) {
			offset_expr = callback(move(offset_expr));
			return;
		} else {
			index--;
		}
	}
	assert(index == 0 && default_expr);
	default_expr = callback(move(default_expr));
}

void WindowExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	serializer.WriteOptional(child);
	serializer.WriteList(partitions);
	//	auto order_count = source.Read<uint32_t>();
	serializer.Write<uint32_t>(ordering.orders.size());
	for (auto &order : ordering.orders) {
		serializer.Write<OrderType>(order.type);
		order.expression->Serialize(serializer);
	}
	serializer.Write<uint8_t>(start);
	serializer.Write<uint8_t>(end);

	serializer.WriteOptional(start_expr);
	serializer.WriteOptional(end_expr);
	serializer.WriteOptional(offset_expr);
	serializer.WriteOptional(default_expr);
}

unique_ptr<Expression> WindowExpression::Deserialize(ExpressionType type, TypeId return_type, Deserializer &source) {
	auto child = source.ReadOptional<Expression>();
	auto expr = make_unique<WindowExpression>(type, move(child));
	source.ReadList<Expression>(expr->partitions);

	auto order_count = source.Read<uint32_t>();
	for (size_t i = 0; i < order_count; i++) {
		auto order_type = source.Read<OrderType>();
		auto expression = Expression::Deserialize(source);
		expr->ordering.orders.push_back(OrderByNode(order_type, move(expression)));
	}
	expr->start = (WindowBoundary)source.Read<uint8_t>();
	expr->end = (WindowBoundary)source.Read<uint8_t>();

	expr->start_expr = source.ReadOptional<Expression>();
	expr->end_expr = source.ReadOptional<Expression>();
	expr->offset_expr = source.ReadOptional<Expression>();
	expr->default_expr = source.ReadOptional<Expression>();
	return expr;
}

bool WindowExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (WindowExpression *)other_;

	if (start != other->start || end != other->end) {
		return false;
	}
	// check if the child expressions are equivalent
	if (!Expression::Equals(child.get(), other->child.get()) ||
	    !Expression::Equals(start_expr.get(), other->start_expr.get()) ||
	    !Expression::Equals(end_expr.get(), other->end_expr.get()) ||
	    !Expression::Equals(offset_expr.get(), other->offset_expr.get()) ||
	    !Expression::Equals(default_expr.get(), other->default_expr.get())) {
		return false;
	}

	// check if the partitions are equivalent
	if (partitions.size() != other->partitions.size()) {
		return false;
	}
	for (size_t i = 0; i < partitions.size(); i++) {
		if (!partitions[i]->Equals(other->partitions[i].get())) {
			return false;
		}
	}
	// check if the orderings are equivalent
	if (ordering.orders.size() != other->ordering.orders.size()) {
		return false;
	}
	for (size_t i = 0; i < ordering.orders.size(); i++) {
		if (ordering.orders[i].type != other->ordering.orders[i].type) {
			return false;
		}
		if (!ordering.orders[i].expression->Equals(other->ordering.orders[i].expression.get())) {
			return false;
		}
	}
	return true;
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
	if (offset_expr) {
		offset_expr->ResolveType();
	}
	if (default_expr) {
		default_expr->ResolveType();
	}

	for (auto &order : ordering.orders) {
		order.expression->ResolveType();
	}

	for (auto &part : partitions) {
		part->ResolveType();
	}

	switch (type) {
	case ExpressionType::WINDOW_SUM:
		if (child->IsScalar()) {
			stats.has_stats = false;
			switch (child->return_type) {
			case TypeId::BOOLEAN:
			case TypeId::TINYINT:
			case TypeId::SMALLINT:
			case TypeId::INTEGER:
			case TypeId::BIGINT:
				return_type = TypeId::BIGINT;
				break;
			default:
				return_type = child->return_type;
			}
		} else {
			ExpressionStatistics::Count(child->stats, stats);
			ExpressionStatistics::Sum(child->stats, stats);
			return_type = max(child->return_type, stats.MinimalType());
		}

		break;
	case ExpressionType::WINDOW_AVG:
	case ExpressionType::WINDOW_PERCENT_RANK:
	case ExpressionType::WINDOW_CUME_DIST:
		return_type = TypeId::DECIMAL;
		break;
	case ExpressionType::WINDOW_ROW_NUMBER:
	case ExpressionType::WINDOW_COUNT_STAR:
	case ExpressionType::WINDOW_RANK:
	case ExpressionType::WINDOW_RANK_DENSE:
	case ExpressionType::WINDOW_NTILE:
		return_type = TypeId::BIGINT;
		break;
	case ExpressionType::WINDOW_MIN:
	case ExpressionType::WINDOW_MAX:
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_LAST_VALUE:
		if (!child) {
			throw Exception("Window function needs an expression");
		}
		return_type = child->return_type;
		break;
	case ExpressionType::WINDOW_LEAD:
	case ExpressionType::WINDOW_LAG:
		if (!child) {
			throw Exception("Window function LEAD/LAG needs at least one expression");
		}
		return_type = child->return_type;
		break;
	default:
		throw NotImplementedException("Unsupported window type!");
	}
}
