#include "parser/expression/window_expression.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

WindowExpression::WindowExpression(ExpressionType type, unique_ptr<ParsedExpression> child) :
	ParsedExpression(type, ExpressionClass::WINDOW) {
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

string WindowExpression::ToString() const {
	return "WINDOW";
}

bool WindowExpression::Equals(const ParsedExpression *other_) const {
	if (!ParsedExpression::Equals(other_)) {
		return false;
	}
	auto other = (WindowExpression *)other_;

	if (start != other->start || end != other->end) {
		return false;
	}
	// check if the child expressions are equivalent
	if (!ParsedExpression::Equals(child.get(), other->child.get()) ||
	    !ParsedExpression::Equals(start_expr.get(), other->start_expr.get()) ||
	    !ParsedExpression::Equals(end_expr.get(), other->end_expr.get()) ||
	    !ParsedExpression::Equals(offset_expr.get(), other->offset_expr.get()) ||
	    !ParsedExpression::Equals(default_expr.get(), other->default_expr.get())) {
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

unique_ptr<ParsedExpression> WindowExpression::Copy() {
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

	return move(new_window);
}

void WindowExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
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

unique_ptr<ParsedExpression> WindowExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto child = source.ReadOptional<ParsedExpression>();
	auto expr = make_unique<WindowExpression>(type, move(child));
	source.ReadList<ParsedExpression>(expr->partitions);

	auto order_count = source.Read<uint32_t>();
	for (size_t i = 0; i < order_count; i++) {
		auto order_type = source.Read<OrderType>();
		auto expression = ParsedExpression::Deserialize(source);
		expr->ordering.orders.push_back(OrderByNode(order_type, move(expression)));
	}
	expr->start = (WindowBoundary)source.Read<uint8_t>();
	expr->end = (WindowBoundary)source.Read<uint8_t>();

	expr->start_expr = source.ReadOptional<ParsedExpression>();
	expr->end_expr = source.ReadOptional<ParsedExpression>();
	expr->offset_expr = source.ReadOptional<ParsedExpression>();
	expr->default_expr = source.ReadOptional<ParsedExpression>();
	return move(expr);
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

ParsedExpression *WindowExpression::GetChild(size_t index) const {
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

void WindowExpression::ReplaceChild(std::function<unique_ptr<ParsedExpression>(unique_ptr<ParsedExpression> expression)> callback,
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
