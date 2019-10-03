#include "parser/expression/window_expression.hpp"

#include "common/serializer.hpp"
#include "common/string_util.hpp"

using namespace duckdb;
using namespace std;

WindowExpression::WindowExpression(ExpressionType type, string schema, string function_name)
    : ParsedExpression(type, ExpressionClass::WINDOW), schema(schema),
      function_name(StringUtil::Lower(function_name)) {
	switch (type) {
	case ExpressionType::WINDOW_AGGREGATE:
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
}

string WindowExpression::ToString() const {
	return "WINDOW";
}

bool WindowExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (WindowExpression *)other_;

	// check if the child expressions are equivalent
	if (other->children.size() != children.size()) {
		return false;
	}
	for (index_t i = 0; i < children.size(); i++) {
		if (!children[i]->Equals(other->children[i].get())) {
			return false;
		}
	}
	if (start != other->start || end != other->end) {
		return false;
	}
	// check if the framing expressions are equivalent
	if (!BaseExpression::Equals(start_expr.get(), other->start_expr.get()) ||
	    !BaseExpression::Equals(end_expr.get(), other->end_expr.get()) ||
	    !BaseExpression::Equals(offset_expr.get(), other->offset_expr.get()) ||
	    !BaseExpression::Equals(default_expr.get(), other->default_expr.get())) {
		return false;
	}

	// check if the partitions are equivalent
	if (partitions.size() != other->partitions.size()) {
		return false;
	}
	for (index_t i = 0; i < partitions.size(); i++) {
		if (!partitions[i]->Equals(other->partitions[i].get())) {
			return false;
		}
	}
	// check if the orderings are equivalent
	if (orders.size() != other->orders.size()) {
		return false;
	}
	for (index_t i = 0; i < orders.size(); i++) {
		if (orders[i].type != other->orders[i].type) {
			return false;
		}
		if (!orders[i].expression->Equals(other->orders[i].expression.get())) {
			return false;
		}
	}
	return true;
}

unique_ptr<ParsedExpression> WindowExpression::Copy() const {
	auto new_window = make_unique<WindowExpression>(type, schema, function_name);
	new_window->CopyProperties(*this);

	for (auto &child : children) {
		new_window->children.push_back(child->Copy());
	}

	for (auto &e : partitions) {
		new_window->partitions.push_back(e->Copy());
	}

	for (auto &o : orders) {
		OrderByNode node;
		node.type = o.type;
		node.expression = o.expression->Copy();
		new_window->orders.push_back(move(node));
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
	serializer.WriteString(function_name);
	serializer.WriteString(schema);
	serializer.WriteList(children);
	serializer.WriteList(partitions);
	assert(orders.size() <= numeric_limits<uint32_t>::max());
	serializer.Write<uint32_t>((uint32_t)orders.size());
	for (auto &order : orders) {
		serializer.Write<OrderType>(order.type);
		order.expression->Serialize(serializer);
	}
	serializer.Write<WindowBoundary>(start);
	serializer.Write<WindowBoundary>(end);

	serializer.WriteOptional(start_expr);
	serializer.WriteOptional(end_expr);
	serializer.WriteOptional(offset_expr);
	serializer.WriteOptional(default_expr);
}

unique_ptr<ParsedExpression> WindowExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto function_name = source.Read<string>();
	auto schema = source.Read<string>();
	auto expr = make_unique<WindowExpression>(type, schema, function_name);
	source.ReadList<ParsedExpression>(expr->children);
	source.ReadList<ParsedExpression>(expr->partitions);

	auto order_count = source.Read<uint32_t>();
	for (index_t i = 0; i < order_count; i++) {
		auto order_type = source.Read<OrderType>();
		auto expression = ParsedExpression::Deserialize(source);
		expr->orders.push_back(OrderByNode(order_type, move(expression)));
	}
	expr->start = source.Read<WindowBoundary>();
	expr->end = source.Read<WindowBoundary>();

	expr->start_expr = source.ReadOptional<ParsedExpression>();
	expr->end_expr = source.ReadOptional<ParsedExpression>();
	expr->offset_expr = source.ReadOptional<ParsedExpression>();
	expr->default_expr = source.ReadOptional<ParsedExpression>();
	return move(expr);
}
