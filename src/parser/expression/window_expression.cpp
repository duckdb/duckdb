#include "duckdb/parser/expression/window_expression.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

WindowExpression::WindowExpression(ExpressionType type, string schema, const string &function_name)
    : ParsedExpression(type, ExpressionClass::WINDOW), schema(move(schema)),
      function_name(StringUtil::Lower(function_name)) {
	switch (type) {
	case ExpressionType::WINDOW_AGGREGATE:
	case ExpressionType::WINDOW_ROW_NUMBER:
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_LAST_VALUE:
	case ExpressionType::WINDOW_NTH_VALUE:
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

string WindowExpression::GetName() const {
	return !alias.empty() ? alias : function_name;
}

string WindowExpression::ToString() const {
	// Start with function call
	string result = function_name + "(";
	result += StringUtil::Join(children, children.size(), ", ",
	                           [](const unique_ptr<ParsedExpression> &child) { return child->ToString(); });
	// Lead/Lag extra arguments
	if (offset_expr.get()) {
		result += ", ";
		result += offset_expr->ToString();
	}
	if (default_expr.get()) {
		result += ", ";
		result += default_expr->ToString();
	}
	// Over clause
	result += ") OVER(";
	string sep;

	// Partitions
	if (!partitions.empty()) {
		result += "PARTITION BY ";
		result += StringUtil::Join(partitions, partitions.size(), ", ",
		                           [](const unique_ptr<ParsedExpression> &partition) { return partition->ToString(); });
		sep = " ";
	}

	// Orders
	if (!orders.empty()) {
		result += sep;
		result += "ORDER BY ";
		result +=
		    StringUtil::Join(orders, orders.size(), ", ", [](const OrderByNode &order) { return order.ToString(); });
		sep = " ";
	}

	// Rows/Range
	string units = "ROWS";
	string from;
	switch (start) {
	case WindowBoundary::CURRENT_ROW_RANGE:
	case WindowBoundary::CURRENT_ROW_ROWS:
		from = "CURRENT ROW";
		units = (start == WindowBoundary::CURRENT_ROW_RANGE) ? "RANGE" : "ROWS";
		break;
	case WindowBoundary::UNBOUNDED_PRECEDING:
		if (end != WindowBoundary::CURRENT_ROW_RANGE) {
			from = "UNBOUNDED PRECEDING";
		}
		break;
	case WindowBoundary::EXPR_PRECEDING_ROWS:
	case WindowBoundary::EXPR_PRECEDING_RANGE:
		from = start_expr->GetName() + " PRECEDING";
		units = (start == WindowBoundary::EXPR_PRECEDING_RANGE) ? "RANGE" : "ROWS";
		break;
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
	case WindowBoundary::EXPR_FOLLOWING_RANGE:
		from = start_expr->GetName() + " FOLLOWING";
		units = (start == WindowBoundary::EXPR_FOLLOWING_RANGE) ? "RANGE" : "ROWS";
		break;
	default:
		break;
	}

	string to;
	switch (end) {
	case WindowBoundary::CURRENT_ROW_RANGE:
		if (start != WindowBoundary::UNBOUNDED_PRECEDING) {
			to = "CURRENT ROW";
			units = "RANGE";
		}
		break;
	case WindowBoundary::CURRENT_ROW_ROWS:
		to = "CURRENT ROW";
		units = "ROWS";
		break;
	case WindowBoundary::UNBOUNDED_PRECEDING:
		to = "UNBOUNDED PRECEDING";
		break;
	case WindowBoundary::EXPR_PRECEDING_ROWS:
	case WindowBoundary::EXPR_PRECEDING_RANGE:
		to = end_expr->GetName() + " PRECEDING";
		units = (start == WindowBoundary::EXPR_PRECEDING_RANGE) ? "RANGE" : "ROWS";
		break;
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
	case WindowBoundary::EXPR_FOLLOWING_RANGE:
		to = end_expr->GetName() + " FOLLOWING";
		units = (start == WindowBoundary::EXPR_FOLLOWING_RANGE) ? "RANGE" : "ROWS";
		break;
	default:
		break;
	}

	if (!from.empty() || !to.empty()) {
		result += sep + units;
	}
	if (!from.empty() && !to.empty()) {
		result += " BETWEEN ";
		result += from;
		result += " AND ";
		result += to;
	} else if (!from.empty()) {
		result += " ";
		result += from;
	} else if (!to.empty()) {
		result += " ";
		result += to;
	}

	result += ")";

	return result;
}

bool WindowExpression::Equals(const WindowExpression *a, const WindowExpression *b) {
	// check if the child expressions are equivalent
	if (b->children.size() != a->children.size()) {
		return false;
	}
	for (idx_t i = 0; i < a->children.size(); i++) {
		if (!a->children[i]->Equals(b->children[i].get())) {
			return false;
		}
	}
	if (a->start != b->start || a->end != b->end) {
		return false;
	}
	// check if the framing expressions are equivalent
	if (!BaseExpression::Equals(a->start_expr.get(), b->start_expr.get()) ||
	    !BaseExpression::Equals(a->end_expr.get(), b->end_expr.get()) ||
	    !BaseExpression::Equals(a->offset_expr.get(), b->offset_expr.get()) ||
	    !BaseExpression::Equals(a->default_expr.get(), b->default_expr.get())) {
		return false;
	}

	// check if the partitions are equivalent
	if (a->partitions.size() != b->partitions.size()) {
		return false;
	}
	for (idx_t i = 0; i < a->partitions.size(); i++) {
		if (!a->partitions[i]->Equals(b->partitions[i].get())) {
			return false;
		}
	}
	// check if the orderings are equivalent
	if (a->orders.size() != b->orders.size()) {
		return false;
	}
	for (idx_t i = 0; i < a->orders.size(); i++) {
		if (a->orders[i].type != b->orders[i].type) {
			return false;
		}
		if (!a->orders[i].expression->Equals(b->orders[i].expression.get())) {
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
		new_window->orders.emplace_back(o.type, o.null_order, o.expression->Copy());
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
	D_ASSERT(orders.size() <= NumericLimits<uint32_t>::Maximum());
	serializer.Write<uint32_t>((uint32_t)orders.size());
	for (auto &order : orders) {
		order.Serialize(serializer);
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
	for (idx_t i = 0; i < order_count; i++) {
		expr->orders.push_back(OrderByNode::Deserialize((source)));
	}
	expr->start = source.Read<WindowBoundary>();
	expr->end = source.Read<WindowBoundary>();

	expr->start_expr = source.ReadOptional<ParsedExpression>();
	expr->end_expr = source.ReadOptional<ParsedExpression>();
	expr->offset_expr = source.ReadOptional<ParsedExpression>();
	expr->default_expr = source.ReadOptional<ParsedExpression>();
	return move(expr);
}

} // namespace duckdb
