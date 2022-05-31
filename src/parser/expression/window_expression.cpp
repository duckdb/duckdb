#include "duckdb/parser/expression/window_expression.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

WindowExpression::WindowExpression(ExpressionType type, string schema, const string &function_name)
    : ParsedExpression(type, ExpressionClass::WINDOW), schema(move(schema)),
      function_name(StringUtil::Lower(function_name)), ignore_nulls(false) {
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

string WindowExpression::ToString() const {
	return ToString<WindowExpression, ParsedExpression, OrderByNode>(*this, schema, function_name);
}

bool WindowExpression::Equals(const WindowExpression *a, const WindowExpression *b) {
	// check if the child expressions are equivalent
	if (b->children.size() != a->children.size()) {
		return false;
	}
	if (a->ignore_nulls != b->ignore_nulls) {
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
	// check if the filter clauses are equivalent
	if (!BaseExpression::Equals(a->filter_expr.get(), b->filter_expr.get())) {
		return false;
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

	new_window->filter_expr = filter_expr ? filter_expr->Copy() : nullptr;

	new_window->start = start;
	new_window->end = end;
	new_window->start_expr = start_expr ? start_expr->Copy() : nullptr;
	new_window->end_expr = end_expr ? end_expr->Copy() : nullptr;
	new_window->offset_expr = offset_expr ? offset_expr->Copy() : nullptr;
	new_window->default_expr = default_expr ? default_expr->Copy() : nullptr;
	new_window->ignore_nulls = ignore_nulls;

	return move(new_window);
}

void WindowExpression::Serialize(FieldWriter &writer) const {
	auto &serializer = writer.GetSerializer();

	writer.WriteString(function_name);
	writer.WriteString(schema);
	writer.WriteSerializableList(children);
	writer.WriteSerializableList(partitions);
	// FIXME: should not use serializer here (probably)?
	D_ASSERT(orders.size() <= NumericLimits<uint32_t>::Maximum());
	writer.WriteField<uint32_t>((uint32_t)orders.size());
	for (auto &order : orders) {
		order.Serialize(serializer);
	}
	writer.WriteField<WindowBoundary>(start);
	writer.WriteField<WindowBoundary>(end);

	writer.WriteOptional(start_expr);
	writer.WriteOptional(end_expr);
	writer.WriteOptional(offset_expr);
	writer.WriteOptional(default_expr);
	writer.WriteField<bool>(ignore_nulls);
	writer.WriteOptional(filter_expr);
}

unique_ptr<ParsedExpression> WindowExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto function_name = reader.ReadRequired<string>();
	auto schema = reader.ReadRequired<string>();
	auto expr = make_unique<WindowExpression>(type, schema, function_name);
	expr->children = reader.ReadRequiredSerializableList<ParsedExpression>();
	expr->partitions = reader.ReadRequiredSerializableList<ParsedExpression>();

	auto order_count = reader.ReadRequired<uint32_t>();
	auto &source = reader.GetSource();
	for (idx_t i = 0; i < order_count; i++) {
		expr->orders.push_back(OrderByNode::Deserialize((source)));
	}
	expr->start = reader.ReadRequired<WindowBoundary>();
	expr->end = reader.ReadRequired<WindowBoundary>();

	expr->start_expr = reader.ReadOptional<ParsedExpression>(nullptr);
	expr->end_expr = reader.ReadOptional<ParsedExpression>(nullptr);
	expr->offset_expr = reader.ReadOptional<ParsedExpression>(nullptr);
	expr->default_expr = reader.ReadOptional<ParsedExpression>(nullptr);
	expr->ignore_nulls = reader.ReadRequired<bool>();
	expr->filter_expr = reader.ReadOptional<ParsedExpression>(nullptr);
	return move(expr);
}

} // namespace duckdb
