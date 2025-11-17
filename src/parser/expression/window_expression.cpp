#include "duckdb/parser/expression/window_expression.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

WindowExpression::WindowExpression(ExpressionType type) : ParsedExpression(type, ExpressionClass::WINDOW) {
}

WindowExpression::WindowExpression(ExpressionType type, string catalog_name, string schema, const string &function_name)
    : ParsedExpression(type, ExpressionClass::WINDOW), catalog(std::move(catalog_name)), schema(std::move(schema)),
      function_name(StringUtil::Lower(function_name)), ignore_nulls(false), distinct(false) {
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
	case ExpressionType::WINDOW_FILL:
		break;
	default:
		throw NotImplementedException("Window aggregate type %s not supported", ExpressionTypeToString(type).c_str());
	}
}

static const WindowFunctionDefinition internal_window_functions[] = {
    {"rank", ExpressionType::WINDOW_RANK},
    {"rank_dense", ExpressionType::WINDOW_RANK_DENSE},
    {"dense_rank", ExpressionType::WINDOW_RANK_DENSE},
    {"percent_rank", ExpressionType::WINDOW_PERCENT_RANK},
    {"row_number", ExpressionType::WINDOW_ROW_NUMBER},
    {"first_value", ExpressionType::WINDOW_FIRST_VALUE},
    {"first", ExpressionType::WINDOW_FIRST_VALUE},
    {"last_value", ExpressionType::WINDOW_LAST_VALUE},
    {"last", ExpressionType::WINDOW_LAST_VALUE},
    {"nth_value", ExpressionType::WINDOW_NTH_VALUE},
    {"cume_dist", ExpressionType::WINDOW_CUME_DIST},
    {"lead", ExpressionType::WINDOW_LEAD},
    {"lag", ExpressionType::WINDOW_LAG},
    {"ntile", ExpressionType::WINDOW_NTILE},
    {"fill", ExpressionType::WINDOW_FILL},
    {nullptr, ExpressionType::INVALID}};

const WindowFunctionDefinition *WindowExpression::WindowFunctions() {
	return internal_window_functions;
}

ExpressionType WindowExpression::WindowToExpressionType(string &fun_name) {
	D_ASSERT(StringUtil::IsLower(fun_name));
	auto functions = WindowFunctions();
	for (idx_t i = 0; functions[i].name != nullptr; i++) {
		if (fun_name == functions[i].name) {
			return functions[i].expression_type;
		}
	}
	return ExpressionType::WINDOW_AGGREGATE;
}

string WindowExpression::ToString() const {
	return ToString<WindowExpression, ParsedExpression, OrderByNode>(*this, schema, function_name);
}

bool WindowExpression::Equal(const WindowExpression &a, const WindowExpression &b) {
	// check if the child expressions are equivalent
	if (a.ignore_nulls != b.ignore_nulls) {
		return false;
	}
	if (a.distinct != b.distinct) {
		return false;
	}
	if (!ParsedExpression::ListEquals(a.children, b.children)) {
		return false;
	}
	if (a.start != b.start || a.end != b.end) {
		return false;
	}
	if (a.exclude_clause != b.exclude_clause) {
		return false;
	}
	// check if the framing expressions are equivalent
	if (!ParsedExpression::Equals(a.start_expr, b.start_expr) || !ParsedExpression::Equals(a.end_expr, b.end_expr) ||
	    !ParsedExpression::Equals(a.offset_expr, b.offset_expr) ||
	    !ParsedExpression::Equals(a.default_expr, b.default_expr)) {
		return false;
	}

	// check if the argument orderings are equivalent
	if (a.arg_orders.size() != b.arg_orders.size()) {
		return false;
	}
	for (idx_t i = 0; i < a.arg_orders.size(); i++) {
		if (a.arg_orders[i].type != b.arg_orders[i].type) {
			return false;
		}
		if (a.arg_orders[i].null_order != b.arg_orders[i].null_order) {
			return false;
		}
		if (!a.arg_orders[i].expression->Equals(*b.arg_orders[i].expression)) {
			return false;
		}
	}

	// check if the partitions are equivalent
	if (!ParsedExpression::ListEquals(a.partitions, b.partitions)) {
		return false;
	}
	// check if the orderings are equivalent
	if (a.orders.size() != b.orders.size()) {
		return false;
	}
	for (idx_t i = 0; i < a.orders.size(); i++) {
		if (a.orders[i].type != b.orders[i].type) {
			return false;
		}
		if (a.orders[i].null_order != b.orders[i].null_order) {
			return false;
		}
		if (!a.orders[i].expression->Equals(*b.orders[i].expression)) {
			return false;
		}
	}
	// check if the filter clauses are equivalent
	if (!ParsedExpression::Equals(a.filter_expr, b.filter_expr)) {
		return false;
	}

	return true;
}

unique_ptr<ParsedExpression> WindowExpression::Copy() const {
	auto new_window = make_uniq<WindowExpression>(type, catalog, schema, function_name);
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

	for (auto &o : arg_orders) {
		new_window->arg_orders.emplace_back(o.type, o.null_order, o.expression->Copy());
	}

	new_window->filter_expr = filter_expr ? filter_expr->Copy() : nullptr;

	new_window->start = start;
	new_window->end = end;
	new_window->exclude_clause = exclude_clause;
	new_window->start_expr = start_expr ? start_expr->Copy() : nullptr;
	new_window->end_expr = end_expr ? end_expr->Copy() : nullptr;
	new_window->offset_expr = offset_expr ? offset_expr->Copy() : nullptr;
	new_window->default_expr = default_expr ? default_expr->Copy() : nullptr;
	new_window->ignore_nulls = ignore_nulls;
	new_window->distinct = distinct;

	return std::move(new_window);
}

} // namespace duckdb
