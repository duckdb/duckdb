#include "duckdb/parser/expression/window_expression.hpp"

#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

WindowExpression::WindowExpression(ExpressionType type, vector<unique_ptr<ParsedExpression>> children_p,
                                   unique_ptr<ParsedExpression> offset_expr, unique_ptr<ParsedExpression> default_expr)
    : ParsedExpression(type, ExpressionClass::WINDOW), children(std::move(children_p)) {
	if (offset_expr) {
		children.emplace_back(std::move(offset_expr));
	}
	if (default_expr) {
		children.emplace_back(std::move(default_expr));
	}
}

vector<unique_ptr<ParsedExpression>> WindowExpression::SerializedChildren(Serializer &serializer) const {
	vector<unique_ptr<ParsedExpression>> result;
	idx_t nargs = children.size();
	if (!serializer.ShouldSerialize(8) && (function_name == "lead" || function_name == "lag")) {
		nargs = 1;
	}

	for (idx_t i = 0; i < nargs; ++i) {
		result.emplace_back(children[i]->Copy());
	}

	return result;
}

unique_ptr<ParsedExpression> WindowExpression::SerializedOffset(Serializer &serializer) const {
	if (!serializer.ShouldSerialize(8) && children.size() > 1 && (function_name == "lead" || function_name == "lag")) {
		return children[1]->Copy();
	}

	return nullptr;
}

unique_ptr<ParsedExpression> WindowExpression::SerializedDefault(Serializer &serializer) const {
	if (!serializer.ShouldSerialize(8) && children.size() > 2 && (function_name == "lead" || function_name == "lag")) {
		return children[2]->Copy();
	}

	return nullptr;
}

WindowExpression::WindowExpression(const string &catalog_name, const string &schema, const string &function_name)
    : ParsedExpression(WindowToExpressionType(function_name), ExpressionClass::WINDOW), catalog(catalog_name),
      schema(schema), function_name(StringUtil::Lower(function_name)), ignore_nulls(false), distinct(false) {
}

struct WindowFunctionDefinition {
	const char *name;
	ExpressionType expression_type;
};

static const WindowFunctionDefinition internal_window_functions[] = {
    {"rank", ExpressionType::WINDOW_RANK},
    {"rank_dense", ExpressionType::WINDOW_RANK_DENSE},
    {"dense_rank", ExpressionType::WINDOW_RANK_DENSE},
    {"percent_rank", ExpressionType::WINDOW_PERCENT_RANK},
    {"row_number", ExpressionType::WINDOW_ROW_NUMBER},
    {"first_value", ExpressionType::WINDOW_FIRST_VALUE},
    {"last_value", ExpressionType::WINDOW_LAST_VALUE},
    {"nth_value", ExpressionType::WINDOW_NTH_VALUE},
    {"cume_dist", ExpressionType::WINDOW_CUME_DIST},
    {"lead", ExpressionType::WINDOW_LEAD},
    {"lag", ExpressionType::WINDOW_LAG},
    {"ntile", ExpressionType::WINDOW_NTILE},
    {"fill", ExpressionType::WINDOW_FILL},
    {nullptr, ExpressionType::INVALID}};

ExpressionType WindowExpression::WindowToExpressionType(const string &fun_name) {
	D_ASSERT(StringUtil::IsLower(fun_name));
	auto functions = internal_window_functions;
	for (idx_t i = 0; functions[i].name != nullptr; i++) {
		if (fun_name == functions[i].name) {
			return functions[i].expression_type;
		}
	}
	return ExpressionType::WINDOW_AGGREGATE;
}

void WindowExpression::SetFunctionName(const string &function_name_p) {
	function_name = function_name_p;
	type = WindowToExpressionType(function_name);
}

string WindowExpression::ToString() const {
	return ToString<WindowExpression, ParsedExpression, OrderByNode>(*this, schema, function_name);
}

bool WindowExpression::Equal(const WindowExpression &a, const WindowExpression &b) {
	// check if the child expressions are equivalent
	if (a.has_ignore_nulls != b.has_ignore_nulls) {
		return false;
	}
	if (a.has_ignore_nulls && a.ignore_nulls != b.ignore_nulls) {
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
	if (!ParsedExpression::Equals(a.start_expr, b.start_expr) || !ParsedExpression::Equals(a.end_expr, b.end_expr)) {
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

bool WindowExpression::HasBoundedParts() {
	for (auto &child : children) {
		if ((*child).GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION) {
			return true;
		}
	}
	for (auto &partition : partitions) {
		if ((*partition).GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION) {
			return true;
		}
	}

	for (auto &o : orders) {
		if ((*o.expression).GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION) {
			return true;
		}
	}

	for (auto &o : arg_orders) {
		if ((*o.expression).GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION) {
			return true;
		}
	}
	return false;
}

unique_ptr<ParsedExpression> WindowExpression::Copy() const {
	auto new_window = make_uniq<WindowExpression>(catalog, schema, function_name);
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
	new_window->has_ignore_nulls = has_ignore_nulls;
	new_window->ignore_nulls = ignore_nulls;
	new_window->distinct = distinct;

	return std::move(new_window);
}

} // namespace duckdb
