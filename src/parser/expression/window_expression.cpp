#include "duckdb/parser/expression/window_expression.hpp"

#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

WindowExpression::WindowExpression() : ParsedExpression(ExpressionType::INVALID, ExpressionClass::WINDOW) {
}

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
	if (!serializer.ShouldSerialize(StorageVersion::V2_0_0) && (function_name == "lead" || function_name == "lag")) {
		nargs = 1;
	}

	for (idx_t i = 0; i < nargs; ++i) {
		result.emplace_back(children[i]->Copy());
	}

	return result;
}

unique_ptr<ParsedExpression> WindowExpression::SerializedOffset(Serializer &serializer) const {
	if (!serializer.ShouldSerialize(StorageVersion::V2_0_0) && children.size() > 1 &&
	    (function_name == "lead" || function_name == "lag")) {
		return children[1]->Copy();
	}

	return nullptr;
}

unique_ptr<ParsedExpression> WindowExpression::SerializedDefault(Serializer &serializer) const {
	if (!serializer.ShouldSerialize(StorageVersion::V2_0_0) && children.size() > 2 &&
	    (function_name == "lead" || function_name == "lag")) {
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

string WindowExpression::ExpressionTypeToWindow(ExpressionType expression_type) {
	auto functions = internal_window_functions;
	for (idx_t i = 0; functions[i].name != nullptr; i++) {
		if (expression_type == functions[i].expression_type) {
			return functions[i].name;
		}
	}
	return "";
}

void WindowExpression::SetFunctionName(const string &function_name_p) {
	function_name = Identifier(function_name_p);
	type = WindowToExpressionType(function_name.GetName());
}

string WindowExpression::ToString() const {
	return ToString<WindowExpression, ParsedExpression, OrderByNode>(*this, schema.GetName(), function_name.GetName());
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

} // namespace duckdb
