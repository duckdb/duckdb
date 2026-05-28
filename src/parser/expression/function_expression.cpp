#include "duckdb/parser/expression/function_expression.hpp"

#include <utility>
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hash.hpp"

namespace duckdb {

FunctionExpression::FunctionExpression() : ParsedExpression(ExpressionType::FUNCTION, ExpressionClass::FUNCTION) {
}

FunctionExpression::FunctionExpression(string catalog, string schema, const string &function_name,
                                       vector<unique_ptr<ParsedExpression>> children_p,
                                       unique_ptr<ParsedExpression> filter, unique_ptr<OrderModifier> order_bys_p,
                                       bool distinct, bool is_operator, bool export_state_p)
    : ParsedExpression(ExpressionType::FUNCTION, ExpressionClass::FUNCTION), catalog(std::move(catalog)),
      schema(std::move(schema)), function_name(StringUtil::Lower(function_name)), is_operator(is_operator),
      distinct(distinct), filter(std::move(filter)), order_bys(std::move(order_bys_p)), export_state(export_state_p) {
	children.reserve(children_p.size());
	for (auto &child : children_p) {
		children.emplace_back(std::move(child));
	}
	D_ASSERT(!function_name.empty());
	if (!order_bys) {
		order_bys = make_uniq<OrderModifier>();
	}
}

FunctionExpression::FunctionExpression(const string &function_name, vector<unique_ptr<ParsedExpression>> children_p,
                                       unique_ptr<ParsedExpression> filter, unique_ptr<OrderModifier> order_bys,
                                       bool distinct, bool is_operator, bool export_state_p)
    : FunctionExpression(INVALID_CATALOG, INVALID_SCHEMA, function_name, std::move(children_p), std::move(filter),
                         std::move(order_bys), distinct, is_operator, export_state_p) {
}

FunctionExpression::FunctionExpression(string catalog_name, string schema_name, const string &function_name,
                                       vector<FunctionArgument> children, unique_ptr<ParsedExpression> filter,
                                       unique_ptr<OrderModifier> order_bys_p, bool distinct, bool is_operator,
                                       bool export_state)
    : ParsedExpression(ExpressionType::FUNCTION, ExpressionClass::FUNCTION), catalog(std::move(catalog_name)),
      schema(std::move(schema_name)), function_name(StringUtil::Lower(function_name)), is_operator(is_operator),
      children(std::move(children)), distinct(distinct), filter(std::move(filter)), order_bys(std::move(order_bys_p)),
      export_state(export_state) {
	D_ASSERT(!function_name.empty());
	if (!order_bys) {
		this->order_bys = make_uniq<OrderModifier>();
	}
}

FunctionExpression::FunctionExpression(const string &function_name, vector<FunctionArgument> children,
                                       unique_ptr<ParsedExpression> filter, unique_ptr<OrderModifier> order_bys,
                                       bool distinct, bool is_operator, bool export_state)
    : FunctionExpression(INVALID_CATALOG, INVALID_SCHEMA, function_name, std::move(children), std::move(filter),
                         std::move(order_bys), distinct, is_operator, export_state) {
}

string FunctionExpression::ToString() const {
	if (is_operator) {
		// built-in operator
		D_ASSERT(!distinct);
		if (children.size() == 1) {
			if (StringUtil::Contains(function_name, "__postfix")) {
				return "((" + children[0].ToString() + ")" + StringUtil::Replace(function_name, "__postfix", "") + ")";
			}
			return function_name + "(" + children[0].ToString() + ")";
		}
		if (children.size() == 2) {
			return StringUtil::Format("(%s %s %s)", children[0].ToString(), function_name, children[1].ToString());
		}
	}
	// standard function call
	string result;
	if (!catalog.empty()) {
		result += SQLIdentifier(catalog) + ".";
	}
	if (!schema.empty()) {
		result += SQLIdentifier(schema) + ".";
	}
	result += SQLIdentifier(function_name);
	result += "(";
	if (distinct) {
		result += "DISTINCT ";
	}
	result += StringUtil::Join(children, children.size(), ", ",
	                           [&](const FunctionArgument &child) { return child.ToString(); });
	// ordered aggregate
	if (order_bys && !order_bys->orders.empty()) {
		if (children.empty()) {
			result += ") WITHIN GROUP (";
		}
		result += " ORDER BY ";
		for (idx_t i = 0; i < order_bys->orders.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += order_bys->orders[i].ToString();
		}
	}
	result += ")";

	// filtered aggregate
	if (filter) {
		result += " FILTER (WHERE " + filter->ToString() + ")";
	}

	if (export_state) {
		result += " EXPORT_STATE";
	}

	return result;
}

void FunctionExpression::Verify() const {
	D_ASSERT(!function_name.empty());
}

optional_ptr<ParsedExpression> FunctionExpression::IsLambdaFunction() {
	// Ignore the ->> operator (JSON extension).
	if (function_name == "->>") {
		return nullptr;
	}
	// Check the children for lambda expressions.
	for (auto &child : children) {
		if (child.GetExpression().GetExpressionClass() == ExpressionClass::LAMBDA) {
			return *child.GetExpressionMutable();
		}
	}
	return nullptr;
}

hash_t FunctionArgument::Hash() const {
	hash_t result = duckdb::Hash<const char *>(name.c_str());
	if (expression) {
		result = CombineHash(result, expression->Hash());
	}
	return result;
}

} // namespace duckdb
