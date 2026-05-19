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
      children(std::move(children_p)), distinct(distinct), filter(std::move(filter)), order_bys(std::move(order_bys_p)),
      export_state(export_state_p) {
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

string FunctionExpression::ToString() const {
	return ToString<FunctionExpression, ParsedExpression>(*this, catalog, schema, function_name, is_operator, distinct,
	                                                      filter.get(), order_bys.get(), export_state, true);
}

void FunctionExpression::Verify() const {
	D_ASSERT(!function_name.empty());
}

optional_ptr<ParsedExpression> FunctionExpression::IsLambdaFunction() const {
	// Ignore the ->> operator (JSON extension).
	if (function_name == "->>") {
		return nullptr;
	}
	// Check the children for lambda expressions.
	for (auto &child : children) {
		if (child->GetExpressionClass() == ExpressionClass::LAMBDA) {
			return *child;
		}
	}
	return nullptr;
}

} // namespace duckdb
