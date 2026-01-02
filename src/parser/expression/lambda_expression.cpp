#include "duckdb/parser/expression/lambda_expression.hpp"

#include "duckdb/common/types/hash.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

LambdaExpression::LambdaExpression() : ParsedExpression(ExpressionType::LAMBDA, ExpressionClass::LAMBDA) {
}

LambdaExpression::LambdaExpression(vector<string> named_parameters_p, unique_ptr<ParsedExpression> expr)
    : ParsedExpression(ExpressionType::LAMBDA, ExpressionClass::LAMBDA), syntax_type(LambdaSyntaxType::LAMBDA_KEYWORD),
      expr(std::move(expr)) {
	if (named_parameters_p.size() == 1) {
		lhs = make_uniq<ColumnRefExpression>(named_parameters_p.back());
		return;
	}
	// Create a dummy row function and insert the children.
	vector<unique_ptr<ParsedExpression>> children;
	for (const auto &name : named_parameters_p) {
		auto child = make_uniq<ColumnRefExpression>(name);
		children.push_back(std::move(child));
	}
	lhs = make_uniq<FunctionExpression>("row", std::move(children));
}

LambdaExpression::LambdaExpression(unique_ptr<ParsedExpression> lhs, unique_ptr<ParsedExpression> expr)
    : ParsedExpression(ExpressionType::LAMBDA, ExpressionClass::LAMBDA), syntax_type(LambdaSyntaxType::SINGLE_ARROW),
      lhs(std::move(lhs)), expr(std::move(expr)) {
}

vector<reference<const ParsedExpression>> LambdaExpression::ExtractColumnRefExpressions(string &error_message) const {
	// we return an error message because we can't throw a binder exception here,
	// since we can't distinguish between a lambda function and the JSON operator yet
	vector<reference<const ParsedExpression>> column_refs;

	if (lhs->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		// single column reference
		column_refs.emplace_back(*lhs);
		return column_refs;
	}

	if (lhs->GetExpressionClass() == ExpressionClass::FUNCTION) {
		// list of column references
		auto &func_expr = lhs->Cast<FunctionExpression>();
		if (func_expr.function_name != "row") {
			error_message = InvalidParametersErrorMessage();
			return column_refs;
		}

		for (auto &child : func_expr.children) {
			if (child->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
				error_message = InvalidParametersErrorMessage();
				return column_refs;
			}
			column_refs.emplace_back(*child);
		}
	}

	if (column_refs.empty()) {
		error_message = InvalidParametersErrorMessage();
	}
	return column_refs;
}

string LambdaExpression::InvalidParametersErrorMessage() {
	// FIXME: remove this once we only support the new lambda syntax.
	return "Invalid lambda parameters! Parameters must be unqualified comma-separated names like x or (x, y).";
}

bool LambdaExpression::IsLambdaParameter(const vector<unordered_set<string>> &lambda_params,
                                         const string &parameter_name) {
	for (const auto &level : lambda_params) {
		if (level.find(parameter_name) != level.end()) {
			return true;
		}
	}
	return false;
}

string LambdaExpression::ToString() const {
	if (syntax_type != LambdaSyntaxType::LAMBDA_KEYWORD) {
		return "(" + lhs->ToString() + " -> " + expr->ToString() + ")";
	}

	string str = "";
	auto column_refs = ExtractColumnRefExpressions(str);

	str = "(lambda ";
	for (idx_t i = 0; i < column_refs.size(); i++) {
		auto &column_ref = column_refs[i].get();
		auto &cast_column_ref = column_ref.Cast<ColumnRefExpression>();

		if (i == column_refs.size() - 1) {
			str += cast_column_ref.ToString();
			continue;
		}
		str += cast_column_ref.ToString() + ", ";
	}
	return str + ": " + expr->ToString() + ")";
}

bool LambdaExpression::Equal(const LambdaExpression &a, const LambdaExpression &b) {
	return a.lhs->Equals(*b.lhs) && a.expr->Equals(*b.expr);
}

hash_t LambdaExpression::Hash() const {
	hash_t result = lhs->Hash();
	ParsedExpression::Hash();
	result = CombineHash(result, expr->Hash());
	return result;
}

unique_ptr<ParsedExpression> LambdaExpression::Copy() const {
	auto copy = make_uniq<LambdaExpression>(lhs->Copy(), expr->Copy());
	copy->syntax_type = syntax_type;
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb
