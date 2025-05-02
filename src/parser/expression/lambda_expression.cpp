#include "duckdb/parser/expression/lambda_expression.hpp"

#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

LambdaExpression::LambdaExpression() : ParsedExpression(ExpressionType::LAMBDA, ExpressionClass::LAMBDA) {
}

LambdaExpression::LambdaExpression(vector<string> named_parameters_p, unique_ptr<ParsedExpression> expr)
    : ParsedExpression(ExpressionType::LAMBDA, ExpressionClass::LAMBDA),
      named_parameters(std::move(named_parameters_p)), expr(std::move(expr)) {
	if (named_parameters.size() == 1) {
		lhs = make_uniq<ColumnRefExpression>(named_parameters.back());
		return;
	}
	// Create a dummy row function and insert the children.
	vector<unique_ptr<ParsedExpression>> children;
	for (const auto &name : named_parameters) {
		auto child = make_uniq<ColumnRefExpression>(name);
		children.push_back(std::move(child));
	}
	lhs = make_uniq<FunctionExpression>("row", std::move(children));
}

LambdaExpression::LambdaExpression(unique_ptr<ParsedExpression> lhs, unique_ptr<ParsedExpression> expr)
    : ParsedExpression(ExpressionType::LAMBDA, ExpressionClass::LAMBDA), lhs(std::move(lhs)), expr(std::move(expr)) {
}

vector<reference<ParsedExpression>> LambdaExpression::ExtractColumnRefExpressions(string &error_message) {

	// we return an error message because we can't throw a binder exception here,
	// since we can't distinguish between a lambda function and the JSON operator yet
	vector<reference<ParsedExpression>> column_refs;

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
	if (named_parameters.empty()) {
		return "(" + lhs->ToString() + " -> " + expr->ToString() + ")";
	}
	string lhs_str = "(lambda ";
	for (idx_t i = 0; i < named_parameters.size(); i++) {
		if (i == named_parameters.size() - 1) {
			lhs_str += "\"" + named_parameters[i] + "\"";
			continue;
		}
		lhs_str += "\"" + named_parameters[i] + "\"" + ", ";
	}

	return lhs_str + ": " + expr->ToString() + ")";
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
	if (named_parameters.empty()) {
		auto copy = make_uniq<LambdaExpression>(lhs->Copy(), expr->Copy());
		copy->CopyProperties(*this);
		return std::move(copy);
	}

	auto copy = make_uniq<LambdaExpression>(named_parameters, expr->Copy());
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb
