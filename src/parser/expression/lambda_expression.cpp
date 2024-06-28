#include "duckdb/parser/expression/lambda_expression.hpp"

#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

LambdaExpression::LambdaExpression() : ParsedExpression(ExpressionType::LAMBDA, ExpressionClass::LAMBDA) {
}

LambdaExpression::LambdaExpression(unique_ptr<ParsedExpression> lhs, unique_ptr<ParsedExpression> expr)
    : ParsedExpression(ExpressionType::LAMBDA, ExpressionClass::LAMBDA), lhs(std::move(lhs)), expr(std::move(expr)) {
}

vector<reference<ParsedExpression>> LambdaExpression::ExtractColumnRefExpressions(string &error_message) {

	// we return an error message because we can't throw a binder exception here,
	// since we can't distinguish between a lambda function and the JSON operator yet
	vector<reference<ParsedExpression>> column_refs;

	if (lhs->expression_class == ExpressionClass::COLUMN_REF) {
		// single column reference
		column_refs.emplace_back(*lhs);
		return column_refs;
	}

	if (lhs->expression_class == ExpressionClass::FUNCTION) {
		// list of column references
		auto &func_expr = lhs->Cast<FunctionExpression>();
		if (func_expr.function_name != "row") {
			error_message = InvalidParametersErrorMessage();
			return column_refs;
		}

		for (auto &child : func_expr.children) {
			if (child->expression_class != ExpressionClass::COLUMN_REF) {
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
	return "(" + lhs->ToString() + " -> " + expr->ToString() + ")";
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
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb
