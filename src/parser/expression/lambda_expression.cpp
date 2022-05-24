#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

LambdaExpression::LambdaExpression(vector<unique_ptr<ParsedExpression>> params, unique_ptr<ParsedExpression> expr)
    : ParsedExpression(ExpressionType::LAMBDA, ExpressionClass::LAMBDA), params(move(params)), expr(move(expr)) {
}

string LambdaExpression::ToString() const {

	vector<string> params_strings;
	for (auto &param : params) {
		params_strings.push_back(param->ToString());
	}

	auto params_string = StringUtil::Join(params_strings, ", ");
	if (params_strings.size() > 1) {
		params_string = "(" + params_string + ")";
	}

	return params_string + " -> " + expr->ToString();
}

bool LambdaExpression::Equals(const LambdaExpression *a, const LambdaExpression *b) {

	if (a->params.size() != b->params.size()) {
		return false;
	}

	for (idx_t i = 0; i < a->params.size(); i++) {
		if (!a->params[i]->Equals(b->params[i].get())) {
			return false;
		}
	}

	return a->expr->Equals(b->expr.get());
}

hash_t LambdaExpression::Hash() const {

	hash_t result = ParsedExpression::Hash();
	for (auto &param : params) {
		result = CombineHash(result, param->Hash());
	}
	result = CombineHash(result, expr->Hash());
	return result;
}

unique_ptr<ParsedExpression> LambdaExpression::Copy() const {

	auto result = make_unique<LambdaExpression>(vector<unique_ptr<ParsedExpression>>(), expr->Copy());
	for (auto &param : params) {
		result->params.push_back(param->Copy());
	}
	return move(result);
}

void LambdaExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializableList(params);
	writer.WriteSerializable(*expr);
}

unique_ptr<ParsedExpression> LambdaExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto params = reader.ReadRequiredSerializableList<ParsedExpression>();
	auto expr = reader.ReadRequiredSerializable<ParsedExpression>();
	return make_unique<LambdaExpression>(move(params), move(expr));
}

} // namespace duckdb
