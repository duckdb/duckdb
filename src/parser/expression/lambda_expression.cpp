#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

LambdaExpression::LambdaExpression(vector<unique_ptr<ParsedExpression>> lhs, unique_ptr<ParsedExpression> rhs)
    : ParsedExpression(ExpressionType::LAMBDA, ExpressionClass::LAMBDA), lhs(move(lhs)), rhs(move(rhs)) {
}

string LambdaExpression::ToString() const {

	vector<string> lhs_strings;
	for (auto &expr : lhs) {
		lhs_strings.push_back(expr->ToString());
	}

	auto lhs_string = StringUtil::Join(lhs_strings, ", ");
	if (lhs_strings.size() > 1) {
		lhs_string = "(" + lhs_string + ")";
	}

	return lhs_string + " -> " + rhs->ToString();
}

bool LambdaExpression::Equals(const LambdaExpression *a, const LambdaExpression *b) {

	if (a->lhs.size() != b->lhs.size()) {
		return false;
	}

	for (idx_t i = 0; i < a->lhs.size(); i++) {
		if (!a->lhs[i]->Equals(b->lhs[i].get())) {
			return false;
		}
	}

	return a->rhs->Equals(b->rhs.get());
}

hash_t LambdaExpression::Hash() const {

	hash_t result = ParsedExpression::Hash();
	for (auto &expr : lhs) {
		result = CombineHash(result, expr->Hash());
	}
	result = CombineHash(result, rhs->Hash());
	return result;
}

unique_ptr<ParsedExpression> LambdaExpression::Copy() const {

	auto result = make_unique<LambdaExpression>(vector<unique_ptr<ParsedExpression>>(), rhs->Copy());
	for (auto &expr : lhs) {
		result->lhs.push_back(expr->Copy());
	}
	return move(result);
}

void LambdaExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializableList(lhs);
	writer.WriteSerializable(*rhs);
}

unique_ptr<ParsedExpression> LambdaExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto lhs = reader.ReadRequiredSerializableList<ParsedExpression>();
	auto rhs = reader.ReadRequiredSerializable<ParsedExpression>();
	return make_unique<LambdaExpression>(move(lhs), move(rhs));
}

} // namespace duckdb
