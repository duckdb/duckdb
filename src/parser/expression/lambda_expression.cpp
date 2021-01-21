#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/types/hash.hpp"

namespace duckdb {

LambdaExpression::LambdaExpression(const string &capture_name, unique_ptr<ParsedExpression> expression) :
	ParsedExpression(ExpressionType::LAMBDA, ExpressionClass::LAMBDA), capture_name(capture_name), expression(move(expression)) {

}


string LambdaExpression::ToString() const {
	return capture_name + " -> " + expression->ToString();
}

bool LambdaExpression::Equals(const LambdaExpression *a, const LambdaExpression *b) {
	if (a->capture_name != b->capture_name) {
		return false;
	}
	if (!a->expression->Equals(b->expression.get())) {
		return false;
	}
	return true;
}

hash_t LambdaExpression::Hash() const {
	hash_t result = ParsedExpression::Hash();
	result = CombineHash(result, duckdb::Hash<const char *>(capture_name.c_str()));
	result = CombineHash(result, expression->Hash());
	return result;
}

unique_ptr<ParsedExpression> LambdaExpression::Copy() const {
	return make_unique<LambdaExpression>(capture_name, expression->Copy());
}

void LambdaExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	serializer.WriteString(capture_name);
	expression->Serialize(serializer);
}

unique_ptr<ParsedExpression> LambdaExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto capture_name = source.Read<string>();
	auto expression = ParsedExpression::Deserialize(source);

	return make_unique<LambdaExpression>(move(capture_name), move(expression));
}

}
