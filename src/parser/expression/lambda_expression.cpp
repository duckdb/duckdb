#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/hash.hpp"

namespace duckdb {

LambdaExpression::LambdaExpression(vector<string> parameters, unique_ptr<ParsedExpression> expression)
    : ParsedExpression(ExpressionType::LAMBDA, ExpressionClass::LAMBDA), parameters(move(parameters)),
      expression(move(expression)) {
}

string LambdaExpression::ToString() const {
	string parameter_str;
	if (parameters.size() == 1) {
		parameter_str = parameters[0];
	} else {
		for (auto &parameter : parameters) {
			if (!parameter_str.empty()) {
				parameter_str += ", ";
			}
			parameter_str += parameter;
		}
		parameter_str = "(" + parameter_str + ")";
	}
	return parameter_str + " -> " + expression->ToString();
}

bool LambdaExpression::Equals(const LambdaExpression *a, const LambdaExpression *b) {
	if (a->parameters != b->parameters) {
		return false;
	}
	if (!a->expression->Equals(b->expression.get())) {
		return false;
	}
	return true;
}

hash_t LambdaExpression::Hash() const {
	hash_t result = ParsedExpression::Hash();
	for (auto &parameter : parameters) {
		result = CombineHash(result, duckdb::Hash<const char *>(parameter.c_str()));
	}
	result = CombineHash(result, expression->Hash());
	return result;
}

unique_ptr<ParsedExpression> LambdaExpression::Copy() const {
	return make_unique<LambdaExpression>(parameters, expression->Copy());
}

void LambdaExpression::Serialize(FieldWriter &writer) const {
	writer.WriteList<string>(parameters);
	writer.WriteSerializable(*expression);
}

unique_ptr<ParsedExpression> LambdaExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto parameters = reader.ReadRequiredList<string>();
	auto expression = reader.ReadRequiredSerializable<ParsedExpression>();
	return make_unique<LambdaExpression>(move(parameters), move(expression));
}

} // namespace duckdb
