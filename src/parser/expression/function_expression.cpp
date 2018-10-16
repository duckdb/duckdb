
#include "parser/expression/function_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

FunctionExpression::FunctionExpression(std::string func_name,
                                       vector<unique_ptr<Expression>> &children)
    : Expression(ExpressionType::FUNCTION),
      func_name(StringUtil::Lower(func_name)) {
	for (auto &child : children) {
		AddChild(move(child));
	}
}

void FunctionExpression::ResolveType() {
	Expression::ResolveType();
	if (func_name == "abs") {
		return_type = children[0]->return_type;
	}
}

unique_ptr<Expression> FunctionExpression::Copy() {
	vector<unique_ptr<Expression>> copy_children;
	for (auto &child : children) {
		copy_children.push_back(child->Copy());
	}
	auto copy = make_unique<FunctionExpression>(func_name, copy_children);
	copy->CopyProperties(*this);
	return copy;
}

void FunctionExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	serializer.WriteString(func_name);
}

unique_ptr<Expression>
FunctionExpression::Deserialize(ExpressionDeserializeInformation *info,
                                Deserializer &source) {
	bool failed = false;
	auto func_name = source.Read<string>(failed);
	if (failed) {
		return nullptr;
	}
	return make_unique<FunctionExpression>(func_name, info->children);
}
