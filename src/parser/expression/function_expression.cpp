#include "parser/expression/function_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"
#include "common/types/hash.hpp"

using namespace duckdb;
using namespace std;

FunctionExpression::FunctionExpression(string schema, string function_name, vector<unique_ptr<Expression>> &children)
    : Expression(ExpressionType::FUNCTION), schema(schema), function_name(StringUtil::Lower(function_name)) {
	for (auto &child : children) {
		this->children.push_back(move(child));
	}
}

void FunctionExpression::ResolveType() {
	throw Exception("Cannot resolve type of FunctionExpression! Function has to be bound first.");
}

unique_ptr<Expression> FunctionExpression::Copy() {
	vector<unique_ptr<Expression>> copy_children;
	for (auto &child : children) {
		copy_children.push_back(child->Copy());
	}
	auto copy = make_unique<FunctionExpression>(function_name, copy_children);
	copy->schema = schema;
	copy->CopyProperties(*this);
	return copy;
}

uint64_t FunctionExpression::Hash() const {
	uint64_t result = Expression::Hash();
	result = CombineHash(result, duckdb::Hash<const char *>(schema.c_str()));
	result = CombineHash(result, duckdb::Hash<const char *>(function_name.c_str()));
	return result;
}

size_t FunctionExpression::ChildCount() const {
	return children.size();
}

Expression *FunctionExpression::GetChild(size_t index) const {
	assert(index < children.size());
	return children[index].get();
}

void FunctionExpression::ReplaceChild(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
                                      size_t index) {
	assert(index < children.size());
	children[index] = callback(move(children[index]));
}

void FunctionExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	serializer.WriteString(function_name);
	serializer.WriteString(schema);
	serializer.WriteList(children);
}

unique_ptr<Expression> FunctionExpression::Deserialize(ExpressionType type, TypeId return_type, Deserializer &source) {
	vector<unique_ptr<Expression>> children;
	auto function_name = source.Read<string>();
	auto schema = source.Read<string>();
	source.ReadList<Expression>(children);

	auto function = make_unique<FunctionExpression>(function_name, children);
	function->schema = schema;
	return function;
}

bool FunctionExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (FunctionExpression *)other_;
	if (schema != other->schema && function_name != other->function_name) {
		return false;
	}
	if (other->children.size() != children.size()) {
		return false;
	}
	for (size_t i = 0; i < children.size(); i++) {
		if (!children[i]->Equals(other->children[i].get())) {
			return false;
		}
	}
	return true;
}

string FunctionExpression::ToString() const {
	string result = function_name + "(";
	for (size_t i = 0; i < children.size(); i++) {
		result += children[i]->ToString() + (i + 1 == children.size() ? ")" : ",");
	}
	return result;
}
