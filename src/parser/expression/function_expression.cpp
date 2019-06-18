#include "parser/expression/function_expression.hpp"
#include "common/string_util.hpp"
#include "common/exception.hpp"
#include "common/serializer.hpp"
#include "common/types/hash.hpp"

using namespace duckdb;
using namespace std;

FunctionExpression::FunctionExpression(string schema, string function_name,
                                       vector<unique_ptr<ParsedExpression>> &children)
    : ParsedExpression(ExpressionType::FUNCTION, ExpressionClass::FUNCTION), schema(schema),
      function_name(StringUtil::Lower(function_name)) {
	for (auto &child : children) {
		this->children.push_back(move(child));
	}
}

FunctionExpression::FunctionExpression(string function_name, vector<unique_ptr<ParsedExpression>> &children)
    : FunctionExpression(DEFAULT_SCHEMA, function_name, children) {
}

string FunctionExpression::ToString() const {
	string result = function_name + "(";
	for (index_t i = 0; i < children.size(); i++) {
		result += children[i]->ToString() + (i + 1 == children.size() ? ")" : ",");
	}
	return result;
}

bool FunctionExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (FunctionExpression *)other_;
	if (schema != other->schema && function_name != other->function_name) {
		return false;
	}
	if (other->children.size() != children.size()) {
		return false;
	}
	for (index_t i = 0; i < children.size(); i++) {
		if (!children[i]->Equals(other->children[i].get())) {
			return false;
		}
	}
	return true;
}

uint64_t FunctionExpression::Hash() const {
	uint64_t result = ParsedExpression::Hash();
	result = CombineHash(result, duckdb::Hash<const char *>(schema.c_str()));
	result = CombineHash(result, duckdb::Hash<const char *>(function_name.c_str()));
	return result;
}

unique_ptr<ParsedExpression> FunctionExpression::Copy() const {
	vector<unique_ptr<ParsedExpression>> copy_children;
	for (auto &child : children) {
		copy_children.push_back(child->Copy());
	}
	auto copy = make_unique<FunctionExpression>(function_name, copy_children);
	copy->schema = schema;
	copy->CopyProperties(*this);
	return move(copy);
}

void FunctionExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	serializer.WriteString(function_name);
	serializer.WriteString(schema);
	serializer.WriteList(children);
}

unique_ptr<ParsedExpression> FunctionExpression::Deserialize(ExpressionType type, Deserializer &source) {
	vector<unique_ptr<ParsedExpression>> children;
	auto function_name = source.Read<string>();
	auto schema = source.Read<string>();
	source.ReadList<ParsedExpression>(children);

	auto function = make_unique<FunctionExpression>(function_name, children);
	function->schema = schema;
	return move(function);
}
