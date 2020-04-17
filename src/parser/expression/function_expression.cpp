#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/types/hash.hpp"

using namespace duckdb;
using namespace std;

FunctionExpression::FunctionExpression(string schema, string function_name,
                                       vector<unique_ptr<ParsedExpression>> &children, bool distinct, bool is_operator)
    : ParsedExpression(ExpressionType::FUNCTION, ExpressionClass::FUNCTION), schema(schema),
      function_name(StringUtil::Lower(function_name)), is_operator(is_operator), distinct(distinct) {
	for (auto &child : children) {
		this->children.push_back(move(child));
	}
}

FunctionExpression::FunctionExpression(string function_name, vector<unique_ptr<ParsedExpression>> &children,
                                       bool distinct, bool is_operator)
    : FunctionExpression(DEFAULT_SCHEMA, function_name, children, distinct, is_operator) {
}

string FunctionExpression::ToString() const {
	if (is_operator) {
		// built-in operator
		if (children.size() == 1) {
			return function_name + children[0]->ToString();
		} else if (children.size() == 2) {
			return children[0]->ToString() + " " + function_name + " " + children[1]->ToString();
		}
	}
	// standard function call
	string result = function_name + "(";
	result += StringUtil::Join(children, children.size(), ", ",
	                           [](const unique_ptr<ParsedExpression> &child) { return child->ToString(); });
	return result + ")";
}

bool FunctionExpression::Equals(const FunctionExpression *a, const FunctionExpression *b) {
	if (a->schema != b->schema || a->function_name != b->function_name || b->distinct != a->distinct) {
		return false;
	}
	if (b->children.size() != a->children.size()) {
		return false;
	}
	for (idx_t i = 0; i < a->children.size(); i++) {
		if (!a->children[i]->Equals(b->children[i].get())) {
			return false;
		}
	}
	return true;
}

hash_t FunctionExpression::Hash() const {
	hash_t result = ParsedExpression::Hash();
	result = CombineHash(result, duckdb::Hash<const char *>(schema.c_str()));
	result = CombineHash(result, duckdb::Hash<const char *>(function_name.c_str()));
	result = CombineHash(result, duckdb::Hash<bool>(distinct));
	return result;
}

unique_ptr<ParsedExpression> FunctionExpression::Copy() const {
	vector<unique_ptr<ParsedExpression>> copy_children;
	for (auto &child : children) {
		copy_children.push_back(child->Copy());
	}
	auto copy = make_unique<FunctionExpression>(function_name, copy_children, distinct, is_operator);
	copy->schema = schema;
	copy->CopyProperties(*this);
	return move(copy);
}

void FunctionExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	serializer.WriteString(function_name);
	serializer.WriteString(schema);
	serializer.WriteList(children);
	serializer.Write<bool>(distinct);
	serializer.Write<bool>(is_operator);
}

unique_ptr<ParsedExpression> FunctionExpression::Deserialize(ExpressionType type, Deserializer &source) {
	vector<unique_ptr<ParsedExpression>> children;
	auto function_name = source.Read<string>();
	auto schema = source.Read<string>();
	source.ReadList<ParsedExpression>(children);
	auto distinct = source.Read<bool>();
	auto is_operator = source.Read<bool>();

	auto function = make_unique<FunctionExpression>(function_name, children, distinct, is_operator);
	function->schema = schema;
	return move(function);
}
