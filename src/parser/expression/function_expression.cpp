#include "parser/expression/function_expression.hpp"
#include "common/string_util.hpp"
#include "common/exception.hpp"
#include "common/serializer.hpp"
#include "common/types/hash.hpp"

using namespace duckdb;
using namespace std;

FunctionExpression::FunctionExpression(string schema, string function_name,
                                       vector<unique_ptr<ParsedExpression>> &children, bool distinct, OperatorType op_type)
    : ParsedExpression(ExpressionType::FUNCTION, ExpressionClass::FUNCTION), schema(schema),
      function_name(StringUtil::Lower(function_name)), op_type(op_type), distinct(distinct) {
	for (auto &child : children) {
		this->children.push_back(move(child));
	}
}

FunctionExpression::FunctionExpression(string function_name, vector<unique_ptr<ParsedExpression>> &children,
                                       bool distinct, OperatorType op_type)
    : FunctionExpression(DEFAULT_SCHEMA, function_name, children, distinct, op_type) {
}

string FunctionExpression::ToString() const {
	if (op_type != OperatorType::NONE) {
		// built-in operator
		if (children.size() == 1) {
			return OperatorTypeToOperator(op_type) + children[0]->ToString();
		} else if (children.size() == 2) {
			return children[0]->ToString() + " " + OperatorTypeToOperator(op_type) + " " + children[1]->ToString();
		}
	}
	// standard function call
	string result = function_name + "(";
	for (index_t i = 0; i < children.size(); i++) {
		if (i != 0) {
			result += ", ";
		}
		result += children[i]->ToString();
	}
	return result + ")";
}

bool FunctionExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (FunctionExpression *)other_;
	if (schema != other->schema || function_name != other->function_name || other->distinct != distinct) {
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
	result = CombineHash(result, duckdb::Hash<bool>(distinct));
	return result;
}

unique_ptr<ParsedExpression> FunctionExpression::Copy() const {
	vector<unique_ptr<ParsedExpression>> copy_children;
	for (auto &child : children) {
		copy_children.push_back(child->Copy());
	}
	auto copy = make_unique<FunctionExpression>(function_name, copy_children, distinct, op_type);
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
	serializer.Write<OperatorType>(op_type);
}

unique_ptr<ParsedExpression> FunctionExpression::Deserialize(ExpressionType type, Deserializer &source) {
	vector<unique_ptr<ParsedExpression>> children;
	auto function_name = source.Read<string>();
	auto schema = source.Read<string>();
	source.ReadList<ParsedExpression>(children);
	auto distinct = source.Read<bool>();
	auto op_type = source.Read<OperatorType>();

	auto function = make_unique<FunctionExpression>(function_name, children, distinct, op_type);
	function->schema = schema;
	return move(function);
}
