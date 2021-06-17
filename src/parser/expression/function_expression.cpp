#include "duckdb/parser/expression/function_expression.hpp"

#include <utility>
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/types/hash.hpp"

namespace duckdb {

FunctionExpression::FunctionExpression(string schema, const string &function_name,
                                       vector<unique_ptr<ParsedExpression>> children_p,
                                       unique_ptr<ParsedExpression> filter, bool distinct, bool is_operator)
    : ParsedExpression(ExpressionType::FUNCTION, ExpressionClass::FUNCTION), schema(std::move(schema)),
      function_name(StringUtil::Lower(function_name)), is_operator(is_operator), children(move(children_p)),
      distinct(distinct), filter(move(filter)) {
}

FunctionExpression::FunctionExpression(const string &function_name, vector<unique_ptr<ParsedExpression>> children_p,
                                       unique_ptr<ParsedExpression> filter, bool distinct, bool is_operator)
    : FunctionExpression(INVALID_SCHEMA, function_name, move(children_p), move(filter), distinct, is_operator) {
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
	if (!BaseExpression::Equals(a->filter.get(), b->filter.get())) {
		return false;
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
	unique_ptr<ParsedExpression> filter_copy;
	for (auto &child : children) {
		copy_children.push_back(child->Copy());
	}
	if (filter) {
		filter_copy = filter->Copy();
	}
	auto copy =
	    make_unique<FunctionExpression>(function_name, move(copy_children), move(filter_copy), distinct, is_operator);
	copy->schema = schema;
	copy->CopyProperties(*this);
	return move(copy);
}

void FunctionExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	serializer.WriteString(function_name);
	serializer.WriteString(schema);
	serializer.WriteList(children);
	serializer.WriteOptional(filter);
	serializer.Write<bool>(distinct);
	serializer.Write<bool>(is_operator);
}

unique_ptr<ParsedExpression> FunctionExpression::Deserialize(ExpressionType type, Deserializer &source) {
	vector<unique_ptr<ParsedExpression>> children;
	auto function_name = source.Read<string>();
	auto schema = source.Read<string>();
	source.ReadList<ParsedExpression>(children);
	auto filter = source.ReadOptional<ParsedExpression>();
	auto distinct = source.Read<bool>();
	auto is_operator = source.Read<bool>();
	unique_ptr<FunctionExpression> function;
	function = make_unique<FunctionExpression>(function_name, move(children), move(filter), distinct, is_operator);
	function->schema = schema;
	return move(function);
}

} // namespace duckdb
