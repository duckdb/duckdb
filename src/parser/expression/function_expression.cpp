
#include "parser/expression/function_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"

#include "catalog/catalog_entry/scalar_function_catalog_entry.hpp"

using namespace duckdb;
using namespace std;

FunctionExpression::FunctionExpression(std::string schema,
                                       std::string function_name,
                                       vector<unique_ptr<Expression>> &children)
    : Expression(ExpressionType::FUNCTION), schema(schema),
      function_name(StringUtil::Lower(function_name)), bound_function(nullptr) {
	for (auto &child : children) {
		AddChild(move(child));
	}
}

void FunctionExpression::ResolveType() {
	Expression::ResolveType();
	vector<TypeId> child_types;
	for (auto &child : children) {
		child_types.push_back(child->return_type);
	}
	if (!bound_function->matches(child_types)) {
		throw CatalogException("Incorrect set of arguments for function \"%s\"",
		                       function_name.c_str());
	}
	return_type = bound_function->return_type(child_types);
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

void FunctionExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	serializer.WriteString(function_name);
	serializer.WriteString(schema);
}

unique_ptr<Expression>
FunctionExpression::Deserialize(ExpressionDeserializeInfo *info,
                                Deserializer &source) {
	auto function_name = source.Read<string>();
	auto function =
	    make_unique<FunctionExpression>(function_name, info->children);
	function->schema = source.Read<string>();
	return function;
}
