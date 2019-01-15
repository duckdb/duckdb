#include "parser/expression/bound_function_expression.hpp"

#include "catalog/catalog_entry/schema_catalog_entry.hpp"
#include "common/exception.hpp"
#include "common/serializer.hpp"
#include "common/types/hash.hpp"

using namespace duckdb;
using namespace std;

BoundFunctionExpression::BoundFunctionExpression(unique_ptr<FunctionExpression> function,
                                                 ScalarFunctionCatalogEntry *bound_function)
    : Expression(ExpressionType::BOUND_FUNCTION, function->return_type), function(move(function)),
      bound_function(bound_function) {
	this->alias = this->function->alias;
}

void BoundFunctionExpression::ResolveType() {
	Expression::ResolveType();
	vector<TypeId> child_types;
	for (auto &child : function->children) {
		child_types.push_back(child->return_type);
	}
	if (!bound_function->matches(child_types)) {
		throw CatalogException("Incorrect set of arguments for function \"%s\"", bound_function->name.c_str());
	}
	return_type = bound_function->return_type(child_types);
}

unique_ptr<Expression> BoundFunctionExpression::Copy() {
	throw SerializationException("Cannot serialize Bound Function");
}

void BoundFunctionExpression::Serialize(Serializer &serializer) {
	throw SerializationException("Cannot serialize a BoundFunctionExpression");
}

uint64_t BoundFunctionExpression::Hash() const {
	uint64_t result = Expression::Hash();
	return CombineHash(result, function->Hash());
}

bool BoundFunctionExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (BoundFunctionExpression *)other_;
	if (other->bound_function != bound_function) {
		return false;
	}
	return other->function->Equals(function.get());
}
