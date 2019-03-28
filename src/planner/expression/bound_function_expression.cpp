#include "planner/expression/bound_function_expression.hpp"

#include "catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "common/types/hash.hpp"

using namespace duckdb;
using namespace std;

BoundFunctionExpression::BoundFunctionExpression(TypeId return_type,
                                                 ScalarFunctionCatalogEntry *bound_function,
												 SQLType sql_type)
    : Expression(ExpressionType::BOUND_FUNCTION, ExpressionClass::BOUND_FUNCTION, return_type, sql_type),
      bound_function(bound_function) {
}

string BoundFunctionExpression::ToString() const {
	string str = bound_function->name + "(";
	for (size_t i = 0; i < children.size(); i++) {
		if (i > 0) {
			str += ", ";
		}
		str += children[i]->ToString();
	}
	str += ")";
	return str;
}

uint64_t BoundFunctionExpression::Hash() const {
	uint64_t result = Expression::Hash();
	return CombineHash(result, duckdb::Hash(bound_function->name.c_str()));
}

bool BoundFunctionExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundFunctionExpression *)other_;
	if (other->bound_function != bound_function) {
		return false;
	}
	if (children.size() != other->children.size()) {
		return false;
	}
	for (size_t i = 0; i < children.size(); i++) {
		if (!Expression::Equals(children[i].get(), other->children[i].get())) {
			return false;
		}
	}
	return true;
}

unique_ptr<Expression> BoundFunctionExpression::Copy() {
	auto copy = make_unique<BoundFunctionExpression>(return_type, bound_function, sql_type);
	for (auto &child : children) {
		copy->children.push_back(child->Copy());
	}
	copy->CopyProperties(*this);
	return move(copy);
}
