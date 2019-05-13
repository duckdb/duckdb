#include "planner/expression/bound_function_expression.hpp"

#include "catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "common/types/hash.hpp"

using namespace duckdb;
using namespace std;

BoundFunctionExpression::BoundFunctionExpression(TypeId return_type, ScalarFunctionCatalogEntry *bound_function)
    : Expression(ExpressionType::BOUND_FUNCTION, ExpressionClass::BOUND_FUNCTION, return_type),
      bound_function(bound_function) {
}

bool BoundFunctionExpression::IsFoldable() const {
	// functions with side effects cannot be folded: they have to be executed once for every row
	return bound_function->has_side_effects ? false : Expression::IsFoldable();
}

string BoundFunctionExpression::ToString() const {
	string str = bound_function->name + "(";
	for (index_t i = 0; i < children.size(); i++) {
		if (i > 0) {
			str += ", ";
		}
		str += children[i]->GetName();
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
	for (index_t i = 0; i < children.size(); i++) {
		if (!Expression::Equals(children[i].get(), other->children[i].get())) {
			return false;
		}
	}
	return true;
}

unique_ptr<Expression> BoundFunctionExpression::Copy() {
	auto copy = make_unique<BoundFunctionExpression>(return_type, bound_function);
	for (auto &child : children) {
		copy->children.push_back(child->Copy());
	}
	copy->bind_info = bind_info ? bind_info->Copy() : nullptr;
	copy->CopyProperties(*this);
	return move(copy);
}
