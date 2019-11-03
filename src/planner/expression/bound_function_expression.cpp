#include "planner/expression/bound_function_expression.hpp"

#include "catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "common/types/hash.hpp"
#include "planner/expression_iterator.hpp"

using namespace duckdb;
using namespace std;

BoundFunctionExpression::BoundFunctionExpression(TypeId return_type, ScalarFunction bound_function, bool is_operator)
    : Expression(ExpressionType::BOUND_FUNCTION, ExpressionClass::BOUND_FUNCTION, return_type),
      function(bound_function), is_operator(is_operator) {
}

bool BoundFunctionExpression::IsFoldable() const {
	// functions with side effects cannot be folded: they have to be executed once for every row
    if(function.has_side_effects) return false;

    bool is_foldable = true;
    ExpressionIterator::EnumerateChildren(*this, [&](const Expression &child) {
        //TODO: Function applications that involve `Interval`s are not supported yet.
        if (!child.IsFoldable() || child.return_type == TypeId::INTERVAL) {
            is_foldable = false;
        }
    });
    return is_foldable;
}

string BoundFunctionExpression::ToString() const {
	string str = function.name + "(";
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
	return CombineHash(result, duckdb::Hash(function.name.c_str()));
}

bool BoundFunctionExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundFunctionExpression *)other_;
	if (other->function != function) {
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
	auto copy = make_unique<BoundFunctionExpression>(return_type, function, is_operator);
	for (auto &child : children) {
		copy->children.push_back(child->Copy());
	}
	copy->bind_info = bind_info ? bind_info->Copy() : nullptr;
	copy->CopyProperties(*this);
	return move(copy);
}
