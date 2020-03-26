#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

BoundFunctionExpression::BoundFunctionExpression(TypeId return_type, ScalarFunction bound_function, bool is_operator)
    : Expression(ExpressionType::BOUND_FUNCTION, ExpressionClass::BOUND_FUNCTION, return_type),
      function(bound_function), is_operator(is_operator) {
}

bool BoundFunctionExpression::IsFoldable() const {
	// functions with side effects cannot be folded: they have to be executed once for every row
	return function.has_side_effects ? false : Expression::IsFoldable();
}

string BoundFunctionExpression::ToString() const {
	string result = function.name + "(";
	result += StringUtil::Join(children, children.size(), ", ",
	                           [](const unique_ptr<Expression> &child) { return child->GetName(); });
	result += ")";
	return result;
}

hash_t BoundFunctionExpression::Hash() const {
	hash_t result = Expression::Hash();
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
	for (idx_t i = 0; i < children.size(); i++) {
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
	copy->arguments = arguments;
	copy->sql_return_type = sql_return_type;
	return move(copy);
}
