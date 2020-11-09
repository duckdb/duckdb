#include "duckdb/planner/expression/bound_macro_expression.hpp"

namespace duckdb {

BoundMacroExpression::BoundMacroExpression(LogicalType return_type, string name,
                                           unique_ptr<Expression> bound_expression,
                                           vector<unique_ptr<Expression>> children)
    : Expression(ExpressionType::BOUND_FUNCTION, ExpressionClass::BOUND_MACRO, return_type), name(name),
      expression(move(bound_expression)), children(move(children)) {
}

bool BoundMacroExpression::IsFoldable() const {
	return expression->IsFoldable();
}

string BoundMacroExpression::ToString() const {
	string result = name + "(";
	result += StringUtil::Join(children, children.size(), ", ",
	                           [](const unique_ptr<Expression> &child) { return child->GetName(); });
	result += ")";
	return result;
}

hash_t BoundMacroExpression::Hash() const {
	hash_t result = Expression::Hash();
	return CombineHash(result, duckdb::Hash(name.c_str()));
}

bool BoundMacroExpression::Equals(const BaseExpression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (BoundMacroExpression *)other_;
	if (other->name != name) {
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

unique_ptr<Expression> BoundMacroExpression::Copy() {
	vector<unique_ptr<Expression>> new_children;
	for (auto &child : children) {
		new_children.push_back(child->Copy());
	}

	auto copy = make_unique<BoundMacroExpression>(return_type, name, expression->Copy(), move(new_children));
	copy->CopyProperties(*this);
	return move(copy);
}

} // namespace duckdb
