#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/parser/expression_map.hpp"

using namespace duckdb;
using namespace std;


BoundConjunctionExpression::BoundConjunctionExpression(ExpressionType type)
    : Expression(type, ExpressionClass::BOUND_CONJUNCTION, TypeId::BOOLEAN) {

}


BoundConjunctionExpression::BoundConjunctionExpression(ExpressionType type, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : Expression(type, ExpressionClass::BOUND_CONJUNCTION, TypeId::BOOLEAN) {
	children.push_back(move(left));
	children.push_back(move(right));
}



string BoundConjunctionExpression::ToString() const {
	string result = "(" + children[0]->ToString();
	for(index_t i = 1; i < children.size(); i++) {
		result += " " + ExpressionTypeToOperator(type) + " " + children[i]->ToString();
	}
	return result + ")";
}

bool BoundConjunctionExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundConjunctionExpression *)other_;
	// FIXME: duplicate from ConjunctionExpression, move to ExpressionUtil
	// conjunctions are commutative, check if all children have an equivalent expression on the other side

	// we create a map of expression -> count for the left side
	// we keep the count because the same expression can occur multiple times (e.g. "1 AND 1" is legal)
	// in this case we track the following value: map["Constant(1)"] = 2
	expression_map_t<index_t> map;
	for(index_t i = 0; i < children.size(); i++) {
		map[children[i].get()]++;
	}
	// now on the right side we reduce the counts again
	// if the conjunctions are identical, all the counts will be 0 after the
	for(auto &expr : other->children) {
		auto entry = map.find(expr.get());
		// first we check if we can find the expression in the map at all
		if (entry == map.end()) {
			return false;
		}
		// if we found it we check the count; if the count is already 0 we return false
		// this happens if e.g. the left side contains "1 AND X", and the right side contains "1 AND 1"
		// "1" is contained in the map, however, the right side contains the expression twice
		// hence we know the children are not identical in this case because the LHS and RHS have a different count for the Constant(1) expression
		if (entry->second == 0) {
			return false;
		}
		entry->second--;
	}
	return true;
}

unique_ptr<Expression> BoundConjunctionExpression::Copy() {
	auto copy = make_unique<BoundConjunctionExpression>(type);
	for(auto &expr : children) {
		copy->children.push_back(expr->Copy());
	}
	copy->CopyProperties(*this);
	return move(copy);
}
