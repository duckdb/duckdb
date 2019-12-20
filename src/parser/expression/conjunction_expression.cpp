#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/parser/expression_map.hpp"

using namespace duckdb;
using namespace std;

ConjunctionExpression::ConjunctionExpression(ExpressionType type) :
	ParsedExpression(type, ExpressionClass::CONJUNCTION) {

}

ConjunctionExpression::ConjunctionExpression(ExpressionType type, vector<unique_ptr<ParsedExpression>> children) :
	ParsedExpression(type, ExpressionClass::CONJUNCTION) {
	for(auto &child : children) {
		AddExpression(move(child));
	}

}

ConjunctionExpression::ConjunctionExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
                                             unique_ptr<ParsedExpression> right)
    : ParsedExpression(type, ExpressionClass::CONJUNCTION) {
	AddExpression(move(left));
	AddExpression(move(right));
}

void ConjunctionExpression::AddExpression(unique_ptr<ParsedExpression> expr) {
	if (expr->type == type) {
		// expr is a conjunction of the same type: merge the expression lists together
		auto &other = (ConjunctionExpression&) *expr;
		for(auto &child : other.children) {
			children.push_back(move(child));
		}
	} else {
		children.push_back(move(expr));
	}
}

string ConjunctionExpression::ToString() const {
	string result = children[0]->ToString();
	for(index_t i = 1; i < children.size(); i++) {
		result += " " + ExpressionTypeToOperator(type) + " " + children[i]->ToString();
	}
	return result;
}

bool ConjunctionExpression::Equals(const ConjunctionExpression *a, const ConjunctionExpression *b) {
	if (a->children.size() != b->children.size()) {
		return false;
	}
	// conjunctions are commutative, check if all children have an equivalent expression on the other side

	// we create a map of expression -> count for the left side
	// we keep the count because the same expression can occur multiple times (e.g. "1 AND 1" is legal)
	// in this case we track the following value: map["Constant(1)"] = 2
	expression_map_t<index_t> map;
	for(index_t i = 0; i < a->children.size(); i++) {
		map[a->children[i].get()]++;
	}
	// now on the right side we reduce the counts again
	// if the conjunctions are identical, all the counts will be 0 after the
	for(auto &expr : b->children) {
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

unique_ptr<ParsedExpression> ConjunctionExpression::Copy() const {
	auto copy = make_unique<ConjunctionExpression>(type);
	for(auto &expr : children) {
		copy->children.push_back(expr->Copy());
	}
	copy->CopyProperties(*this);
	return move(copy);
}

void ConjunctionExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	serializer.WriteList<ParsedExpression>(children);
}

unique_ptr<ParsedExpression> ConjunctionExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto result = make_unique<ConjunctionExpression>(type);
	source.ReadList<ParsedExpression>(result->children);
	return move(result);
}
