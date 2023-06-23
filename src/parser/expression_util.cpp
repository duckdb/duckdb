#include "duckdb/parser/expression_util.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/expression_map.hpp"

namespace duckdb {

template <class T>
bool ExpressionUtil::ExpressionListEquals(const vector<unique_ptr<T>> &a, const vector<unique_ptr<T>> &b) {
	if (a.size() != b.size()) {
		return false;
	}
	for (idx_t i = 0; i < a.size(); i++) {
		if (!(*a[i] == *b[i])) {
			return false;
		}
	}
	return true;
}

template <class T, class EXPRESSION_MAP>
bool ExpressionUtil::ExpressionSetEquals(const vector<unique_ptr<T>> &a, const vector<unique_ptr<T>> &b) {
	if (a.size() != b.size()) {
		return false;
	}
	// we create a map of expression -> count for the left side
	// we keep the count because the same expression can occur multiple times (e.g. "1 AND 1" is legal)
	// in this case we track the following value: map["Constant(1)"] = 2
	EXPRESSION_MAP map;
	for (idx_t i = 0; i < a.size(); i++) {
		map[*a[i]]++;
	}
	// now on the right side we reduce the counts again
	// if the conjunctions are identical, all the counts will be 0 after the
	for (auto &expr : b) {
		auto entry = map.find(*expr);
		// first we check if we can find the expression in the map at all
		if (entry == map.end()) {
			return false;
		}
		// if we found it we check the count; if the count is already 0 we return false
		// this happens if e.g. the left side contains "1 AND X", and the right side contains "1 AND 1"
		// "1" is contained in the map, however, the right side contains the expression twice
		// hence we know the children are not identical in this case because the LHS and RHS have a different count for
		// the Constant(1) expression
		if (entry->second == 0) {
			return false;
		}
		entry->second--;
	}
	return true;
}

bool ExpressionUtil::ListEquals(const vector<unique_ptr<ParsedExpression>> &a,
                                const vector<unique_ptr<ParsedExpression>> &b) {
	return ExpressionListEquals<ParsedExpression>(a, b);
}

bool ExpressionUtil::ListEquals(const vector<unique_ptr<Expression>> &a, const vector<unique_ptr<Expression>> &b) {
	return ExpressionListEquals<Expression>(a, b);
}

bool ExpressionUtil::SetEquals(const vector<unique_ptr<ParsedExpression>> &a,
                               const vector<unique_ptr<ParsedExpression>> &b) {
	return ExpressionSetEquals<ParsedExpression, parsed_expression_map_t<idx_t>>(a, b);
}

bool ExpressionUtil::SetEquals(const vector<unique_ptr<Expression>> &a, const vector<unique_ptr<Expression>> &b) {
	return ExpressionSetEquals<Expression, expression_map_t<idx_t>>(a, b);
}

} // namespace duckdb
