#include "planner/operator/logical_join.hpp"

#include "parser/expression/list.hpp"
#include "planner/operator/logical_filter.hpp"

using namespace duckdb;
using namespace std;

vector<string> LogicalJoin::GetNames() {
	auto names = children[0]->GetNames();
	if (type == JoinType::SEMI || type == JoinType::ANTI) {
		// for SEMI and ANTI join we only project the left hand side
		return names;
	}
	if (type == JoinType::MARK) {
		// for SEMI and ANTI join we only project the left hand side
		names.push_back("MARK");
		return names;
	}
	// for other joins we project both sides
	auto right_names = children[1]->GetNames();
	names.insert(names.end(), right_names.begin(), right_names.end());
	return names;
}

void LogicalJoin::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	if (type == JoinType::SEMI || type == JoinType::ANTI) {
		// for SEMI and ANTI join we only project the left hand side
		return;
	}
	if (type == JoinType::MARK) {
		// for MARK join we project the left hand side, plus a BOOLEAN column indicating the MARK
		types.push_back(TypeId::BOOLEAN);
		return;
	}
	// for any other join we project both sides
	types.insert(types.end(), children[1]->types.begin(), children[1]->types.end());
}

string LogicalJoin::ParamsToString() const {
	string result = "";
	if (conditions.size() > 0) {
		result += "[";
		for (size_t i = 0; i < conditions.size(); i++) {
			auto &cond = conditions[i];
			result += ExpressionTypeToString(cond.comparison) + "(" + cond.left->ToString() + ", " +
			          cond.right->ToString() + ")";
			if (i < conditions.size() - 1) {
				result += ", ";
			}
		}
		result += "]";
	}

	return result;
}

size_t LogicalJoin::ExpressionCount() {
	assert(expressions.size() == 0);
	return conditions.size() * 2;
}

Expression *LogicalJoin::GetExpression(size_t index) {
	assert(expressions.size() == 0);
	assert(index < conditions.size() * 2);
	size_t condition = index / 2;
	bool left = index % 2 == 0 ? true : false;
	assert(condition < conditions.size());
	return left ? conditions[condition].left.get() : conditions[condition].right.get();
}

void LogicalJoin::ReplaceExpression(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
                                    size_t index) {
	assert(expressions.size() == 0);
	assert(index < conditions.size() * 2);
	size_t condition = index / 2;
	bool left = index % 2 == 0 ? true : false;
	assert(condition < conditions.size());
	if (left) {
		conditions[condition].left = callback(move(conditions[condition].left));
	} else {
		conditions[condition].right = callback(move(conditions[condition].right));
	}
}
