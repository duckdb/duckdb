#include "planner/operator/logical_join.hpp"

#include "parser/expression/list.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

LogicalJoin::LogicalJoin(JoinType type, LogicalOperatorType logical_type)
    : LogicalOperator(logical_type), type(type) {
}

vector<string> LogicalJoin::GetNames() {
	auto names = children[0]->GetNames();
	if (type == JoinType::SEMI || type == JoinType::ANTI) {
		// for SEMI and ANTI join we only project the left hand side
		return names;
	}
	if (type == JoinType::MARK) {
		// MARK join has an additional MARK attribute
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

void LogicalJoin::GetTableReferences(LogicalOperator &op, unordered_set<size_t> &bindings) {
	if (op.type == LogicalOperatorType::GET) {
		auto &get = (LogicalGet &)op;
		bindings.insert(get.table_index);
	} else if (op.type == LogicalOperatorType::SUBQUERY) {
		auto &subquery = (LogicalSubquery &)op;
		bindings.insert(subquery.table_index);
	} else if (op.type == LogicalOperatorType::TABLE_FUNCTION) {
		auto &table_function = (LogicalTableFunction &)op;
		bindings.insert(table_function.table_index);
	} else if (op.type == LogicalOperatorType::CHUNK_GET) {
		auto &chunk = (LogicalChunkGet &)op;
		bindings.insert(chunk.table_index);
	} else if (op.type == LogicalOperatorType::AGGREGATE_AND_GROUP_BY) {
		auto &aggr = (LogicalAggregate &)op;
		bindings.insert(aggr.aggregate_index);
		bindings.insert(aggr.group_index);
	} else if (op.type == LogicalOperatorType::WINDOW) {
		auto &window = (LogicalWindow &)op;
		bindings.insert(window.window_index);
		// window functions pass through bindings from their children
		for (auto &child : op.children) {
			GetTableReferences(*child, bindings);
		}
	} else if (op.type == LogicalOperatorType::PROJECTION) {
		auto &proj = (LogicalProjection &)op;
		bindings.insert(proj.table_index);
	} else {
		// iterate over the children
		for (auto &child : op.children) {
			GetTableReferences(*child, bindings);
		}
	}
}
