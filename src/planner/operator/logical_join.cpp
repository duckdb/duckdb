#include "planner/operator/logical_join.hpp"

#include "parser/expression/list.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

LogicalJoin::LogicalJoin(JoinType type, LogicalOperatorType logical_type) : LogicalOperator(logical_type), type(type) {
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

void LogicalJoin::GetTableReferences(LogicalOperator &op, unordered_set<size_t> &bindings) {
	if (op.type == LogicalOperatorType::GET) {
		auto &get = (LogicalGet &)op;
		bindings.insert(get.table_index);
	} else if (op.type == LogicalOperatorType::EMPTY_RESULT) {
		auto &empty = (LogicalEmptyResult &)op;
		for(auto &table : empty.bound_tables) {
			bindings.insert(table.table_index);
		}
	} else if (op.type == LogicalOperatorType::SUBQUERY) {
		auto &subquery = (LogicalSubquery &)op;
		bindings.insert(subquery.table_index);
	} else if (op.type == LogicalOperatorType::TABLE_FUNCTION) {
		auto &table_function = (LogicalTableFunction &)op;
		bindings.insert(table_function.table_index);
	} else if (op.type == LogicalOperatorType::CHUNK_GET) {
		auto &chunk = (LogicalChunkGet &)op;
		bindings.insert(chunk.table_index);
	} else if (op.type == LogicalOperatorType::DELIM_GET) {
		auto &chunk = (LogicalDelimGet &)op;
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
