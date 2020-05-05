#include "duckdb/storage/index.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/table/append_state.hpp"

using namespace duckdb;
using namespace std;

Index::Index(IndexType type, vector<column_t> column_ids,
             vector<unique_ptr<Expression>> unbound_expressions)
    : type(type), column_ids(column_ids), unbound_expressions(move(unbound_expressions)) {
	for (auto &expr : this->unbound_expressions) {
		types.push_back(expr->return_type);
		bound_expressions.push_back(BindExpression(expr->Copy()));
	}
	for (auto &bound_expr : bound_expressions) {
		executor.AddExpression(*bound_expr);
	}
	for (auto column_id : column_ids) {
		column_id_set.insert(column_id);
	}
}

void Index::InitializeLock(IndexLock &state) {
	state.index_lock = unique_lock<mutex>(lock);
}

bool Index::Append(DataChunk &entries, Vector &row_identifiers) {
	IndexLock state;
	InitializeLock(state);
	return Append(state, entries, row_identifiers);
}

void Index::Delete(DataChunk &entries, Vector &row_identifiers) {
	IndexLock state;
	InitializeLock(state);
	Delete(state, entries, row_identifiers);
}

void Index::ExecuteExpressions(DataChunk &input, DataChunk &result) {
	executor.Execute(input, result);
}

unique_ptr<Expression> Index::BindExpression(unique_ptr<Expression> expr) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = (BoundColumnRefExpression &)*expr;
		return make_unique<BoundReferenceExpression>(expr->return_type, column_ids[bound_colref.binding.column_index]);
	}
	ExpressionIterator::EnumerateChildren(*expr,
	                                      [&](unique_ptr<Expression> expr) { return BindExpression(move(expr)); });
	return expr;
}

bool Index::IndexIsUpdated(vector<column_t> &column_ids) {
	for (auto &column : column_ids) {
		if (column_id_set.find(column) != column_id_set.end()) {
			return true;
		}
	}
	return false;
}
