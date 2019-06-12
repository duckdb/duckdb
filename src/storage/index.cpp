#include "storage/index.hpp"
#include "execution/expression_executor.hpp"
#include "planner/expression_iterator.hpp"
#include "planner/expression/bound_columnref_expression.hpp"
#include "planner/expression/bound_reference_expression.hpp"

using namespace duckdb;
using namespace std;

Index::Index(IndexType type, DataTable &table, vector<column_t> column_ids,
             vector<unique_ptr<Expression>> unbound_expressions)
    : type(type), table(table), column_ids(column_ids), unbound_expressions(move(unbound_expressions)) {
	for (auto &expr : this->unbound_expressions) {
		types.push_back(expr->return_type);
		bound_expressions.push_back(BindExpression(expr->Copy()));
	}
	for (auto column_id : column_ids) {
		column_id_set.insert(column_id);
	}
}

void Index::ExecuteExpressions(DataChunk &input, DataChunk &result) {
	result.Reset();
	ExpressionExecutor executor(input);
	executor.Execute(bound_expressions, result);
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
