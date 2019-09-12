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

void Index::AppendToIndexes(vector<unique_ptr<Index>> &indexes, DataChunk &chunk, row_t row_start) {
	if (indexes.size() == 0) {
		return;
	}
	// first generate the vector of row identifiers
	StaticVector<row_t> row_identifiers;
	row_identifiers.sel_vector = chunk.sel_vector;
	row_identifiers.count = chunk.size();
	VectorOperations::GenerateSequence(row_identifiers, row_start);

	index_t failed_index = INVALID_INDEX;
	// now append the entries to the indices
	for (index_t i = 0; i < indexes.size(); i++) {
		if (!indexes[i]->Append(chunk, row_identifiers)) {
			failed_index = i;
			break;
		}
	}
	if (failed_index != INVALID_INDEX) {
		// constraint violation!
		// remove any appended entries from previous indexes (if any)
		for (index_t i = 0; i < failed_index; i++) {
			indexes[i]->Delete(chunk, row_identifiers);
		}
		throw ConstraintException("PRIMARY KEY or UNIQUE constraint violated: duplicated key");
	}
}
