#include "duckdb/storage/index.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

Index::Index(AttachedDatabase &db, IndexType type, TableIOManager &table_io_manager,
             const vector<column_t> &column_ids_p, const vector<unique_ptr<Expression>> &unbound_expressions,
             IndexConstraintType constraint_type_p, bool track_memory)

    : type(type), table_io_manager(table_io_manager), column_ids(column_ids_p), constraint_type(constraint_type_p),
      db(db), buffer_manager(BufferManager::GetBufferManager(db)), memory_size(0), track_memory(track_memory) {

	for (auto &expr : unbound_expressions) {
		types.push_back(expr->return_type.InternalType());
		logical_types.push_back(expr->return_type);
		auto unbound_expression = expr->Copy();
		bound_expressions.push_back(BindExpression(unbound_expression->Copy()));
		this->unbound_expressions.emplace_back(std::move(unbound_expression));
	}
	for (auto &bound_expr : bound_expressions) {
		executor.AddExpression(*bound_expr);
	}

	// create the column id set
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

bool Index::MergeIndexes(Index *other_index) {

	IndexLock state;
	InitializeLock(state);

	switch (this->type) {
	case IndexType::ART: {
		auto art = (ART *)this;
		return art->MergeIndexes(state, other_index);
	}
	default:
		throw InternalException("Unimplemented index type for merge");
	}
}

void Index::ExecuteExpressions(DataChunk &input, DataChunk &result) {
	executor.Execute(input, result);
}

unique_ptr<Expression> Index::BindExpression(unique_ptr<Expression> expr) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = (BoundColumnRefExpression &)*expr;
		return make_unique<BoundReferenceExpression>(expr->return_type, column_ids[bound_colref.binding.column_index]);
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [this](unique_ptr<Expression> &expr) { expr = BindExpression(std::move(expr)); });
	return expr;
}

bool Index::IndexIsUpdated(const vector<PhysicalIndex> &column_ids) const {
	for (auto &column : column_ids) {
		if (column_id_set.find(column.index) != column_id_set.end()) {
			return true;
		}
	}
	return false;
}

BlockPointer Index::Serialize(duckdb::MetaBlockWriter &writer) {
	throw NotImplementedException("The implementation of this index serialization does not exist.");
}

} // namespace duckdb
