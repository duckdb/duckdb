//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/unordered_set.hpp"
#include "common/enums/index_type.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/tuple.hpp"
#include "parser/parsed_expression.hpp"
#include "planner/expression.hpp"

namespace duckdb {

class ClientContext;
class DataTable;
class Transaction;

struct IndexScanState {
	vector<column_t> column_ids;

	IndexScanState(vector<column_t> column_ids) : column_ids(column_ids) {
	}
	virtual ~IndexScanState() {
	}
};

//! The index is an abstract base class that serves as the basis for indexes
class Index {
public:
	Index(IndexType type, DataTable &table, vector<column_t> column_ids,
	      vector<unique_ptr<Expression>> unbound_expressions);
	virtual ~Index() = default;

	//! The type of the index
	IndexType type;
	//! The table
	DataTable &table;
	//! Column identifiers to extract from the base table
	vector<column_t> column_ids;
	//! unordered_set of column_ids used by the index
	unordered_set<column_t> column_id_set;
	//! Unbound expressions used by the index
	vector<unique_ptr<Expression>> unbound_expressions;
	//! The types of the expressions
	vector<TypeId> types;

public:
	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table when we only have one query predicate
	virtual unique_ptr<IndexScanState> InitializeScanSinglePredicate(Transaction &transaction,
	                                                                 vector<column_t> column_ids, Value value,
	                                                                 ExpressionType expressionType) = 0;
	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table for two query predicates
	virtual unique_ptr<IndexScanState> InitializeScanTwoPredicates(Transaction &transaction,
	                                                               vector<column_t> column_ids, Value low_value,
	                                                               ExpressionType low_expression_type, Value high_value,
	                                                               ExpressionType high_expression_type) = 0;
	//! Perform a lookup on the index
	virtual void Scan(Transaction &transaction, IndexScanState *ss, DataChunk &result) = 0;

	//! Called when data is appended to the index
	virtual bool Append(DataChunk &entries, Vector &row_identifiers) = 0;

	//! Called when data inside the index is Deleted
	virtual void Delete(DataChunk &entries, Vector &row_identifiers) = 0;

	//! Returns true if the index is affected by updates on the specified column ids, and false otherwise
	bool IndexIsUpdated(vector<column_t> &column_ids);

protected:
	void ExecuteExpressions(DataChunk &input, DataChunk &result);

private:
	//! Bound expressions used by the index
	vector<unique_ptr<Expression>> bound_expressions;

	unique_ptr<Expression> BindExpression(unique_ptr<Expression> expr);
};

} // namespace duckdb
