//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// storage/index.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "common/types/tuple.hpp"

#include "parser/expression.hpp"

namespace duckdb {

class ClientContext;
class Transaction;

struct IndexScanState {
	std::vector<column_t> column_ids;
	Expression &expression;

	IndexScanState(std::vector<column_t> column_ids, Expression &expression)
	    : column_ids(column_ids), expression(expression) {
	}
	virtual ~IndexScanState() {
	}
};

//! The index is an abstract base class that serves as the basis for indexes
class Index {
  public:
	Index(IndexType type) : type(type) {
	}
	virtual ~Index() {
	}

	IndexType type;

	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table
	virtual std::unique_ptr<IndexScanState>
	InitializeScan(Transaction &transaction, std::vector<column_t> column_ids,
	               Expression *expression,ExpressionType expressionType) = 0;
	//! Perform a lookup on the index
	virtual void Scan(Transaction &transaction, IndexScanState *ss,
	                  DataChunk &result) = 0;

	//! Called when data is appended to the index
	virtual void Append(ClientContext &context, DataChunk &entries,
	                    size_t row_identifier_start) = 0;
	//! Called when data inside the index is updated
	virtual void Update(ClientContext &context,
	                    std::vector<column_t> &column_ids,
	                    DataChunk &update_data, Vector &row_identifiers) = 0;

	// FIXME: what about delete?
};

} // namespace duckdb
