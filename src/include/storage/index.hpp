//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/enums/index_type.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/tuple.hpp"
#include "parser/parsed_expression.hpp"

namespace duckdb {

class ClientContext;
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
	Index(IndexType type) : type(type) {
	}
	virtual ~Index() {
	}

	IndexType type;

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
	virtual void Append(ClientContext &context, DataChunk &entries, uint64_t row_identifier_start) = 0;
	//! Called when data inside the index is updated
	virtual void Update(ClientContext &context, vector<column_t> &column_ids, DataChunk &update_data,
	                    Vector &row_identifiers) = 0;

	// FIXME: what about delete?
};

} // namespace duckdb
