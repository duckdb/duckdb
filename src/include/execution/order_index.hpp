//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/order_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/tuple.hpp"
#include "common/types/vector.hpp"
#include "parser/parsed_expression.hpp"
#include "storage/data_table.hpp"
#include "storage/index.hpp"

namespace duckdb {

struct OrderIndexScanState : public IndexScanState {
	Value value;
	uint64_t current_index;
	uint64_t final_index;

	OrderIndexScanState(vector<column_t> column_ids) : IndexScanState(column_ids) {
	}
};

//! OrderIndex is a simple sorted list index that can be binary searched
class OrderIndex : public Index {
public:
	OrderIndex(DataTable &table, vector<column_t> column_ids, vector<TypeId> types, vector<TypeId> expression_types,
	           vector<unique_ptr<Expression>> expressions, uint64_t initial_capacity);

	//! Appends data into the index, but does not perform the sort yet! This can
	//! be done separately by calling the OrderIndex::Sort() method
	void Insert(DataChunk &data, Vector &row_ids);
	//! Finalizes index creation, sorting the index
	void Sort();
	//! Print the index to the console
	void Print();

	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table for a single predicate
	unique_ptr<IndexScanState> InitializeScanSinglePredicate(Transaction &transaction, vector<column_t> column_ids,
	                                                         Value value, ExpressionType expressionType) override;

	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table for two predicates
	unique_ptr<IndexScanState> InitializeScanTwoPredicates(Transaction &transaction, vector<column_t> column_ids,
	                                                       Value low_value, ExpressionType low_expression_type,
	                                                       Value high_value,
	                                                       ExpressionType high_expression_type) override;
	//! Perform a lookup on the index
	void Scan(Transaction &transaction, IndexScanState *ss, DataChunk &result) override;

	// Append entries to the index
	void Append(ClientContext &context, DataChunk &entries, uint64_t row_identifier_start) override;
	// Update entries in the index
	void Update(ClientContext &context, vector<column_t> &column_ids, DataChunk &update_data,
	            Vector &row_identifiers) override;

	//! Lock used for updating the index
	std::mutex lock;
	//! The table
	DataTable &table;
	//! Column identifiers to extract from the base table
	vector<column_t> column_ids;
	//! Types of the column identifiers
	vector<TypeId> types;
	//! The expressions to evaluate
	vector<unique_ptr<Expression>> expressions;
	//! The size of one tuple
	uint64_t tuple_size;
	//! The big sorted list
	unique_ptr<uint8_t[]> data;
	//! The amount of entries in the index
	uint64_t count;
	//! The capacity of the index
	uint64_t capacity;

private:
	DataChunk expression_result;

	//! Get the start/end position in the index for a Less Than Equal Operator
	uint64_t SearchLTE(Value value);
	//! Get the start/end position in the index for a Greater Than Equal Operator
	uint64_t SearchGTE(Value value);
	//! Get the start/end position in the index for a Less Than Operator
	uint64_t SearchLT(Value value);
	//! Get the start/end position in the index for a Greater Than Operator
	uint64_t SearchGT(Value value);
	//! Scan the index starting from the position, updating the position.
	//! Returns the amount of tuples scanned.
	void Scan(uint64_t &position_from, uint64_t &position_to, Value value, Vector &result_identifiers);
};

} // namespace duckdb
