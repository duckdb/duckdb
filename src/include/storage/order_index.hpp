//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// storage/order_index.hpp
//
// Author: Pedro Holanda & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/tuple.hpp"
#include "common/types/vector.hpp"

#include "parser/expression.hpp"
#include "storage/data_table.hpp"
#include "storage/index.hpp"

namespace duckdb {

struct OrderIndexScanState : public IndexScanState {
	Value value;
	size_t current_index;

	OrderIndexScanState(std::vector<column_t> column_ids,
	                    Expression &expression)
	    : IndexScanState(column_ids, expression) {
	}
};

//! OrderIndex is a simple sorted list index that can be binary searched
class OrderIndex : public Index {
  public:
	OrderIndex(DataTable &table, std::vector<column_t> column_ids,
	           std::vector<TypeId> types, std::vector<TypeId> expression_types,
	           std::vector<std::unique_ptr<Expression>> expressions,
	           size_t initial_capacity);

	//! Appends data into the index, but does not perform the sort yet! This can
	//! be done separately by calling the OrderIndex::Sort() method
	void Insert(DataChunk &data, Vector &row_ids);
	//! Finalizes index creation, sorting the index
	void Sort();
	//! Print the index to the console
	void Print();

	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table
	std::unique_ptr<IndexScanState>
	InitializeScan(Transaction &transaction, std::vector<column_t> column_ids,
	               Expression *expression) override;
	//! Perform a lookup on the index
	void Scan(Transaction &transaction, IndexScanState *ss,
	          DataChunk &result) override;

	// Append entries to the index
	void Append(ClientContext &context, DataChunk &entries,
	            size_t row_identifier_start) override;
	// Update entries in the index
	void Update(ClientContext &context, std::vector<column_t> &column_ids,
	            DataChunk &update_data, Vector &row_identifiers) override;

	//! Lock used for updating the index
	std::mutex lock;
	//! The table
	DataTable &table;
	//! Column identifiers to extract from the base table
	std::vector<column_t> column_ids;
	//! Types of the column identifiers
	std::vector<TypeId> types;
	//! The expressions to evaluate
	std::vector<std::unique_ptr<Expression>> expressions;
	//! The size of one tuple
	size_t tuple_size;
	//! The big sorted list
	std::unique_ptr<uint8_t[]> data;
	//! The amount of entries in the index
	size_t count;
	//! The capacity of the index
	size_t capacity;

  private:
	DataChunk expression_result;

	//! Get the start position in the index for a constant value (point query)
	size_t Search(Value value);
	//! Scan the index starting from the position, updating the position.
	//! Returns the amount of tuples scanned.
	void Scan(size_t &position, Value value, Vector &result_identifiers);
};

} // namespace duckdb
