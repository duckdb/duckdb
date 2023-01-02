//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/enums/index_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/meta_block_writer.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

class ClientContext;
class TableIOManager;
class Transaction;

struct IndexLock;

//! The index is an abstract base class that serves as the basis for indexes
class Index {
public:
	Index(AttachedDatabase &db, IndexType type, TableIOManager &table_io_manager, const vector<column_t> &column_ids,
	      const vector<unique_ptr<Expression>> &unbound_expressions, IndexConstraintType constraint_type,
	      bool track_memory);
	virtual ~Index() = default;

	//! The type of the index
	IndexType type;
	//! Associated table io manager
	TableIOManager &table_io_manager;
	//! Column identifiers to extract from the base table
	vector<column_t> column_ids;
	//! Unordered_set of column_ids used by the index
	unordered_set<column_t> column_id_set;
	//! Unbound expressions used by the index
	vector<unique_ptr<Expression>> unbound_expressions;
	//! The physical types stored in the index
	vector<PhysicalType> types;
	//! The logical types of the expressions
	vector<LogicalType> logical_types;
	//! Index constraint type (primary key, foreign key, ...)
	IndexConstraintType constraint_type;

	//! Attached database instance
	AttachedDatabase &db;
	//! Buffer manager of the database instance
	BufferManager &buffer_manager;
	//! The size of the index in memory
	//! This does not track the size of the index meta information, but only allocated nodes and leaves
	idx_t memory_size;
	//! Flag determining if this index's size is tracked by the buffer manager
	bool track_memory;

public:
	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table when we only have one query predicate
	virtual unique_ptr<IndexScanState> InitializeScanSinglePredicate(Transaction &transaction, Value value,
	                                                                 ExpressionType expressionType) = 0;
	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table for two query predicates
	virtual unique_ptr<IndexScanState> InitializeScanTwoPredicates(Transaction &transaction, Value low_value,
	                                                               ExpressionType low_expression_type, Value high_value,
	                                                               ExpressionType high_expression_type) = 0;
	//! Perform a lookup on the index, fetching up to max_count result ids. Returns true if all row ids were fetched,
	//! and false otherwise.
	virtual bool Scan(Transaction &transaction, DataTable &table, IndexScanState &state, idx_t max_count,
	                  vector<row_t> &result_ids) = 0;

	//! Obtain a lock on the index
	virtual void InitializeLock(IndexLock &state);
	//! Called when data is appended to the index. The lock obtained from InitializeAppend must be held
	virtual bool Append(IndexLock &state, DataChunk &entries, Vector &row_identifiers) = 0;
	bool Append(DataChunk &entries, Vector &row_identifiers);
	//! Verify that data can be appended to the index
	virtual void VerifyAppend(DataChunk &chunk) = 0;
	//! Verify that data can be appended to the index for foreign key constraint
	virtual void VerifyAppendForeignKey(DataChunk &chunk, string *err_msg_ptr) = 0;
	//! Verify that data can be delete from the index for foreign key constraint
	virtual void VerifyDeleteForeignKey(DataChunk &chunk, string *err_msg_ptr) = 0;

	//! Called when data inside the index is Deleted
	virtual void Delete(IndexLock &state, DataChunk &entries, Vector &row_identifiers) = 0;
	void Delete(DataChunk &entries, Vector &row_identifiers);

	//! Insert data into the index. Does not lock the index.
	virtual bool Insert(IndexLock &lock, DataChunk &input, Vector &row_identifiers) = 0;

	//! Merge other_index into this index.
	virtual bool MergeIndexes(IndexLock &state, Index *other_index) = 0;
	bool MergeIndexes(Index *other_index);

	//! Returns the string representation of an index
	virtual string ToString() = 0;

	//! Returns true if the index is affected by updates on the specified column ids, and false otherwise
	bool IndexIsUpdated(const vector<PhysicalIndex> &column_ids) const;

	//! Returns unique flag
	bool IsUnique() {
		return (constraint_type == IndexConstraintType::UNIQUE || constraint_type == IndexConstraintType::PRIMARY);
	}
	//! Returns primary flag
	bool IsPrimary() {
		return (constraint_type == IndexConstraintType::PRIMARY);
	}
	//! Returns foreign flag
	bool IsForeign() {
		return (constraint_type == IndexConstraintType::FOREIGN);
	}
	//! Serializes the index and returns the pair of block_id offset positions
	virtual BlockPointer Serialize(duckdb::MetaBlockWriter &writer);

	//! Returns block/offset of where index was most recently serialized.
	BlockPointer GetSerializedDataPointer() const {
		return serialized_data_pointer;
	}

protected:
	void ExecuteExpressions(DataChunk &input, DataChunk &result);

	//! Lock used for updating the index
	mutex lock;

	//! Pointer to most recently checkpointed index data.
	BlockPointer serialized_data_pointer;

private:
	//! Bound expressions used by the index
	vector<unique_ptr<Expression>> bound_expressions;
	//! Expression executor for the index expressions
	ExpressionExecutor executor;

	unique_ptr<Expression> BindExpression(unique_ptr<Expression> expr);
};

} // namespace duckdb
