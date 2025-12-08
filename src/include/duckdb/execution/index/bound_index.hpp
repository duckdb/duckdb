//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/bound_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/unbound_index.hpp"
#include "duckdb/common/enums/index_constraint_type.hpp"
#include "duckdb/common/types/constraint_conflict_info.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

class ClientContext;
class TableIOManager;
class Transaction;
class ConflictManager;

struct IndexLock;
struct IndexScanState;

enum class IndexAppendMode : uint8_t { DEFAULT = 0, IGNORE_DUPLICATES = 1, INSERT_DUPLICATES = 2 };

class IndexAppendInfo {
public:
	IndexAppendInfo() : append_mode(IndexAppendMode::DEFAULT), delete_index(nullptr) {};
	IndexAppendInfo(const IndexAppendMode append_mode, const optional_ptr<BoundIndex> delete_index)
	    : append_mode(append_mode), delete_index(delete_index) {};

public:
	IndexAppendMode append_mode;
	optional_ptr<BoundIndex> delete_index;
};

//! The index is an abstract base class that serves as the basis for indexes
class BoundIndex : public Index {
public:
	BoundIndex(const string &name, const string &index_type, IndexConstraintType index_constraint_type,
	           const vector<column_t> &column_ids, TableIOManager &table_io_manager,
	           const vector<unique_ptr<Expression>> &unbound_expressions, AttachedDatabase &db);

	//! The physical types stored in the index
	vector<PhysicalType> types;
	//! The logical types of the expressions
	vector<LogicalType> logical_types;

	//! The name of the index
	string name;
	//! The index type (ART, B+-tree, Skip-List, ...)
	string index_type;
	//! The index constraint type
	IndexConstraintType index_constraint_type;

	//! The vector of unbound expressions, which are later turned into bound expressions.
	//! We need to store the unbound expressions, as we might not always have the context
	//! available to bind directly.
	//! The leaves of these unbound expressions are BoundColumnRefExpressions.
	//! These BoundColumnRefExpressions contain a binding (ColumnBinding),
	//! and that contains a table_index and a column_index.
	//! The table_index is a dummy placeholder.
	//! The column_index indexes the column_ids vector in the Index base class.
	//! Those column_ids store the physical table indexes of the Index,
	//! and we use them when binding the unbound expressions.
	vector<unique_ptr<Expression>> unbound_expressions;

public:
	bool IsBound() const override {
		return true;
	}
	const string &GetIndexType() const override {
		return index_type;
	}
	const string &GetIndexName() const override {
		return name;
	}
	IndexConstraintType GetConstraintType() const override {
		return index_constraint_type;
	}

public:
	//! Obtains a lock on the index.
	void InitializeLock(IndexLock &state);
	//! Appends data to the locked index.
	virtual ErrorData Append(IndexLock &l, DataChunk &chunk, Vector &row_ids) = 0;
	//! Obtains a lock and calls Append while holding that lock.
	ErrorData Append(DataChunk &chunk, Vector &row_ids);
	//! Appends data to the locked index and verifies constraint violations.
	virtual ErrorData Append(IndexLock &l, DataChunk &chunk, Vector &row_ids, IndexAppendInfo &info);
	//! Obtains a lock and calls Append while holding that lock.
	ErrorData Append(DataChunk &chunk, Vector &row_ids, IndexAppendInfo &info);

	//! Verify that data can be appended to the index without a constraint violation.
	virtual void VerifyAppend(DataChunk &chunk, IndexAppendInfo &info, optional_ptr<ConflictManager> manager);
	//! Verifies the constraint for a chunk of data.
	virtual void VerifyConstraint(DataChunk &chunk, IndexAppendInfo &info, ConflictManager &manager);

	//! Deletes all data from the index. The lock obtained from InitializeLock must be held
	virtual void CommitDrop(IndexLock &index_lock) = 0;
	//! Deletes all data from the index
	void CommitDrop() override;
	//! Delete a chunk of entries from the index. The lock obtained from InitializeLock must be held
	virtual void Delete(IndexLock &state, DataChunk &entries, Vector &row_identifiers) = 0;
	//! Obtains a lock and calls Delete while holding that lock
	void Delete(DataChunk &entries, Vector &row_identifiers);

	//! Insert a chunk.
	virtual ErrorData Insert(IndexLock &l, DataChunk &chunk, Vector &row_ids) = 0;
	//! Insert a chunk and verifies constraint violations.
	virtual ErrorData Insert(IndexLock &l, DataChunk &chunk, Vector &row_ids, IndexAppendInfo &info);

	//! Merge another index into this index. The lock obtained from InitializeLock must be held, and the other
	//! index must also be locked during the merge
	virtual bool MergeIndexes(IndexLock &state, BoundIndex &other_index) = 0;
	//! Obtains a lock and calls MergeIndexes while holding that lock
	bool MergeIndexes(BoundIndex &other_index);

	//! Performs a full traversal of the ART while vacuuming the qualifying nodes.
	//! The lock obtained from InitializeLock must be held.
	virtual void Vacuum(IndexLock &l) = 0;
	//! Obtains a lock and calls Vacuum while holding that lock.
	void Vacuum();

	//! Returns the in-memory usage of the index. The lock obtained from InitializeLock must be held
	virtual idx_t GetInMemorySize(IndexLock &state) = 0;
	//! Returns the in-memory usage of the index
	idx_t GetInMemorySize();

	//! Returns the string representation of an index, or only traverses and verifies the index.
	virtual void Verify(IndexLock &l) = 0;
	//! Obtains a lock and calls VerifyAndToString.
	void Verify();

	//! Returns the string representation of an index.
	virtual string ToString(IndexLock &l, bool display_ascii = false) = 0;
	//! Obtains a lock and calls ToString.
	string ToString(bool display_ascii = false);

	//! Ensures that the node allocation counts match the node counts.
	virtual void VerifyAllocations(IndexLock &l) = 0;
	//! Obtains a lock and calls VerifyAllocations.
	void VerifyAllocations();

	//! Verify the index buffers.
	virtual void VerifyBuffers(IndexLock &l);
	//! Obtains a lock and calls VerifyBuffers.
	void VerifyBuffers();

	//! Returns true if the index is affected by updates on the specified column IDs, and false otherwise
	bool IndexIsUpdated(const vector<PhysicalIndex> &column_ids) const;

	//! Serializes index memory to disk and returns the index storage information.
	virtual IndexStorageInfo SerializeToDisk(QueryContext context, const case_insensitive_map_t<Value> &options);
	//! Serializes index memory to the WAL and returns the index storage information.
	virtual IndexStorageInfo SerializeToWAL(const case_insensitive_map_t<Value> &options);

	//! Execute the index expressions on an input chunk
	void ExecuteExpressions(DataChunk &input, DataChunk &result);
	static string AppendRowError(DataChunk &input, idx_t index);

	//! Throw a constraint violation exception
	virtual string GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index,
	                                             DataChunk &input) = 0;

	//! Replay index insert and delete operations buffered during WAL replay.
	//! table_types has the physical types of the table in the order they appear, not logical (no generated columns).
	//! mapped_column_ids contains the sorted order of Indexed physical column ID's (see unbound_index.hpp comments).
	void ApplyBufferedReplays(const vector<LogicalType> &table_types, vector<BufferedIndexData> &buffered_replays,
	                          const vector<StorageIndex> &mapped_column_ids);

protected:
	//! Lock used for any changes to the index
	mutex lock;

	//! The vector of bound expressions to generate the Index keys based on a data chunk.
	//! The leaves of the bound expressions are BoundReferenceExpressions.
	//! These BoundReferenceExpressions contain offsets into the DataChunk to retrieve the columns
	//! for the expression.
	//!	With these offsets into the DataChunk, the expression executor can now evaluate the expression
	//! on incoming data chunks to generate the keys.
	vector<unique_ptr<Expression>> bound_expressions;

private:
	//! Expression executor to execute the index expressions
	ExpressionExecutor executor;

	//! Bind the unbound expressions of the index
	unique_ptr<Expression> BindExpression(unique_ptr<Expression> expr);
};

} // namespace duckdb
