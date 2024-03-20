//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/index_constraint_type.hpp"
#include "duckdb/common/types/constraint_conflict_info.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

class ClientContext;
class TableIOManager;
class Transaction;
class ConflictManager;

struct IndexLock;
struct IndexScanState;

//! The index is an abstract base class that serves as the basis for indexes
class Index {
public:
	Index(const string &name, const string &index_type, IndexConstraintType index_constraint_type,
	      const vector<column_t> &column_ids, TableIOManager &table_io_manager,
	      const vector<unique_ptr<Expression>> &unbound_expressions, AttachedDatabase &db);
	virtual ~Index() = default;

	//! The name of the index
	string name;
	//! The index type (ART, B+-tree, Skip-List, ...)
	string index_type;
	//! The index constraint type
	IndexConstraintType index_constraint_type;
	//! The logical column ids of the indexed table
	vector<column_t> column_ids;

	//! Associated table io manager
	TableIOManager &table_io_manager;
	//! Unordered set of column_ids used by the index
	unordered_set<column_t> column_id_set;
	//! Unbound expressions used by the index during optimizations
	vector<unique_ptr<Expression>> unbound_expressions;
	//! The physical types stored in the index
	vector<PhysicalType> types;
	//! The logical types of the expressions
	vector<LogicalType> logical_types;

	//! Attached database instance
	AttachedDatabase &db;

public:
	//! Returns true if the index is a unknown index, and false otherwise
	virtual bool IsUnknown() {
		return false;
	}

	//! Obtain a lock on the index
	void InitializeLock(IndexLock &state);
	//! Called when data is appended to the index. The lock obtained from InitializeLock must be held
	virtual ErrorData Append(IndexLock &state, DataChunk &entries, Vector &row_identifiers) = 0;
	//! Obtains a lock and calls Append while holding that lock
	ErrorData Append(DataChunk &entries, Vector &row_identifiers);
	//! Verify that data can be appended to the index without a constraint violation
	virtual void VerifyAppend(DataChunk &chunk) = 0;
	//! Verify that data can be appended to the index without a constraint violation using the conflict manager
	virtual void VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) = 0;
	//! Performs constraint checking for a chunk of input data
	virtual void CheckConstraintsForChunk(DataChunk &input, ConflictManager &conflict_manager) = 0;

	//! Deletes all data from the index. The lock obtained from InitializeLock must be held
	virtual void CommitDrop(IndexLock &index_lock) = 0;
	//! Deletes all data from the index
	void CommitDrop();
	//! Delete a chunk of entries from the index. The lock obtained from InitializeLock must be held
	virtual void Delete(IndexLock &state, DataChunk &entries, Vector &row_identifiers) = 0;
	//! Obtains a lock and calls Delete while holding that lock
	void Delete(DataChunk &entries, Vector &row_identifiers);

	//! Insert a chunk of entries into the index
	virtual ErrorData Insert(IndexLock &lock, DataChunk &input, Vector &row_identifiers) = 0;

	//! Merge another index into this index. The lock obtained from InitializeLock must be held, and the other
	//! index must also be locked during the merge
	virtual bool MergeIndexes(IndexLock &state, Index &other_index) = 0;
	//! Obtains a lock and calls MergeIndexes while holding that lock
	bool MergeIndexes(Index &other_index);

	//! Traverses an ART and vacuums the qualifying nodes. The lock obtained from InitializeLock must be held
	virtual void Vacuum(IndexLock &state) = 0;
	//! Obtains a lock and calls Vacuum while holding that lock
	void Vacuum();

	//! Returns the in-memory usage of the index. The lock obtained from InitializeLock must be held
	virtual idx_t GetInMemorySize(IndexLock &state) = 0;
	//! Returns the in-memory usage of the index
	idx_t GetInMemorySize();

	//! Returns the string representation of an index, or only traverses and verifies the index
	virtual string VerifyAndToString(IndexLock &state, const bool only_verify) = 0;
	//! Obtains a lock and calls VerifyAndToString while holding that lock
	string VerifyAndToString(const bool only_verify);

	//! Returns true if the index is affected by updates on the specified column IDs, and false otherwise
	bool IndexIsUpdated(const vector<PhysicalIndex> &column_ids) const;

	//! Returns unique flag
	bool IsUnique() {
		return (index_constraint_type == IndexConstraintType::UNIQUE ||
		        index_constraint_type == IndexConstraintType::PRIMARY);
	}
	//! Returns primary key flag
	bool IsPrimary() {
		return (index_constraint_type == IndexConstraintType::PRIMARY);
	}
	//! Returns foreign key flag
	bool IsForeign() {
		return (index_constraint_type == IndexConstraintType::FOREIGN);
	}

	//! Returns all index storage information for serialization
	virtual IndexStorageInfo GetStorageInfo(const bool get_buffers);

	//! Execute the index expressions on an input chunk
	void ExecuteExpressions(DataChunk &input, DataChunk &result);
	static string AppendRowError(DataChunk &input, idx_t index);

	//! Throw a constraint violation exception
	virtual string GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index,
	                                             DataChunk &input) = 0;

protected:
	//! Lock used for any changes to the index
	mutex lock;

private:
	//! Bound expressions used during expression execution
	vector<unique_ptr<Expression>> bound_expressions;
	//! Expression executor to execute the index expressions
	ExpressionExecutor executor;

	//! Bind the unbound expressions of the index
	unique_ptr<Expression> BindExpression(unique_ptr<Expression> expr);

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace duckdb
