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
protected:
	Index(const vector<column_t> &column_ids, TableIOManager &table_io_manager, AttachedDatabase &db);

	//! The logical column ids of the indexed table
	vector<column_t> column_ids;
	//! Unordered set of column_ids used by the index
	unordered_set<column_t> column_id_set;

public:
	//! Associated table io manager
	TableIOManager &table_io_manager;
	//! Attached database instance
	AttachedDatabase &db;

public:
	virtual ~Index() = default;

	//! Returns true if the index is a bound index, and false otherwise
	virtual bool IsBound() const = 0;

	//! The index type (ART, B+-tree, Skip-List, ...)
	virtual const string &GetIndexType() const = 0;

	//! The name of the index
	virtual const string &GetIndexName() const = 0;

	//! The index constraint type
	virtual IndexConstraintType GetConstraintType() const = 0;

	//! Returns unique flag
	bool IsUnique() const {
		auto index_constraint_type = GetConstraintType();
		return (index_constraint_type == IndexConstraintType::UNIQUE ||
		        index_constraint_type == IndexConstraintType::PRIMARY);
	}

	//! Returns primary key flag
	bool IsPrimary() const {
		auto index_constraint_type = GetConstraintType();
		return (index_constraint_type == IndexConstraintType::PRIMARY);
	}

	//! Returns foreign key flag
	bool IsForeign() const {
		auto index_constraint_type = GetConstraintType();
		return (index_constraint_type == IndexConstraintType::FOREIGN);
	}

	const vector<column_t> &GetColumnIds() const {
		return column_ids;
	}

	const unordered_set<column_t> &GetColumnIdSet() const {
		return column_id_set;
	}

	// All indexes can be dropped, even if they are unbound
	virtual void CommitDrop() = 0;

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace duckdb
