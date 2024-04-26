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
public:
	Index(const string &name, const string &index_type, IndexConstraintType index_constraint_type,
	      const vector<column_t> &column_ids, TableIOManager &table_io_manager, AttachedDatabase &db);
	virtual ~Index() = default;

	//! The name of the index
	string name;
	//! The index type (ART, B+-tree, Skip-List, ...)
	string index_type;
	//! The index constraint type
	IndexConstraintType index_constraint_type;

	//! The logical column ids of the indexed table
	vector<column_t> column_ids;
	//! Unordered set of column_ids used by the index
	unordered_set<column_t> column_id_set;

	//! Associated table io manager
	TableIOManager &table_io_manager;
	//! Attached database instance
	AttachedDatabase &db;

	//! Unbound expressions used by the index during optimizations
	// vector<unique_ptr<ParsedExpression>> unbound_expressions;

	//! The physical types stored in the index
	// vector<PhysicalType> types;
	//! The logical types of the expressions
	// vector<LogicalType> logical_types;

public:
	//! Returns true if the index is a unknown index, and false otherwise
	virtual bool IsUnbound() = 0;

	// TODO: Make these const
	bool IsBound() {
		return !IsUnbound();
	}

	//! Returns unique flag
	bool IsUnique() const {
		return (index_constraint_type == IndexConstraintType::UNIQUE ||
		        index_constraint_type == IndexConstraintType::PRIMARY);
	}
	//! Returns primary key flag
	bool IsPrimary() const {
		return (index_constraint_type == IndexConstraintType::PRIMARY);
	}
	//! Returns foreign key flag
	bool IsForeign() const {
		return (index_constraint_type == IndexConstraintType::FOREIGN);
	}

	const string &GetIndexType() const {
		return index_type;
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
