//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/prepared_statement_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/enums/statement_type.hpp"

namespace duckdb {
class CatalogEntry;
class PhysicalOperator;

struct PreparedValueEntry {
	unique_ptr<Value> value;
	SQLType target_type;
};

class PreparedStatementData {
public:
	PreparedStatementData(StatementType type);
	~PreparedStatementData();

	StatementType statement_type;
	//! The fully prepared physical plan of the prepared statement
	unique_ptr<PhysicalOperator> plan;
	//! The map of parameter index to the actual value entry
	unordered_map<idx_t, PreparedValueEntry> value_map;
	//! Any internal catalog dependencies of the prepared statement
	unordered_set<CatalogEntry *> dependencies;

	//! The result names of the transaction
	vector<string> names;
	//! The (internal) result types of the statement
	vector<TypeId> types;
	//! The result SQL types of the transaction
	vector<SQLType> sql_types;

	//! Whether or not the statement is a read-only statement, or whether it can result in changes to the database
	bool read_only;
	//! Whether or not the statement requires a valid transaction. Almost all statements require this, with the
	//! exception of
	bool requires_valid_transaction;

public:
	//! Bind a set of values to the prepared statement data
	void Bind(vector<Value> values);
	//! Get the expected SQL Type of the bound parameter
	SQLType GetType(idx_t param_index);
};

} // namespace duckdb
