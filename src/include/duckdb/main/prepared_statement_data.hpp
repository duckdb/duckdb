//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/prepared_statement_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {
class CatalogEntry;
class PhysicalOperator;
class SQLStatement;

class PreparedStatementData {
public:
	DUCKDB_API explicit PreparedStatementData(StatementType type);
	DUCKDB_API ~PreparedStatementData();

	StatementType statement_type;
	//! The unbound SQL statement that was prepared
	unique_ptr<SQLStatement> unbound_statement;
	//! The fully prepared physical plan of the prepared statement
	unique_ptr<PhysicalOperator> plan;
	//! The map of parameter index to the actual value entry
	unordered_map<idx_t, vector<unique_ptr<Value>>> value_map;

	//! The result names of the transaction
	vector<string> names;
	//! The result types of the transaction
	vector<LogicalType> types;

	//! The statement properties
	StatementProperties properties;

	//! The catalog version of when the prepared statement was bound
	//! If this version is lower than the current catalog version, we have to rebind the prepared statement
	idx_t catalog_version;

public:
	//! Bind a set of values to the prepared statement data
	DUCKDB_API void Bind(vector<Value> values);
	//! Get the expected SQL Type of the bound parameter
	DUCKDB_API LogicalType GetType(idx_t param_index);
};

} // namespace duckdb