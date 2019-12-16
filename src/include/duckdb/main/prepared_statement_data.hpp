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
	unique_ptr<PhysicalOperator> plan;
	unordered_map<index_t, PreparedValueEntry> value_map;
	unordered_set<CatalogEntry *> dependencies;

	vector<string> names;
	vector<TypeId> types;
	vector<SQLType> sql_types;

public:
	//! Bind a set of values to the prepared statement data
	void Bind(vector<Value> values);
	//! Get the expected SQL Type of the bound parameter
	SQLType GetType(index_t param_index);
};

} // namespace duckdb
