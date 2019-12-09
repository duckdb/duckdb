//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/prepared_statement_data.hpp
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

struct PreparedStatementData {
	PreparedStatementData(StatementType type);
	~PreparedStatementData();

	StatementType statement_type;
	unique_ptr<PhysicalOperator> plan;
	unordered_map<index_t, PreparedValueEntry> value_map;
	unordered_set<CatalogEntry *> dependencies;

	vector<string> names;
	vector<TypeId> types;
	vector<SQLType> sql_types;
};

} // namespace duckdb
