//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_prepare.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/prepared_statement_catalog_entry.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class TableCatalogEntry;

class LogicalPrepare : public LogicalOperator {
public:
	LogicalPrepare(string name, StatementType statement_type, vector<string> names, vector<SQLType> sql_types,
	               unordered_map<index_t, PreparedValueEntry> value_map, unique_ptr<LogicalOperator> logical_plan)
	    : LogicalOperator(LogicalOperatorType::PREPARE), name(name), statement_type(statement_type), names(names),
	      sql_types(sql_types), value_map(move(value_map)) {
		children.push_back(move(logical_plan));
	}

	string name;
	StatementType statement_type;
	vector<string> names;
	vector<SQLType> sql_types;
	unordered_map<index_t, PreparedValueEntry> value_map;

	void GetTableBindings(unordered_set<TableCatalogEntry *> &result_list);

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::BOOLEAN);
	}
};
} // namespace duckdb
