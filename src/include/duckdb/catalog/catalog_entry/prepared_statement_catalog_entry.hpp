//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/catalog_entry/prepared_statement_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class BoundParameterExpression;
class PhysicalOperator;
class TableCatalogEntry;

struct PreparedValueEntry {
	unique_ptr<Value> value;
	SQLType target_type;
};

//! A view catalog entry
class PreparedStatementCatalogEntry : public CatalogEntry {
public:
	PreparedStatementCatalogEntry(string name, StatementType statement_type)
	    : CatalogEntry(CatalogType::PREPARED_STATEMENT, nullptr, name), statement_type(statement_type) {
	}

	unique_ptr<PhysicalOperator> plan;
	unordered_map<index_t, PreparedValueEntry> value_map;
	unordered_set<TableCatalogEntry *> tables;

	vector<string> names;
	vector<TypeId> types;
	vector<SQLType> sql_types;
	StatementType statement_type;
};
} // namespace duckdb
