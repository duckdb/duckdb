//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/catalog_entry/prepared_statement_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry.hpp"
#include "execution/physical_operator.hpp"

#include <string>
#include <unordered_set>

namespace duckdb {

class BoundParameterExpression;
class PhysicalOperator;
class TableCatalogEntry;

//! A view catalog entry
class PreparedStatementCatalogEntry : public CatalogEntry {
public:
	PreparedStatementCatalogEntry(string name, StatementType statement_type)
	    : CatalogEntry(CatalogType::PREPARED_STATEMENT, nullptr, name), statement_type(statement_type) {
	}

	unique_ptr<PhysicalOperator> plan;
	unordered_map<size_t, unique_ptr<Value>> value_map;
	unordered_set<TableCatalogEntry *> tables;

	vector<string> names;
	vector<TypeId> types;
	StatementType statement_type;
};
} // namespace duckdb
