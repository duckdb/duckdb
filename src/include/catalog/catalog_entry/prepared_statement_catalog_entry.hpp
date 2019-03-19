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
	//! Create a real TableCatalogEntry and initialize storage for it
	PreparedStatementCatalogEntry(string name, StatementType statement_type)
	    : CatalogEntry(CatalogType::PREPARED_STATEMENT, nullptr, name), statement_type(statement_type) {
	}

	unique_ptr<PhysicalOperator> plan;
	unordered_map<size_t, BoundParameterExpression *> parameter_expression_map;
	unordered_set<TableCatalogEntry *> tables;

	vector<string> names;
	vector<TypeId> types;
	StatementType statement_type;
};
} // namespace duckdb
