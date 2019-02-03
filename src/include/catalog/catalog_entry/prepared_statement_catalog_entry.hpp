//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/catalog_entry/prepared_statement_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry.hpp"
#include "common/types/statistics.hpp"
#include "execution/physical_operator.hpp" // FIXME uuugly, perhaps move this header elsewhere
#include "parser/column_definition.hpp"
#include "parser/constraint.hpp"
#include "parser/parsed_data.hpp"

#include <string>

namespace duckdb {

class PhysicalOperator;

//! A view catalog entry
class PreparedStatementCatalogEntry : public CatalogEntry {
public:
	//! Create a real TableCatalogEntry and initialize storage for it
	PreparedStatementCatalogEntry(string name, unique_ptr<PhysicalOperator> plan)
	    : CatalogEntry(CatalogType::PREPARED_STATEMENT, nullptr, name), plan(move(plan)) {
	}

	// TODO use PrepareStatementInformation to hold this stuff

	unique_ptr<PhysicalOperator> plan;
	std::unordered_map<size_t, ParameterExpression *> parameter_expression_map;
	vector<string> names;
	vector<TypeId> types;
};
} // namespace duckdb
