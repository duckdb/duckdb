//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operation/physical_copy.hpp
//
// Author: Pedro Holanda
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"
#include <fstream>

namespace duckdb {

//! Physically copy file into a table
class PhysicalCopy : public PhysicalOperator {
  public:
	PhysicalCopy(TableCatalogEntry *table, std::string file_path, bool is_from,
	             char delimiter, char quote, char escape,
	             std::vector<std::string> select_list)
	    : PhysicalOperator(PhysicalOperatorType::COPY), table(table),
	      file_path(file_path), is_from(is_from), delimiter(delimiter),
	      quote(quote), escape(escape), select_list(select_list) {}

	PhysicalCopy(std::string file_path, bool is_from, char delimiter,
	             char quote, char escape)
	    : PhysicalOperator(PhysicalOperatorType::COPY), file_path(file_path),
	      is_from(is_from), delimiter(delimiter), quote(quote), escape(escape),
	      table(nullptr) {}
	std::vector<TypeId> GetTypes() override;
	virtual void _GetChunk(ClientContext &context, DataChunk &chunk,
	                       PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *executor) override;

	TableCatalogEntry *table;
	std::string file_path;
	bool is_from;
	std::vector<std::string> select_list;

	char delimiter = ',';
	char quote = '"';
	char escape = '"';
};
} // namespace duckdb
