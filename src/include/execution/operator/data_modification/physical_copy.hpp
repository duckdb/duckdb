//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/data_modification/physical_copy.hpp
//
//
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
	      file_path(file_path), is_from(is_from), select_list(select_list),
	      delimiter(delimiter), quote(quote), escape(escape) {
	}

	PhysicalCopy(std::string file_path, bool is_from, char delimiter,
	             char quote, char escape)
	    : PhysicalOperator(PhysicalOperatorType::COPY), table(nullptr),
	      file_path(file_path), is_from(is_from), delimiter(delimiter),
	      quote(quote), escape(escape) {
	}

	std::vector<TypeId> GetTypes() override;

	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

	std::unique_ptr<PhysicalOperatorState>
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
