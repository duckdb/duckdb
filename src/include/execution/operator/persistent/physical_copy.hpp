//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/persistent/physical_copy.hpp
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
	PhysicalCopy(LogicalOperator &op, TableCatalogEntry *table, string file_path, bool is_from, char delimiter,
	             char quote, char escape, vector<string> select_list)
	    : PhysicalOperator(PhysicalOperatorType::COPY, op.types), table(table), file_path(file_path), is_from(is_from),
	      select_list(select_list), delimiter(delimiter), quote(quote), escape(escape) {
	}

	PhysicalCopy(LogicalOperator &op, string file_path, bool is_from, char delimiter, char quote, char escape)
	    : PhysicalOperator(PhysicalOperatorType::COPY, op.types), table(nullptr), file_path(file_path),
	      is_from(is_from), delimiter(delimiter), quote(quote), escape(escape) {
	}

	void _GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	TableCatalogEntry *table;
	string file_path;
	bool is_from;
	vector<string> select_list;

	char delimiter = ',';
	char quote = '"';
	char escape = '"';
};
} // namespace duckdb
