//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// planner/operator/logical_copy.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalCopy : public LogicalOperator {
  public:
	LogicalCopy(TableCatalogEntry *table, std::string file_path, bool is_from,
	            char delimiter, char quote, char escape,
	            std::vector<std::string> select_list)
	    : LogicalOperator(LogicalOperatorType::COPY), table(table),
	      file_path(file_path), select_list(select_list), is_from(is_from),
	      delimiter(delimiter), quote(quote), escape(escape) {
	}
	LogicalCopy(std::string file_path, bool is_from, char delimiter, char quote,
	            char escape)
	    : LogicalOperator(LogicalOperatorType::COPY), file_path(file_path),
	      is_from(is_from), delimiter(delimiter), quote(quote), escape(escape) {
	}

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}

	TableCatalogEntry *table;

	std::string file_path;

	std::vector<std::string> select_list;

	bool is_from;

	char delimiter = ',';
	char quote = '"';
	char escape = '"';
};
} // namespace duckdb
