//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_copy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalCopy : public LogicalOperator {
public:
	LogicalCopy(TableCatalogEntry *table, string file_path, bool is_from, char delimiter, char quote, char escape,
	            vector<string> select_list)
	    : LogicalOperator(LogicalOperatorType::COPY), table(table), file_path(file_path), select_list(select_list),
	      is_from(is_from), delimiter(delimiter), quote(quote), escape(escape) {
	}
	LogicalCopy(string file_path, bool is_from, char delimiter, char quote, char escape)
	    : LogicalOperator(LogicalOperatorType::COPY), file_path(file_path), is_from(is_from), delimiter(delimiter),
	      quote(quote), escape(escape) {
	}

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}

	vector<string> GetNames() override {
		return {"Count"};
	}

	TableCatalogEntry *table;

	string file_path;

	vector<string> select_list;

	bool is_from;

	char delimiter = ',';
	char quote = '"';
	char escape = '"';

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::BIGINT);
	}
};
} // namespace duckdb
