//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_copy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/copy_info.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalCopy : public LogicalOperator {
public:
	LogicalCopy(TableCatalogEntry *table, unique_ptr<CopyInfo> info)
	    : LogicalOperator(LogicalOperatorType::COPY), table(table), info(move(info)) {
	}

	TableCatalogEntry *table;
	unique_ptr<CopyInfo> info;
	vector<string> names;

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::BIGINT);
	}
};
} // namespace duckdb
