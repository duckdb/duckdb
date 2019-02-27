//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_copy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalCopy : public LogicalOperator {
public:
	LogicalCopy(TableCatalogEntry *table, unique_ptr<CopyInformation> info)
	    : LogicalOperator(LogicalOperatorType::COPY), table(table), info(move(info)) {
	}

	vector<string> GetNames() override {
		return {"Count"};
	}

	TableCatalogEntry *table;
	unique_ptr<CopyInformation> info;
	vector<string> names;

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::BIGINT);
	}
};
} // namespace duckdb
