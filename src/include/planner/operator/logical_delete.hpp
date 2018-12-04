//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalDelete : public LogicalOperator {
	public:
	LogicalDelete(TableCatalogEntry *table) : LogicalOperator(LogicalOperatorType::DELETE), table(table) {
	}

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}
	std::vector<string> GetNames() override {
		return {"Count"};
	}

	TableCatalogEntry *table;

	protected:
	void ResolveTypes() override {
		types.push_back(TypeId::BIGINT);
	}
};
} // namespace duckdb
