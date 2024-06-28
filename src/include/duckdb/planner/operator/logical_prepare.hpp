//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_prepare.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class TableCatalogEntry;

class LogicalPrepare : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_PREPARE;

public:
	LogicalPrepare(string name_p, shared_ptr<PreparedStatementData> prepared, unique_ptr<LogicalOperator> logical_plan)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_PREPARE), name(std::move(name_p)),
	      prepared(std::move(prepared)) {
		if (logical_plan) {
			children.push_back(std::move(logical_plan));
		}
	}

	string name;
	shared_ptr<PreparedStatementData> prepared;

public:
	idx_t EstimateCardinality(ClientContext &context) override;
	//! Skips the serialization check in VerifyPlan
	bool SupportSerialization() const override {
		return false;
	}

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType::BOOLEAN);
	}

	bool RequireOptimizer() const override {
		if (!prepared->properties.bound_all_parameters) {
			return false;
		}
		return children[0]->RequireOptimizer();
	}
};
} // namespace duckdb
