//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_create_secret.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalCreateSecret represents a simple logical operator that only passes on the parse info
class LogicalCreateSecret : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CREATE_SECRET;

public:
	explicit LogicalCreateSecret(CreateSecretInfo info_p)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_SECRET), info(std::move(info_p)) {
	}

	CreateSecretInfo info;

public:
	idx_t EstimateCardinality(ClientContext &context) override {
		return 1;
	};

	//! Skips the serialization check in VerifyPlan
	bool SupportSerialization() const override {
		return false;
	}

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType::BOOLEAN);
	}
};
} // namespace duckdb
