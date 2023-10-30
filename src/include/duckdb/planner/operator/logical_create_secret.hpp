//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_create_secret.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/create_secret_function.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalSimple represents a simple logical operator that only passes on the parse info
class LogicalCreateSecret : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CREATE_SECRET;

public:
	LogicalCreateSecret(CreateSecretFunction function_p, CreateSecretInfo info_p)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_SECRET), function(std::move(function_p)),
	      info(std::move(info_p)) {
	}

	//! The pragma function to call
	CreateSecretFunction function;
	//! The context of the call
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
