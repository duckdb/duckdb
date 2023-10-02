//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_pragma.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/pragma_function.hpp"
#include "duckdb/parser/parsed_data/pragma_info.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalSimple represents a simple logical operator that only passes on the parse info
class LogicalPragma : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_PRAGMA;

public:
	LogicalPragma(PragmaFunction function_p, PragmaInfo info_p)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_PRAGMA), function(std::move(function_p)),
	      info(std::move(info_p)) {
	}

	//! The pragma function to call
	PragmaFunction function;
	//! The context of the call
	PragmaInfo info;

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
};
} // namespace duckdb
