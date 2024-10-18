//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_pragma.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/parsed_data/bound_pragma_info.hpp"

namespace duckdb {

//! LogicalPragma represents a simple logical operator that only passes on the parse info
class LogicalPragma : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_PRAGMA;

public:
	explicit LogicalPragma(unique_ptr<BoundPragmaInfo> info_p)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_PRAGMA), info(std::move(info_p)) {
	}

	unique_ptr<BoundPragmaInfo> info;

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
