//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_limit_percent.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalLimitPercent represents a LIMIT PERCENT clause
class LogicalLimitPercent : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_LIMIT_PERCENT;

public:
	LogicalLimitPercent(double limit_percent, int64_t offset_val, unique_ptr<Expression> limit,
	                    unique_ptr<Expression> offset)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_LIMIT_PERCENT), limit_percent(limit_percent),
	      offset_val(offset_val), limit(std::move(limit)), offset(std::move(offset)) {
	}

	//! Limit percent and offset values in case they are constants, used in optimizations.
	double limit_percent;
	int64_t offset_val;
	//! The maximum amount of elements to emit
	unique_ptr<Expression> limit;
	//! The offset from the start to begin emitting elements
	unique_ptr<Expression> offset;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
