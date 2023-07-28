//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_extension_operator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator_extension.hpp"

namespace duckdb {

struct LogicalExtensionOperator : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;

public:
	LogicalExtensionOperator() : LogicalOperator(LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
	}
	LogicalExtensionOperator(vector<unique_ptr<Expression>> expressions)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR, std::move(expressions)) {
	}

	static unique_ptr<LogicalExtensionOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);

	virtual void FormatSerialize(FormatSerializer &serializer) const override;
	static unique_ptr<LogicalOperator> FormatDeserialize(FormatDeserializer &deserializer);

	virtual unique_ptr<PhysicalOperator> CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) = 0;

	virtual string GetExtensionName() const;
};
} // namespace duckdb
