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

class ColumnBindingResolver;

struct LogicalExtensionOperator : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;

public:
	LogicalExtensionOperator() : LogicalOperator(LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
	}
	LogicalExtensionOperator(vector<unique_ptr<Expression>> expressions)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR, std::move(expressions)) {
	}

	virtual void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	virtual unique_ptr<PhysicalOperator> CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) = 0;

	virtual void ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings);
	virtual string GetExtensionName() const;
};
} // namespace duckdb
