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
	explicit LogicalExtensionOperator(vector<unique_ptr<Expression>> expressions)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR, std::move(expressions)) {
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	virtual PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) = 0;

	//! Opt into standard child/expression traversal with exact output type verification
	virtual bool SupportsTypeVerification() const {
		return false;
	}

	virtual void ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings);
	virtual string GetExtensionName() const;
};
} // namespace duckdb
