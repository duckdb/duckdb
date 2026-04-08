//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_extension_operator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator_extension.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class ColumnBindingResolver;
class ClientContext;
class Deserializer;
class PhysicalOperator;
class PhysicalPlanGenerator;
class Serializer;
struct ColumnBinding;

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

	virtual void ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings);
	virtual string GetExtensionName() const;
};
} // namespace duckdb
