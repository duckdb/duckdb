//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/logical_operator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog.hpp"
#include "common/common.hpp"
#include "common/printable.hpp"
#include "parser/expression.hpp"
#include "parser/statement/select_statement.hpp"
#include "planner/logical_operator_visitor.hpp"

#include <unordered_set>
#include <vector>

namespace duckdb {

//! Returns true if the node is a projection
bool IsProjection(LogicalOperatorType type);
//! Returns the root projection or join node
LogicalOperator *GetProjection(LogicalOperator *);

//! LogicalOperator is the base class of the logical operators present in the
//! logical query tree
class LogicalOperator : public Printable {
public:
	LogicalOperator(LogicalOperatorType type) : type(type) {
	}

	LogicalOperator(LogicalOperatorType type, vector<unique_ptr<Expression>> expressions)
	    : type(type), expressions(std::move(expressions)) {
	}

	LogicalOperatorType GetOperatorType() {
		return type;
	}

	//! Return a vector of the column names that will be returned by this
	//! operator
	virtual vector<string> GetNames() = 0;
	//! Resolve the types of the logical operator and its children
	void ResolveOperatorTypes();

	virtual string ParamsToString() const;
	string ToString() const override;

	virtual void Accept(LogicalOperatorVisitor *) = 0;
	virtual void AcceptChildren(LogicalOperatorVisitor *v) {
		for (auto &child : children) {
			child->Accept(v);
		}
	}

	void AddChild(unique_ptr<LogicalOperator> child) {
		children.push_back(move(child));
	}

	//! The type of the logical operator
	LogicalOperatorType type;
	//! The set of children of the operator
	vector<unique_ptr<LogicalOperator>> children;
	//! The set of expressions contained within the operator, if any
	vector<unique_ptr<Expression>> expressions;
	//! The types returned by this logical operator. Set by calling LogicalOperator::ResolveTypes.
	vector<TypeId> types;

	virtual size_t EstimateCardinality() {
		// simple estimator, just take the max of the children
		size_t max_cardinality = 0;
		for (auto &child : children) {
			max_cardinality = std::max(child->EstimateCardinality(), max_cardinality);
		}
		return max_cardinality;
	}

	virtual size_t ExpressionCount();
	virtual Expression *GetExpression(size_t index);
	virtual void ReplaceExpression(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                               size_t index);

protected:
	//! Resolve types for this specific operator
	virtual void ResolveTypes() = 0;
};
} // namespace duckdb
