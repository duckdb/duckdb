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
#include "common/enums/logical_operator_type.hpp"
#include "planner/expression.hpp"
#include "planner/logical_operator_visitor.hpp"

#include <functional>

namespace duckdb {

//! LogicalOperator is the base class of the logical operators present in the
//! logical query tree
class LogicalOperator {
public:
	LogicalOperator(LogicalOperatorType type) : type(type) {
	}
	LogicalOperator(LogicalOperatorType type, vector<unique_ptr<Expression>> expressions)
	    : type(type), expressions(move(expressions)) {
	}
	virtual ~LogicalOperator() {
	}

	//! The type of the logical operator
	LogicalOperatorType type;
	//! The set of children of the operator
	vector<unique_ptr<LogicalOperator>> children;
	//! The set of expressions contained within the operator, if any
	vector<unique_ptr<Expression>> expressions;
	//! The types returned by this logical operator. Set by calling LogicalOperator::ResolveTypes.
	vector<TypeId> types;

public:
	LogicalOperatorType GetOperatorType() {
		return type;
	}

	//! Resolve the types of the logical operator and its children
	void ResolveOperatorTypes();

	virtual string ParamsToString() const;
	virtual string ToString(index_t depth = 0) const;
	void Print();

	void AddChild(unique_ptr<LogicalOperator> child) {
		children.push_back(move(child));
	}

	virtual index_t EstimateCardinality() {
		// simple estimator, just take the max of the children
		index_t max_cardinality = 0;
		for (auto &child : children) {
			max_cardinality = std::max(child->EstimateCardinality(), max_cardinality);
		}
		return max_cardinality;
	}

protected:
	//! Resolve types for this specific operator
	virtual void ResolveTypes() = 0;
};
} // namespace duckdb
