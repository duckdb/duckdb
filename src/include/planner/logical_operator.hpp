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

	LogicalOperator(LogicalOperatorType type, std::vector<std::unique_ptr<Expression>> expressions)
	    : type(type), expressions(std::move(expressions)) {
	}

	LogicalOperatorType GetOperatorType() {
		return type;
	}

	//! Return a vector of the column names that will be returned by this
	//! operator
	virtual std::vector<std::string> GetNames() = 0;
	//! Resolve the types of the logical operator and its children
	void ResolveOperatorTypes() {
		if (types.size() > 0) {
			// types already resolved for this node
			return;
		}
		// first resolve child types
		for (auto &child : children) {
			child->ResolveOperatorTypes();
		}
		// now resolve the types for this operator
		ResolveTypes();
	}

	virtual std::string ParamsToString() const;
	std::string ToString() const override;

	virtual void Accept(LogicalOperatorVisitor *) = 0;
	virtual void AcceptChildren(LogicalOperatorVisitor *v) {
		for (auto &child : children) {
			child->Accept(v);
		}
	}

	void AddChild(std::unique_ptr<LogicalOperator> child) {
		referenced_tables.insert(child->referenced_tables.begin(), child->referenced_tables.end());
		children.push_back(move(child));
	}

	//! The type of the logical operator
	LogicalOperatorType type;
	//! The set of tables that is accessible from this operator
	std::unordered_set<size_t> referenced_tables;
	//! The set of children of the operator
	std::vector<std::unique_ptr<LogicalOperator>> children;
	//! The set of expressions contained within the operator, if any
	std::vector<std::unique_ptr<Expression>> expressions;
	//! The types returned by this logical operator. Set by calling LogicalOperator::ResolveTypes.
	std::vector<TypeId> types;

	virtual size_t ExpressionCount();
	virtual Expression *GetExpression(size_t index);
	virtual void SetExpression(size_t index, std::unique_ptr<Expression> expr);

	protected:
	//! Resolve types for this specific operator
	virtual void ResolveTypes() = 0;
};
} // namespace duckdb
