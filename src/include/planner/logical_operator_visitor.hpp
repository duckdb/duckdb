//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/logical_operator_visitor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_node_visitor.hpp"

namespace duckdb {

class LogicalAggregate;
class LogicalCreateTable;
class LogicalCreateIndex;
class LogicalCrossProduct;
class LogicalDelete;
class LogicalDelimGet;
class LogicalFilter;
class LogicalGet;
class LogicalJoin;
class LogicalLimit;
class LogicalOperator;
class LogicalOrder;
class LogicalProjection;
class LogicalInsert;
class LogicalCopy;
class LogicalExplain;
class LogicalUnion;
class LogicalExcept;
class LogicalIntersect;
class LogicalSubquery;
class LogicalUpdate;
class LogicalTableFunction;
class LogicalPruneColumns;
class LogicalWindow;

//! The LogicalOperatorVisitor is an abstract base class that implements the
//! Visitor pattern on LogicalOperator.
class LogicalOperatorVisitor : public SQLNodeVisitor {
public:
	using SQLNodeVisitor::Visit;

	virtual ~LogicalOperatorVisitor(){};

	virtual void VisitOperator(LogicalOperator &op);

protected:
	//! Automatically calls the Visit method for LogicalOperator children of the current operator. Can be overloaded to
	//! change this behavior.
	void VisitOperatorChildren(LogicalOperator &op);
	//! Automatically calls the Visit method for Expression children of the current operator. Can be overloaded to
	//! change this behavior.
	void VisitOperatorExpressions(LogicalOperator &op);
};
} // namespace duckdb
