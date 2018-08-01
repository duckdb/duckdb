//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/sql_node_visitor.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class SelectStatement;
class CreateStatement;
class InsertStatement;

class AggregateExpression;
class BaseTableRefExpression;
class CastExpression;
class ColumnRefExpression;
class ComparisonExpression;
class ConjunctionExpression;
class ConstantExpression;
class CrossProductExpression;
class FunctionExpression;
class GroupRefExpression;
class JoinExpression;
class OperatorExpression;
class SubqueryExpression;
class TableRefExpression;

//! The SQLNodeVisitor is an abstract base class that implements the Visitor
//! pattern on AbstractExpression and SQLStatement. It will visit nodes
//! recursively and call the Visit expression corresponding to the expression
//! visited.
class SQLNodeVisitor {
  public:
	virtual ~SQLNodeVisitor(){};

	virtual void Visit(SelectStatement &);
	virtual void Visit(CreateStatement &){};
	virtual void Visit(InsertStatement &){};

	virtual void Visit(AggregateExpression &expr);
	virtual void Visit(BaseTableRefExpression &expr);
	virtual void Visit(CastExpression &expr);
	virtual void Visit(ColumnRefExpression &expr);
	virtual void Visit(ComparisonExpression &expr);
	virtual void Visit(ConjunctionExpression &expr);
	virtual void Visit(ConstantExpression &expr);
	virtual void Visit(CrossProductExpression &expr);
	virtual void Visit(FunctionExpression &expr);
	virtual void Visit(GroupRefExpression &expr);
	virtual void Visit(JoinExpression &expr);
	virtual void Visit(OperatorExpression &expr);
	virtual void Visit(SubqueryExpression &expr);
	virtual void Visit(TableRefExpression &expr);
};
} // namespace duckdb
