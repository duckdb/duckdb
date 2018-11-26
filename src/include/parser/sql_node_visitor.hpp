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

class AlterTableStatement;
class CopyStatement;
class CreateSchemaStatement;
class CreateTableStatement;
class DeleteStatement;
class DropSchemaStatement;
class DropTableStatement;
class InsertStatement;
class SelectStatement;
class TransactionStatement;
class UpdateStatement;

class AggregateExpression;
class CaseExpression;
class CastExpression;
class ColumnRefExpression;
class ComparisonExpression;
class ConjunctionExpression;
class ConstantExpression;
class DefaultExpression;
class FunctionExpression;
class GroupRefExpression;
class OperatorExpression;
class StarExpression;
class SubqueryExpression;

class NotNullConstraint;
class CheckConstraint;
class ParsedConstraint;

class BaseTableRef;
class CrossProductRef;
class JoinRef;
class SubqueryRef;
class TableRef;
class TableFunction;
//! The SQLNodeVisitor is an abstract base class that implements the Visitor
//! pattern on Expression and SQLStatement. It will visit nodes
//! recursively and call the Visit expression corresponding to the expression
//! visited.
class SQLNodeVisitor {
  public:
	virtual ~SQLNodeVisitor(){};

	virtual void Visit(AlterTableStatement &){};
	virtual void Visit(CopyStatement &){};
	virtual void Visit(CreateSchemaStatement &){};
	virtual void Visit(CreateTableStatement &){};
	virtual void Visit(DeleteStatement &){};
	virtual void Visit(DropSchemaStatement &){};
	virtual void Visit(DropTableStatement &){};
	virtual void Visit(InsertStatement &){};
	virtual void Visit(SelectStatement &);
	virtual void Visit(TransactionStatement &){};
	virtual void Visit(UpdateStatement &){};

	virtual void Visit(AggregateExpression &expr);
	virtual void Visit(CaseExpression &expr);
	virtual void Visit(CastExpression &expr);
	virtual void Visit(ColumnRefExpression &expr);
	virtual void Visit(ComparisonExpression &expr);
	virtual void Visit(ConjunctionExpression &expr);
	virtual void Visit(ConstantExpression &expr);
	virtual void Visit(DefaultExpression &expr);
	virtual void Visit(FunctionExpression &expr);
	virtual void Visit(GroupRefExpression &expr);
	virtual void Visit(OperatorExpression &expr);
	virtual void Visit(StarExpression &expr);
	virtual void Visit(SubqueryExpression &expr);

	virtual void Visit(NotNullConstraint &expr);
	virtual void Visit(CheckConstraint &expr);
	virtual void Visit(ParsedConstraint &expr);

	virtual void Visit(BaseTableRef &expr);
	virtual void Visit(CrossProductRef &expr);
	virtual void Visit(JoinRef &expr);
	virtual void Visit(SubqueryRef &expr);
	virtual void Visit(TableFunction &expr);
};
} // namespace duckdb
