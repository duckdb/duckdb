//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/tokens.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

//===--------------------------------------------------------------------===//
// Statements
//===--------------------------------------------------------------------===//
class SQLStatement;

class AlterTableStatement;
class CopyStatement;
class CreateIndexStatement;
class CreateSchemaStatement;
class CreateTableStatement;
class CreateViewStatement;
class DeleteStatement;
class DropSchemaStatement;
class DropTableStatement;
class DropIndexStatement;
class DropViewStatement;
class InsertStatement;
class SelectStatement;
class TransactionStatement;
class UpdateStatement;

//===--------------------------------------------------------------------===//
// Query Node
//===--------------------------------------------------------------------===//
class QueryNode;
class SelectNode;
class SetOperationNode;

//===--------------------------------------------------------------------===//
// Expressions
//===--------------------------------------------------------------------===//
class Expression;

class AggregateExpression;
class BoundColumnRefExpression;
class BoundFunctionExpression;
class BoundExpression;
class BoundSubqueryExpression;
class CaseExpression;
class CastExpression;
class CommonSubExpression;
class ColumnRefExpression;
class ComparisonExpression;
class ConjunctionExpression;
class ConstantExpression;
class DefaultExpression;
class FunctionExpression;
class OperatorExpression;
class StarExpression;
class SubqueryExpression;
class WindowExpression;

//===--------------------------------------------------------------------===//
// Constraints
//===--------------------------------------------------------------------===//
class Constraint;

class NotNullConstraint;
class CheckConstraint;
class ParsedConstraint;

//===--------------------------------------------------------------------===//
// TableRefs
//===--------------------------------------------------------------------===//
class TableRef;

class BaseTableRef;
class CrossProductRef;
class JoinRef;
class SubqueryRef;
class TableFunction;

} // namespace duckdb
