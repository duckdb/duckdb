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
class CreateTableAsStatement;
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
class PrepareStatement;
class ExecuteStatement;
class DeallocateStatement;

//===--------------------------------------------------------------------===//
// Query Node
//===--------------------------------------------------------------------===//
class QueryNode;
class SelectNode;
class SetOperationNode;

//===--------------------------------------------------------------------===//
// Expressions
//===--------------------------------------------------------------------===//
class ParsedExpression;

class AggregateExpression;
class CaseExpression;
class CastExpression;
class ColumnRefExpression;
class ComparisonExpression;
class ConjunctionExpression;
class ConstantExpression;
class DefaultExpression;
class FunctionExpression;
class OperatorExpression;
class ParameterExpression;
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
