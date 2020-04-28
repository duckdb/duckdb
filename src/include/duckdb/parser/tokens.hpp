//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tokens.hpp
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
class CreateStatement;
class DeleteStatement;
class DropStatement;
class InsertStatement;
class SelectStatement;
class TransactionStatement;
class UpdateStatement;
class PrepareStatement;
class ExecuteStatement;
class PragmaStatement;
class ExplainStatement;
class VacuumStatement;
class RelationStatement;

//===--------------------------------------------------------------------===//
// Query Node
//===--------------------------------------------------------------------===//
class QueryNode;
class SelectNode;
class SetOperationNode;
class RecursiveCTENode;

//===--------------------------------------------------------------------===//
// Expressions
//===--------------------------------------------------------------------===//
class ParsedExpression;

class CaseExpression;
class CastExpression;
class CollateExpression;
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
class UniqueConstraint;

//===--------------------------------------------------------------------===//
// TableRefs
//===--------------------------------------------------------------------===//
class TableRef;

class BaseTableRef;
class CrossProductRef;
class JoinRef;
class SubqueryRef;
class TableFunctionRef;
class EmptyTableRef;
class ExpressionListRef;

} // namespace duckdb
