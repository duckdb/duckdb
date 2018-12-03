//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// parser/tokens.hpp
// 
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
class DeleteStatement;
class DropSchemaStatement;
class DropTableStatement;
class InsertStatement;
class SelectStatement;
class TransactionStatement;
class UpdateStatement;

//===--------------------------------------------------------------------===//
// Expressions
//===--------------------------------------------------------------------===//
class Expression;

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