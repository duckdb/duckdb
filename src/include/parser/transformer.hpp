//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/transformer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/column_definition.hpp"
#include "parser/statement/select_statement.hpp"
#include "parser/tokens.hpp"

namespace postgres {
// FIXME defines evil
#include <limits.h>
#define PG_INT32_MAX INT_MAX
#define PGDLLIMPORT

typedef int16_t int16;
typedef int32_t int32;

typedef uint32_t uint32;
typedef uint32 Index;
typedef uint32 bits32;
typedef uint32 Oid;

typedef uintptr_t Datum;

#include "nodes/parsenodes.h"
} // namespace postgres

namespace duckdb {

//! The transformer class is responsible for transforming the internal Postgres
//! parser representation into the DuckDB representation
class Transformer {
public:
	//! Transforms a Postgres parse tree into a set of SQL Statements
	bool TransformParseTree(postgres::List *tree, vector<unique_ptr<SQLStatement>> &statements);

private:
	//! Transforms a Postgres statement into a single SQL statement
	unique_ptr<SQLStatement> TransformStatement(postgres::Node *stmt);
	//===--------------------------------------------------------------------===//
	// Statement transformation
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres T_SelectStmt node into a SelectStatement
	unique_ptr<SelectStatement> TransformSelect(postgres::Node *node);
	//! Transform a Postgres T_AlterStmt node into a AlterTableStatement
	unique_ptr<AlterTableStatement> TransformAlter(postgres::Node *node);
	//! Transform a Postgres T_RenameStmt node into a RenameStatement
	unique_ptr<AlterTableStatement> TransformRename(postgres::Node *node);
	//! Transform a Postgres T_CreateStmt node into a CreateTableStatement
	unique_ptr<CreateTableStatement> TransformCreateTable(postgres::Node *node);
	//! Transform a Postgres T_CreateStmt node into a CreateTableStatement
	unique_ptr<CreateTableStatement> TransformCreateTableAs(postgres::Node *node);
	//! Transform a Postgres T_CreateSchemaStmt node into a
	//! CreateSchemaStatement
	unique_ptr<CreateSchemaStatement> TransformCreateSchema(postgres::Node *node);
	//! Transform a Postgres T_ViewStmt node into a CreateViewStatement
	unique_ptr<CreateViewStatement> TransformCreateView(postgres::Node *node);
	//! Transform a Postgres T_DropStmt node into a Drop[Table,Schema]Statement
	unique_ptr<SQLStatement> TransformDrop(postgres::Node *node);
	//! Transform a Postgres T_InsertStmt node into a InsertStatement
	unique_ptr<InsertStatement> TransformInsert(postgres::Node *node);
	//! Transform a Postgres T_IndexStmt node into CreateIndexStatement
	unique_ptr<CreateIndexStatement> TransformCreateIndex(postgres::Node *node);
	//! Transform a Postgres DropStmt node into a DropTableStatement
	unique_ptr<DropTableStatement> TransformDropTable(postgres::DropStmt *stmt);
	//! Transform a Postgres DropStmt node into a DropIndexStatement
	unique_ptr<DropIndexStatement> TransformDropIndex(postgres::DropStmt *stmt);
	//! Transform a Postgres DropStmt node into a DropSchemaStatement
	unique_ptr<DropSchemaStatement> TransformDropSchema(postgres::DropStmt *stmt);
	//! Transform a Postgres DropStmt node into a DropViewStatement
	unique_ptr<DropViewStatement> TransformDropView(postgres::DropStmt *stmt);
	//! Transform a Postgres T_CopyStmt node into a CopyStatement
	unique_ptr<CopyStatement> TransformCopy(postgres::Node *node);
	//! Transform a Postgres T_TransactionStmt node into a TransactionStatement
	unique_ptr<TransactionStatement> TransformTransaction(postgres::Node *node);
	//! Transform a Postgres T_DeleteStatement node into a DeleteStatement
	unique_ptr<DeleteStatement> TransformDelete(postgres::Node *node);
	//! Transform a Postgres T_UpdateStmt node into a UpdateStatement
	unique_ptr<UpdateStatement> TransformUpdate(postgres::Node *node);

	unique_ptr<PrepareStatement> TransformPrepare(postgres::Node *node);
	unique_ptr<ExecuteStatement> TransformExecute(postgres::Node *node);
	unique_ptr<DeallocateStatement> TransformDeallocate(postgres::Node *node);

	//===--------------------------------------------------------------------===//
	// Query Node Transform
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres T_SelectStmt node into a QueryNode
	unique_ptr<QueryNode> TransformSelectNode(postgres::SelectStmt *node);

	//===--------------------------------------------------------------------===//
	// Expression Transform
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres boolean expression into an Expression
	unique_ptr<Expression> TransformBoolExpr(postgres::BoolExpr *root);
	//! Transform a Postgres case expression into an Expression
	unique_ptr<Expression> TransformCase(postgres::CaseExpr *root);
	//! Transform a Postgres type cast into an Expression
	unique_ptr<Expression> TransformTypeCast(postgres::TypeCast *root);
	//! Transform a Postgres coalesce into an Expression
	unique_ptr<Expression> TransformCoalesce(postgres::A_Expr *root);
	//! Transform a Postgres column reference into an Expression
	unique_ptr<Expression> TransformColumnRef(postgres::ColumnRef *root);
	//! Transform a Postgres constant value into an Expression
	unique_ptr<Expression> TransformValue(postgres::Value val);
	//! Transform a Postgres operator into an Expression
	unique_ptr<Expression> TransformAExpr(postgres::A_Expr *root);
	//! Transform a Postgres abstract expression into an Expression
	unique_ptr<Expression> TransformExpression(postgres::Node *node);
	//! Transform a Postgres function call into an Expression
	unique_ptr<Expression> TransformFuncCall(postgres::FuncCall *root);

	//! Transform a Postgres constant value into an Expression
	unique_ptr<Expression> TransformConstant(postgres::A_Const *c);

	unique_ptr<Expression> TransformResTarget(postgres::ResTarget *root);
	unique_ptr<Expression> TransformNullTest(postgres::NullTest *root);
	unique_ptr<Expression> TransformParamRef(postgres::ParamRef *node);

	unique_ptr<Expression> TransformSubquery(postgres::SubLink *root);
	//===--------------------------------------------------------------------===//
	// Constraints transform
	//===--------------------------------------------------------------------===//
	unique_ptr<Constraint> TransformConstraint(postgres::ListCell *cell);

	unique_ptr<Constraint> TransformConstraint(postgres::ListCell *cell, ColumnDefinition column, size_t index);

	//===--------------------------------------------------------------------===//
	// Helpers
	//===--------------------------------------------------------------------===//
	string TransformAlias(postgres::Alias *root);
	void TransformCTE(postgres::WithClause *de_with_clause, SelectStatement &select);
	// Operator String to ExpressionType (e.g. + => OPERATOR_ADD)
	ExpressionType OperatorToExpressionType(string &op);
	//! Convert a expression node to a constant integer. Throws an exception if the expression is non-scalar.
	int64_t ConstantFromExpression(postgres::Node *node);
	//===--------------------------------------------------------------------===//
	// TableRef transform
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres node into a TableRef
	unique_ptr<TableRef> TransformTableRefNode(postgres::Node *node);
	//! Transform a Postgres FROM clause into an Expression
	unique_ptr<TableRef> TransformFrom(postgres::List *root);
	//! Transform a Postgres table reference into an Expression
	unique_ptr<TableRef> TransformRangeVar(postgres::RangeVar *root);
	//! Transform a Postgres table-producing function into a TableRef
	unique_ptr<TableRef> TransformRangeFunction(postgres::RangeFunction *root);
	//! Transform a Postgres join node into a TableRef
	unique_ptr<TableRef> TransformJoin(postgres::JoinExpr *root);
	//! Transform a table producing subquery into a TableRef
	unique_ptr<TableRef> TransformRangeSubselect(postgres::RangeSubselect *root);

	//! Transform a Postgres TypeName string into a TypeId
	TypeId TransformStringToTypeId(char *str);

	//! Transform a Postgres GROUP BY expression into a list of Expression
	bool TransformGroupBy(postgres::List *group, vector<unique_ptr<Expression>> &result);
	//! Transform a Postgres ORDER BY expression into an OrderByDescription
	bool TransformOrderBy(postgres::List *order, OrderByDescription &result);

	//! Transform a Postgres SELECT clause into a list of Expression
	bool TransformExpressionList(postgres::List *list, vector<unique_ptr<Expression>> &result);

	void TransformWindowDef(postgres::WindowDef *window_spec, WindowExpression *expr);

	//! Holds window expressions defined by name. We need those when transforming the expressions referring to them.
	map<string, postgres::WindowDef *> window_clauses;
};

} // namespace duckdb
