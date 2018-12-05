//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/transform.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/statement/list.hpp"

namespace postgres {
#include "parser/postgres/parsenodes.h"
#include "parser/postgres/pg_list.h"
#include "parser/postgres/pg_query.h"
#include "parser/postgres/pg_trigger.h"
} // namespace postgres

namespace duckdb {

//! Transform a Postgres T_SelectStmt node into a SelectStatement
unique_ptr<SelectStatement> TransformSelect(postgres::Node *node);
//! Transform a Postgres T_AlterStmt node into a AlterTableStatement
unique_ptr<AlterTableStatement> TransformAlter(postgres::Node *node);
//! Transform a Postgres T_RenameStmt node into a RenameStatement
unique_ptr<AlterTableStatement> TransformRename(postgres::Node *node);
//! Transform a Postgres T_CreateStmt node into a CreateTableStatement
unique_ptr<CreateTableStatement> TransformCreateTable(postgres::Node *node);
//! Transform a Postgres T_CreateSchemaStmt node into a CreateSchemaStatement
unique_ptr<CreateSchemaStatement> TransformCreateSchema(postgres::Node *node);
//! Transform a Postgres T_DropStmt node into a Drop[Table,Schema]Statement
unique_ptr<SQLStatement> TransformDrop(postgres::Node *node);
//! Transform a Postgres T_InsertStmt node into a InsertStatement
unique_ptr<InsertStatement> TransformInsert(postgres::Node *node);

//! Transform a Postgres T_IndexStmt node into (Index)CreateStatement
unique_ptr<CreateIndexStatement> TransformCreateIndex(postgres::Node *node);

//! Transform a Postgres T_CopyStmt node into a CopyStatement
unique_ptr<CopyStatement> TransformCopy(postgres::Node *node);
//! Transform a Postgres T_TransactionStmt node into a TransactionStatement
unique_ptr<TransactionStatement> TransformTransaction(postgres::Node *node);
//! Transform a Postgres T_DeleteStatement node into a DeleteStatement
unique_ptr<DeleteStatement> TransformDelete(postgres::Node *node);
//! Transform a Postgres T_UpdateStmt node into a UpdateStatement
unique_ptr<UpdateStatement> TransformUpdate(postgres::Node *node);
//! Transform a Postgres column reference into an Expression
unique_ptr<Expression> TransformColumnRef(postgres::ColumnRef *root);
//! Transform a Postgres constant value into an Expression
unique_ptr<Expression> TransformValue(postgres::value val);
//! Transform a Postgres operator into an Expression
unique_ptr<Expression> TransformAExpr(postgres::A_Expr *root);
//! Transform a Postgres abstract expression into an Expression
unique_ptr<Expression> TransformExpression(postgres::Node *node);
//! Transform a Postgres function call into an Expression
unique_ptr<Expression> TransformFuncCall(postgres::FuncCall *root);
//! Transform a Postgres node into a TableRef
unique_ptr<TableRef> TransformTableRefNode(postgres::Node *node);
//! Transform a Postgres FROM clause into an Expression
unique_ptr<TableRef> TransformFrom(postgres::List *root);
//! Transform a Postgres constant value into an Expression
unique_ptr<Expression> TransformConstant(postgres::A_Const *c);
//! Transform a Postgres table reference into an Expression
unique_ptr<TableRef> TransformRangeVar(postgres::RangeVar *root);
//! Transform a Postgres table-producing function into a TableRef
unique_ptr<TableRef> TransformRangeFunction(postgres::RangeFunction *root);

//! Transform a Postgres TypeName string into a TypeId
TypeId TransformStringToTypeId(char *str);

//! Transform a Postgres GROUP BY expression into a list of Expression
bool TransformGroupBy(postgres::List *group, vector<unique_ptr<Expression>> &result);
//! Transform a Postgres ORDER BY expression into an OrderByDescription
bool TransformOrderBy(postgres::List *order, OrderByDescription &result);

//! Transform a Postgres SELECT clause into a list of Expression
bool TransformExpressionList(postgres::List *list, vector<unique_ptr<Expression>> &result);

} // namespace duckdb
