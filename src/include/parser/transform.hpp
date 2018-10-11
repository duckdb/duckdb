//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/transform.hpp
//
// Author: Mark Raasveldt
// Description: Transform functions take as input elements from the Postgres
//              parse tree, and convert them into the internal representaton
//              used by DuckDB.
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
std::unique_ptr<SelectStatement> TransformSelect(postgres::Node *node);
//! Transform a Postgres T_CreateStmt node into a CreateStatement
std::unique_ptr<CreateTableStatement> TransformCreate(postgres::Node *node);
//! Transform a Postgres T_DropStmt node into a DropTableStatement
std::unique_ptr<DropTableStatement> TransformDrop(postgres::Node *node);
//! Transform a Postgres T_InsertStmt node into a InsertStatement
std::unique_ptr<InsertStatement> TransformInsert(postgres::Node *node);
//! Transform a Postgres T_CopyStmt node into a CopyStatement
std::unique_ptr<CopyStatement> TransformCopy(postgres::Node *node);
//! Transform a Postgres T_TransactionStmt node into a TransactionStatement
std::unique_ptr<TransactionStatement>
TransformTransaction(postgres::Node *node);
//! Transform a Postgres T_DeleteStatement node into a DeleteStatement
std::unique_ptr<DeleteStatement> TransformDelete(postgres::Node *node);
//! Transform a Postgres T_UpdateStmt node into a UpdateStatement
std::unique_ptr<UpdateStatement> TransformUpdate(postgres::Node *node);
//! Transform a Postgres column reference into an Expression
std::unique_ptr<Expression> TransformColumnRef(postgres::ColumnRef *root);
//! Transform a Postgres constant value into an Expression
std::unique_ptr<Expression> TransformValue(postgres::value val);
//! Transform a Postgres operator into an Expression
std::unique_ptr<Expression> TransformAExpr(postgres::A_Expr *root);
//! Transform a Postgres abstract expression into an Expression
std::unique_ptr<Expression> TransformExpression(postgres::Node *node);
//! Transform a Postgres function call into an Expression
std::unique_ptr<Expression> TransformFuncCall(postgres::FuncCall *root);
//! Transform a Postgres FROM clause into an Expression
std::unique_ptr<TableRef> TransformFrom(postgres::List *root);
//! Transform a Postgres constant value into an Expression
std::unique_ptr<Expression> TransformConstant(postgres::A_Const *c);
//! Transform a Postgres table reference into an Expression
std::unique_ptr<TableRef> TransformRangeVar(postgres::RangeVar *root);

//! Transform a Postgres TypeName string into a TypeId
TypeId TransformStringToTypeId(char *str);

//! Transform a Postgres GROUP BY expression into a list of Expression
bool TransformGroupBy(postgres::List *group,
                      std::vector<std::unique_ptr<Expression>> &result);
//! Transform a Postgres ORDER BY expression into an OrderByDescription
bool TransformOrderBy(postgres::List *order, OrderByDescription &result);

//! Transform a Postgres SELECT clause into a list of Expression
bool TransformExpressionList(postgres::List *list,
                             std::vector<std::unique_ptr<Expression>> &result);

} // namespace duckdb
