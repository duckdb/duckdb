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

#include "parser/statement/copy_statement.hpp"
#include "parser/statement/create_statement.hpp"
#include "parser/statement/explain_statement.hpp"
#include "parser/statement/insert_statement.hpp"
#include "parser/statement/select_statement.hpp"
#include "parser/statement/transaction_statement.hpp"

#include "parser/postgres/parsenodes.h"
#include "parser/postgres/pg_list.h"
#include "parser/postgres/pg_query.h"
#include "parser/postgres/pg_trigger.h"

namespace duckdb {

//! Transform a Postgres T_SelectStmt node into a SelectStatement
std::unique_ptr<SelectStatement> TransformSelect(Node *node);
//! Transform a Postgres T_CreateStmt node into a CreateStatement
std::unique_ptr<CreateStatement> TransformCreate(Node *node);
//! Transform a Postgres T_InsertStmt node into a InsertStatement
std::unique_ptr<InsertStatement> TransformInsert(Node *node);
//! Transform a Postgres T_CopyStmt node into a CopyStatement
std::unique_ptr<CopyStatement> TransformCopy(Node *node);
//! Transform a Postgres T_TransactionStmt node into a TransactionStatement
std::unique_ptr<TransactionStatement> TransformTransaction(Node *node);
//! Transform a Postgres EXPLAIN node into a ExplainStatement
std::unique_ptr<ExplainStatement> TransformExplain(Node *node);
//! Transform a Postgres column reference into an AbstractExpression
std::unique_ptr<AbstractExpression> TransformColumnRef(ColumnRef *root);
//! Transform a Postgres constant value into an AbstractExpression
std::unique_ptr<AbstractExpression> TransformValue(value val);
//! Transform a Postgres operator into an AbstractExpression
std::unique_ptr<AbstractExpression> TransformAExpr(A_Expr *root);
//! Transform a Postgres abstract expression into an AbstractExpression
std::unique_ptr<AbstractExpression> TransformExpression(Node *node);
//! Transform a Postgres function call into an AbstractExpression
std::unique_ptr<AbstractExpression> TransformFuncCall(FuncCall *root);
//! Transform a Postgres FROM clause into an AbstractExpression
std::unique_ptr<TableRef> TransformFrom(List *root);
//! Transform a Postgres constant value into an AbstractExpression
std::unique_ptr<AbstractExpression> TransformConstant(A_Const *c);
//! Transform a Postgres table reference into an AbstractExpression
std::unique_ptr<TableRef> TransformRangeVar(RangeVar *root);

//! Transform a Postgres TypeName string into a TypeId
TypeId TransformStringToTypeId(char *str);

//! Transform a Postgres GROUP BY expression into a list of AbstractExpression
bool TransformGroupBy(List *group,
                      std::vector<std::unique_ptr<AbstractExpression>> &result);
//! Transform a Postgres ORDER BY expression into an OrderByDescription
bool TransformOrderBy(List *order, OrderByDescription &result);

//! Transform a Postgres SELECT clause into a list of AbstractExpression
bool TransformExpressionList(
    List *list, std::vector<std::unique_ptr<AbstractExpression>> &result);

//! Transform a Postgres Value List into a list of AbstractExpression
bool TransformValueList(
    List *list, std::vector<std::unique_ptr<AbstractExpression>> &result);
} // namespace duckdb
