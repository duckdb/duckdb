
#pragma once

#include "parser/statement/select_statement.hpp"

#include "parser/parsenodes.h"
#include "parser/pg_list.h"
#include "parser/pg_query.h"
#include "parser/pg_trigger.h"

std::unique_ptr<duckdb::SelectStatement> TransformSelect(Node *node);

std::unique_ptr<duckdb::AbstractExpression> TransformColumnRef(ColumnRef *root);
std::unique_ptr<duckdb::AbstractExpression> TransformValue(value val);
std::unique_ptr<duckdb::AbstractExpression> TransformAExpr(A_Expr *root);
std::unique_ptr<duckdb::AbstractExpression> TransformExpression(Node *node);
std::unique_ptr<duckdb::AbstractExpression> TransformFuncCall(FuncCall *root);
std::unique_ptr<duckdb::AbstractExpression> TransformFrom(List *root);
std::unique_ptr<duckdb::AbstractExpression> TransformConstant(A_Const *c);

bool TransformGroupBy(
    List *group,
    std::vector<std::unique_ptr<duckdb::AbstractExpression>> &result);
bool TransformOrderBy(List *order, duckdb::OrderByDescription &result);

bool TransformExpressionList(
    List *list,
    std::vector<std::unique_ptr<duckdb::AbstractExpression>> &result);
