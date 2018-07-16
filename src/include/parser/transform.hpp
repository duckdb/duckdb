
#pragma once

#include "parser/statement/select_statement.hpp"

#include "parser/parsenodes.h"
#include "parser/pg_list.h"
#include "parser/pg_query.h"
#include "parser/pg_trigger.h"

std::unique_ptr<SelectStatement> TransformSelect(Node *node);

std::unique_ptr<AbstractExpression> TransformColumnRef(ColumnRef *root);
std::unique_ptr<AbstractExpression> TransformValue(value val);
std::unique_ptr<AbstractExpression> TransformAExpr(A_Expr *root);
std::unique_ptr<AbstractExpression> TransformExpression(Node *node);
std::unique_ptr<AbstractExpression> TransformFuncCall(FuncCall *root);

bool TransformExpressionList(
    List *list, std::vector<std::unique_ptr<AbstractExpression>> &result);