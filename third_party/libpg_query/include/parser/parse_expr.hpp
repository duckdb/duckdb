/*-------------------------------------------------------------------------
 *
 * parse_expr.h
 *	  handle expressions in parser
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_expr.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include "parser/parse_node.hpp"

/* GUC parameters */
//extern __thread  bool operator_precedence_warning;
extern bool Transform_null_equals;

extern Node *transformExpr(ParseState *pstate, Node *expr, ParseExprKind exprKind);

extern const char *ParseExprKindName(ParseExprKind exprKind);
