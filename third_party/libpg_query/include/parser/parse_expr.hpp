/*-------------------------------------------------------------------------
 *
 * parse_expr.h
 *	  handle expressions in parser
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
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
bool Transform_null_equals;

PGNode *transformExpr(PGParseState *pstate, PGNode *expr, PGParseExprKind exprKind);

const char *ParseExprKindName(PGParseExprKind exprKind);
