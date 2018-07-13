/*-------------------------------------------------------------------------
 *
 * functions.h
 *		Declarations for execution of SQL-language functions.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/functions.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FUNCTIONS_H
#define FUNCTIONS_H

#include "nodes/execnodes.h"
#include "tcop/dest.h"

/* This struct is known only within executor/functions.c */
typedef struct SQLFunctionParseInfo *SQLFunctionParseInfoPtr;

extern Datum fmgr_sql(PG_FUNCTION_ARGS);

extern SQLFunctionParseInfoPtr prepare_sql_fn_parse_info(HeapTuple procedureTuple,
						  Node *call_expr,
						  Oid inputCollation);

extern void sql_fn_parser_setup(struct ParseState *pstate,
					SQLFunctionParseInfoPtr pinfo);

extern bool check_sql_fn_retval(Oid func_id, Oid rettype,
					List *queryTreeList,
					bool *modifyTargetList,
					JunkFilter **junkFilter);

extern DestReceiver *CreateSQLFunctionDestReceiver(void);

#endif   /* FUNCTIONS_H */
