/*-------------------------------------------------------------------------
 *
 * nodeFuncs.h
 *		Various general-purpose manipulations of PGNode trees
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/nodeFuncs.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include "nodes/parsenodes.hpp"

namespace duckdb_libpgquery {

/* flags bits for query_tree_walker and query_tree_mutator */
#define QTW_IGNORE_RT_SUBQUERIES 0x01 /* subqueries in rtable */
#define QTW_IGNORE_CTE_SUBQUERIES 0x02 /* subqueries in cteList */
#define QTW_IGNORE_RC_SUBQUERIES 0x03 /* both of above */
#define QTW_IGNORE_JOINALIASES 0x04 /* JOIN alias var lists */
#define QTW_IGNORE_RANGE_TABLE 0x08 /* skip rangetable entirely */
#define QTW_EXAMINE_RTES 0x10 /* examine RTEs */
#define QTW_DONT_COPY_QUERY 0x20 /* do not copy top PGQuery */

/* callback function for check_functions_in_node */
typedef bool (*check_function_callback)(PGOid func_id, void *context);

PGOid exprType(const PGNode *expr);
int32_t exprTypmod(const PGNode *expr);
bool exprIsLengthCoercion(const PGNode *expr, int32_t *coercedTypmod);
PGNode *relabel_to_typmod(PGNode *expr, int32_t typmod);
PGNode *strip_implicit_coercions(PGNode *node);
bool expression_returns_set(PGNode *clause);

PGOid exprCollation(const PGNode *expr);
PGOid exprInputCollation(const PGNode *expr);
void exprSetCollation(PGNode *expr, PGOid collation);
void exprSetInputCollation(PGNode *expr, PGOid inputcollation);

int exprLocation(const PGNode *expr);

void fix_opfuncids(PGNode *node);
void set_opfuncid(PGOpExpr *opexpr);
void set_sa_opfuncid(PGScalarArrayOpExpr *opexpr);

bool check_functions_in_node(PGNode *node, check_function_callback checker, void *context);

bool expression_tree_walker(PGNode *node, bool (*walker)(), void *context);
PGNode *expression_tree_mutator(PGNode *node, PGNode *(*mutator)(), void *context);

bool query_tree_walker(PGQuery *query, bool (*walker)(), void *context, int flags);
PGQuery *query_tree_mutator(PGQuery *query, PGNode *(*mutator)(), void *context, int flags);

bool range_table_walker(PGList *rtable, bool (*walker)(), void *context, int flags);
PGList *range_table_mutator(PGList *rtable, PGNode *(*mutator)(), void *context, int flags);

bool query_or_expression_tree_walker(PGNode *node, bool (*walker)(), void *context, int flags);
PGNode *query_or_expression_tree_mutator(PGNode *node, PGNode *(*mutator)(), void *context, int flags);

bool raw_expression_tree_walker(PGNode *node, bool (*walker)(), void *context);

struct PlanState;
bool planstate_tree_walker(struct PlanState *planstate, bool (*walker)(), void *context);

}
