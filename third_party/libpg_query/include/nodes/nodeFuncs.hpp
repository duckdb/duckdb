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


/* flags bits for query_tree_walker and query_tree_mutator */
#define QTW_IGNORE_RT_SUBQUERIES	0x01	/* subqueries in rtable */
#define QTW_IGNORE_CTE_SUBQUERIES	0x02	/* subqueries in cteList */
#define QTW_IGNORE_RC_SUBQUERIES	0x03	/* both of above */
#define QTW_IGNORE_JOINALIASES		0x04	/* JOIN alias var lists */
#define QTW_IGNORE_RANGE_TABLE		0x08	/* skip rangetable entirely */
#define QTW_EXAMINE_RTES			0x10	/* examine RTEs */
#define QTW_DONT_COPY_QUERY			0x20	/* do not copy top PGQuery */

/* callback function for check_functions_in_node */
typedef bool (*check_function_callback) (PGOid func_id, void *context);


extern PGOid	exprType(const PGNode *expr);
extern int32_t exprTypmod(const PGNode *expr);
extern bool exprIsLengthCoercion(const PGNode *expr, int32_t *coercedTypmod);
extern PGNode *relabel_to_typmod(PGNode *expr, int32_t typmod);
extern PGNode *strip_implicit_coercions(PGNode *node);
extern bool expression_returns_set(PGNode *clause);

extern PGOid	exprCollation(const PGNode *expr);
extern PGOid	exprInputCollation(const PGNode *expr);
extern void exprSetCollation(PGNode *expr, PGOid collation);
extern void exprSetInputCollation(PGNode *expr, PGOid inputcollation);

extern int	exprLocation(const PGNode *expr);

extern void fix_opfuncids(PGNode *node);
extern void set_opfuncid(PGOpExpr *opexpr);
extern void set_sa_opfuncid(PGScalarArrayOpExpr *opexpr);

extern bool check_functions_in_node(PGNode *node, check_function_callback checker,
						void *context);

extern bool expression_tree_walker(PGNode *node, bool (*walker) (),
								   void *context);
extern PGNode *expression_tree_mutator(PGNode *node, PGNode *(*mutator) (),
									 void *context);

extern bool query_tree_walker(PGQuery *query, bool (*walker) (),
							  void *context, int flags);
extern PGQuery *query_tree_mutator(PGQuery *query, PGNode *(*mutator) (),
								 void *context, int flags);

extern bool range_table_walker(PGList *rtable, bool (*walker) (),
							   void *context, int flags);
extern PGList *range_table_mutator(PGList *rtable, PGNode *(*mutator) (),
								 void *context, int flags);

extern bool query_or_expression_tree_walker(PGNode *node, bool (*walker) (),
											void *context, int flags);
extern PGNode *query_or_expression_tree_mutator(PGNode *node, PGNode *(*mutator) (),
											  void *context, int flags);

extern bool raw_expression_tree_walker(PGNode *node, bool (*walker) (),
									   void *context);

struct PlanState;
extern bool planstate_tree_walker(struct PlanState *planstate, bool (*walker) (),
								  void *context);

