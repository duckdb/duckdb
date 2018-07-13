/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - operator_precedence_warning
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * parse_expr.c
 *	  handle expressions in parser
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_expr.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "parser/parse_agg.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/xml.h"


/* GUC parameters */
__thread bool		operator_precedence_warning = false;



/*
 * Node-type groups for operator precedence warnings
 * We use zero for everything not otherwise classified
 */
#define PREC_GROUP_POSTFIX_IS	1		/* postfix IS tests (NullTest, etc) */
#define PREC_GROUP_INFIX_IS		2		/* infix IS (IS DISTINCT FROM, etc) */
#define PREC_GROUP_LESS			3		/* < > */
#define PREC_GROUP_EQUAL		4		/* = */
#define PREC_GROUP_LESS_EQUAL	5		/* <= >= <> */
#define PREC_GROUP_LIKE			6		/* LIKE ILIKE SIMILAR */
#define PREC_GROUP_BETWEEN		7		/* BETWEEN */
#define PREC_GROUP_IN			8		/* IN */
#define PREC_GROUP_NOT_LIKE		9		/* NOT LIKE/ILIKE/SIMILAR */
#define PREC_GROUP_NOT_BETWEEN	10		/* NOT BETWEEN */
#define PREC_GROUP_NOT_IN		11		/* NOT IN */
#define PREC_GROUP_POSTFIX_OP	12		/* generic postfix operators */
#define PREC_GROUP_INFIX_OP		13		/* generic infix operators */
#define PREC_GROUP_PREFIX_OP	14		/* generic prefix operators */

/*
 * Map precedence groupings to old precedence ordering
 *
 * Old precedence order:
 * 1. NOT
 * 2. =
 * 3. < >
 * 4. LIKE ILIKE SIMILAR
 * 5. BETWEEN
 * 6. IN
 * 7. generic postfix Op
 * 8. generic Op, including <= => <>
 * 9. generic prefix Op
 * 10. IS tests (NullTest, BooleanTest, etc)
 *
 * NOT BETWEEN etc map to BETWEEN etc when considered as being on the left,
 * but to NOT when considered as being on the right, because of the buggy
 * precedence handling of those productions in the old grammar.
 */



static Node *transformExprRecurse(ParseState *pstate, Node *expr);
static Node *transformParamRef(ParseState *pstate, ParamRef *pref);
static Node *transformAExprOp(ParseState *pstate, A_Expr *a);
static Node *transformAExprOpAny(ParseState *pstate, A_Expr *a);
static Node *transformAExprOpAll(ParseState *pstate, A_Expr *a);
static Node *transformAExprDistinct(ParseState *pstate, A_Expr *a);
static Node *transformAExprNullIf(ParseState *pstate, A_Expr *a);
static Node *transformAExprOf(ParseState *pstate, A_Expr *a);
static Node *transformAExprIn(ParseState *pstate, A_Expr *a);
static Node *transformAExprBetween(ParseState *pstate, A_Expr *a);
static Node *transformBoolExpr(ParseState *pstate, BoolExpr *a);
static Node *transformFuncCall(ParseState *pstate, FuncCall *fn);
static Node *transformMultiAssignRef(ParseState *pstate, MultiAssignRef *maref);
static Node *transformCaseExpr(ParseState *pstate, CaseExpr *c);
static Node *transformSubLink(ParseState *pstate, SubLink *sublink);
static Node *transformArrayExpr(ParseState *pstate, A_ArrayExpr *a,
				   Oid array_type, Oid element_type, int32 typmod);
static Node *transformRowExpr(ParseState *pstate, RowExpr *r);
static Node *transformCoalesceExpr(ParseState *pstate, CoalesceExpr *c);
static Node *transformMinMaxExpr(ParseState *pstate, MinMaxExpr *m);
static Node *transformXmlExpr(ParseState *pstate, XmlExpr *x);
static Node *transformXmlSerialize(ParseState *pstate, XmlSerialize *xs);
static Node *transformBooleanTest(ParseState *pstate, BooleanTest *b);
static Node *transformCurrentOfExpr(ParseState *pstate, CurrentOfExpr *cexpr);
static Node *transformColumnRef(ParseState *pstate, ColumnRef *cref);
static Node *transformWholeRowRef(ParseState *pstate, RangeTblEntry *rte,
					 int location);
static Node *transformIndirection(ParseState *pstate, Node *basenode,
					 List *indirection);
static Node *transformTypeCast(ParseState *pstate, TypeCast *tc);
static Node *transformCollateClause(ParseState *pstate, CollateClause *c);
static Node *make_row_comparison_op(ParseState *pstate, List *opname,
					   List *largs, List *rargs, int location);
static Node *make_row_distinct_op(ParseState *pstate, List *opname,
					 RowExpr *lrow, RowExpr *rrow, int location);
static Expr *make_distinct_op(ParseState *pstate, List *opname,
				 Node *ltree, Node *rtree, int location);
static int	operator_precedence_group(Node *node, const char **nodename);
static void emit_precedence_warnings(ParseState *pstate,
						 int opgroup, const char *opname,
						 Node *lchild, Node *rchild,
						 int location);


/*
 * transformExpr -
 *	  Analyze and transform expressions. Type checking and type casting is
 *	  done here.  This processing converts the raw grammar output into
 *	  expression trees with fully determined semantics.
 */




/*
 * helper routine for delivering "column does not exist" error message
 *
 * (Usually we don't have to work this hard, but the general case of field
 * selection from an arbitrary node needs it.)
 */




/*
 * Transform a ColumnRef.
 *
 * If you find yourself changing this code, see also ExpandColumnRefStar.
 */




/* Test whether an a_expr is a plain NULL constant or not */












/*
 * Checking an expression for match to a list of type names. Will result
 * in a boolean constant node.
 */
















/*
 * transformArrayExpr
 *
 * If the caller specifies the target type, the resulting array will
 * be of exactly that type.  Otherwise we try to infer a common type
 * for the elements using select_common_type().
 */
















/*
 * Construct a whole-row reference to represent the notation "relation.*".
 */


/*
 * Handle an explicit CAST construct.
 *
 * Transform the argument, look up the type name, and apply any necessary
 * coercion function(s).
 */


/*
 * Handle an explicit COLLATE clause.
 *
 * Transform the argument, and look up the collation name.
 */


/*
 * Transform a "row compare-op row" construct
 *
 * The inputs are lists of already-transformed expressions.
 * As with coerce_type, pstate may be NULL if no special unknown-Param
 * processing is wanted.
 *
 * The output may be a single OpExpr, an AND or OR combination of OpExprs,
 * or a RowCompareExpr.  In all cases it is guaranteed to return boolean.
 * The AND, OR, and RowCompareExpr cases further imply things about the
 * behavior of the operators (ie, they behave as =, <>, or < <= > >=).
 */


/*
 * Transform a "row IS DISTINCT FROM row" construct
 *
 * The input RowExprs are already transformed
 */


/*
 * make the node for an IS DISTINCT FROM operator
 */


/*
 * Identify node's group for operator precedence warnings
 *
 * For items in nonzero groups, also return a suitable node name into *nodename
 *
 * Note: group zero is used for nodes that are higher or lower precedence
 * than everything that changed precedence; we need never issue warnings
 * related to such nodes.
 */


/*
 * helper routine for delivering 9.4-to-9.5 operator precedence warnings
 *
 * opgroup/opname/location represent some parent node
 * lchild, rchild are its left and right children (either could be NULL)
 *
 * This should be called before transforming the child nodes, since if a
 * precedence-driven parsing change has occurred in a query that used to work,
 * it's quite possible that we'll get a semantic failure while analyzing the
 * child expression.  We want to produce the warning before that happens.
 * In any case, operator_precedence_group() expects untransformed input.
 */


/*
 * Produce a string identifying an expression by kind.
 *
 * Note: when practical, use a simple SQL keyword for the result.  If that
 * doesn't work well, check call sites to see whether custom error message
 * strings are required.
 */

