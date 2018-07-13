/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - exprLocation
 * - leftmostLoc
 * - raw_expression_tree_walker
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * nodeFuncs.c
 *		Various general-purpose manipulations of Node trees
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/nodeFuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/relation.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


static bool expression_returns_set_walker(Node *node, void *context);
static int	leftmostLoc(int loc1, int loc2);


/*
 *	exprType -
 *	  returns the Oid of the type of the expression's result.
 */


/*
 *	exprTypmod -
 *	  returns the type-specific modifier of the expression's result type,
 *	  if it can be determined.  In many cases, it can't and we return -1.
 */


/*
 * exprIsLengthCoercion
 *		Detect whether an expression tree is an application of a datatype's
 *		typmod-coercion function.  Optionally extract the result's typmod.
 *
 * If coercedTypmod is not NULL, the typmod is stored there if the expression
 * is a length-coercion function, else -1 is stored there.
 *
 * Note that a combined type-and-length coercion will be treated as a
 * length coercion by this routine.
 */


/*
 * relabel_to_typmod
 *		Add a RelabelType node that changes just the typmod of the expression.
 *
 * This is primarily intended to be used during planning.  Therefore, it
 * strips any existing RelabelType nodes to maintain the planner's invariant
 * that there are not adjacent RelabelTypes.
 */


/*
 * strip_implicit_coercions: remove implicit coercions at top level of tree
 *
 * This doesn't modify or copy the input expression tree, just return a
 * pointer to a suitable place within it.
 *
 * Note: there isn't any useful thing we can do with a RowExpr here, so
 * just return it unchanged, even if it's marked as an implicit coercion.
 */


/*
 * expression_returns_set
 *	  Test whether an expression returns a set result.
 *
 * Because we use expression_tree_walker(), this can also be applied to
 * whole targetlists; it'll produce TRUE if any one of the tlist items
 * returns a set.
 */





/*
 *	exprCollation -
 *	  returns the Oid of the collation of the expression's result.
 *
 * Note: expression nodes that can invoke functions generally have an
 * "inputcollid" field, which is what the function should use as collation.
 * That is the resolved common collation of the node's inputs.  It is often
 * but not always the same as the result collation; in particular, if the
 * function produces a non-collatable result type from collatable inputs
 * or vice versa, the two are different.
 */


/*
 *	exprInputCollation -
 *	  returns the Oid of the collation a function should use, if available.
 *
 * Result is InvalidOid if the node type doesn't store this information.
 */


/*
 *	exprSetCollation -
 *	  Assign collation information to an expression tree node.
 *
 * Note: since this is only used during parse analysis, we don't need to
 * worry about subplans or PlaceHolderVars.
 */
#ifdef USE_ASSERT_CHECKING
#endif   /* USE_ASSERT_CHECKING */

/*
 *	exprSetInputCollation -
 *	  Assign input-collation information to an expression tree node.
 *
 * This is a no-op for node types that don't store their input collation.
 * Note we omit RowCompareExpr, which needs special treatment since it
 * contains multiple input collation OIDs.
 */



/*
 *	exprLocation -
 *	  returns the parse location of an expression tree, for error reports
 *
 * -1 is returned if the location can't be determined.
 *
 * For expressions larger than a single token, the intent here is to
 * return the location of the expression's leftmost token, not necessarily
 * the topmost Node's location field.  For example, an OpExpr's location
 * field will point at the operator name, but if it is not a prefix operator
 * then we should return the location of the left-hand operand instead.
 * The reason is that we want to reference the entire expression not just
 * that operator, and pointing to its start seems to be the most natural way.
 *
 * The location is not perfect --- for example, since the grammar doesn't
 * explicitly represent parentheses in the parsetree, given something that
 * had been written "(a + b) * c" we are going to point at "a" not "(".
 * But it should be plenty good enough for error reporting purposes.
 *
 * You might think that this code is overly general, for instance why check
 * the operands of a FuncExpr node, when the function name can be expected
 * to be to the left of them?  There are a couple of reasons.  The grammar
 * sometimes builds expressions that aren't quite what the user wrote;
 * for instance x IS NOT BETWEEN ... becomes a NOT-expression whose keyword
 * pointer is to the right of its leftmost argument.  Also, nodes that were
 * inserted implicitly by parse analysis (such as FuncExprs for implicit
 * coercions) will have location -1, and so we can have odd combinations of
 * known and unknown locations in a tree.
 */
int
exprLocation(const Node *expr)
{
	int			loc;

	if (expr == NULL)
		return -1;
	switch (nodeTag(expr))
	{
		case T_RangeVar:
			loc = ((const RangeVar *) expr)->location;
			break;
		case T_Var:
			loc = ((const Var *) expr)->location;
			break;
		case T_Const:
			loc = ((const Const *) expr)->location;
			break;
		case T_Param:
			loc = ((const Param *) expr)->location;
			break;
		case T_Aggref:
			/* function name should always be the first thing */
			loc = ((const Aggref *) expr)->location;
			break;
		case T_GroupingFunc:
			loc = ((const GroupingFunc *) expr)->location;
			break;
		case T_WindowFunc:
			/* function name should always be the first thing */
			loc = ((const WindowFunc *) expr)->location;
			break;
		case T_ArrayRef:
			/* just use array argument's location */
			loc = exprLocation((Node *) ((const ArrayRef *) expr)->refexpr);
			break;
		case T_FuncExpr:
			{
				const FuncExpr *fexpr = (const FuncExpr *) expr;

				/* consider both function name and leftmost arg */
				loc = leftmostLoc(fexpr->location,
								  exprLocation((Node *) fexpr->args));
			}
			break;
		case T_NamedArgExpr:
			{
				const NamedArgExpr *na = (const NamedArgExpr *) expr;

				/* consider both argument name and value */
				loc = leftmostLoc(na->location,
								  exprLocation((Node *) na->arg));
			}
			break;
		case T_OpExpr:
		case T_DistinctExpr:	/* struct-equivalent to OpExpr */
		case T_NullIfExpr:		/* struct-equivalent to OpExpr */
			{
				const OpExpr *opexpr = (const OpExpr *) expr;

				/* consider both operator name and leftmost arg */
				loc = leftmostLoc(opexpr->location,
								  exprLocation((Node *) opexpr->args));
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				const ScalarArrayOpExpr *saopexpr = (const ScalarArrayOpExpr *) expr;

				/* consider both operator name and leftmost arg */
				loc = leftmostLoc(saopexpr->location,
								  exprLocation((Node *) saopexpr->args));
			}
			break;
		case T_BoolExpr:
			{
				const BoolExpr *bexpr = (const BoolExpr *) expr;

				/*
				 * Same as above, to handle either NOT or AND/OR.  We can't
				 * special-case NOT because of the way that it's used for
				 * things like IS NOT BETWEEN.
				 */
				loc = leftmostLoc(bexpr->location,
								  exprLocation((Node *) bexpr->args));
			}
			break;
		case T_SubLink:
			{
				const SubLink *sublink = (const SubLink *) expr;

				/* check the testexpr, if any, and the operator/keyword */
				loc = leftmostLoc(exprLocation(sublink->testexpr),
								  sublink->location);
			}
			break;
		case T_FieldSelect:
			/* just use argument's location */
			loc = exprLocation((Node *) ((const FieldSelect *) expr)->arg);
			break;
		case T_FieldStore:
			/* just use argument's location */
			loc = exprLocation((Node *) ((const FieldStore *) expr)->arg);
			break;
		case T_RelabelType:
			{
				const RelabelType *rexpr = (const RelabelType *) expr;

				/* Much as above */
				loc = leftmostLoc(rexpr->location,
								  exprLocation((Node *) rexpr->arg));
			}
			break;
		case T_CoerceViaIO:
			{
				const CoerceViaIO *cexpr = (const CoerceViaIO *) expr;

				/* Much as above */
				loc = leftmostLoc(cexpr->location,
								  exprLocation((Node *) cexpr->arg));
			}
			break;
		case T_ArrayCoerceExpr:
			{
				const ArrayCoerceExpr *cexpr = (const ArrayCoerceExpr *) expr;

				/* Much as above */
				loc = leftmostLoc(cexpr->location,
								  exprLocation((Node *) cexpr->arg));
			}
			break;
		case T_ConvertRowtypeExpr:
			{
				const ConvertRowtypeExpr *cexpr = (const ConvertRowtypeExpr *) expr;

				/* Much as above */
				loc = leftmostLoc(cexpr->location,
								  exprLocation((Node *) cexpr->arg));
			}
			break;
		case T_CollateExpr:
			/* just use argument's location */
			loc = exprLocation((Node *) ((const CollateExpr *) expr)->arg);
			break;
		case T_CaseExpr:
			/* CASE keyword should always be the first thing */
			loc = ((const CaseExpr *) expr)->location;
			break;
		case T_CaseWhen:
			/* WHEN keyword should always be the first thing */
			loc = ((const CaseWhen *) expr)->location;
			break;
		case T_ArrayExpr:
			/* the location points at ARRAY or [, which must be leftmost */
			loc = ((const ArrayExpr *) expr)->location;
			break;
		case T_RowExpr:
			/* the location points at ROW or (, which must be leftmost */
			loc = ((const RowExpr *) expr)->location;
			break;
		case T_RowCompareExpr:
			/* just use leftmost argument's location */
			loc = exprLocation((Node *) ((const RowCompareExpr *) expr)->largs);
			break;
		case T_CoalesceExpr:
			/* COALESCE keyword should always be the first thing */
			loc = ((const CoalesceExpr *) expr)->location;
			break;
		case T_MinMaxExpr:
			/* GREATEST/LEAST keyword should always be the first thing */
			loc = ((const MinMaxExpr *) expr)->location;
			break;
		case T_XmlExpr:
			{
				const XmlExpr *xexpr = (const XmlExpr *) expr;

				/* consider both function name and leftmost arg */
				loc = leftmostLoc(xexpr->location,
								  exprLocation((Node *) xexpr->args));
			}
			break;
		case T_NullTest:
			{
				const NullTest *nexpr = (const NullTest *) expr;

				/* Much as above */
				loc = leftmostLoc(nexpr->location,
								  exprLocation((Node *) nexpr->arg));
			}
			break;
		case T_BooleanTest:
			{
				const BooleanTest *bexpr = (const BooleanTest *) expr;

				/* Much as above */
				loc = leftmostLoc(bexpr->location,
								  exprLocation((Node *) bexpr->arg));
			}
			break;
		case T_CoerceToDomain:
			{
				const CoerceToDomain *cexpr = (const CoerceToDomain *) expr;

				/* Much as above */
				loc = leftmostLoc(cexpr->location,
								  exprLocation((Node *) cexpr->arg));
			}
			break;
		case T_CoerceToDomainValue:
			loc = ((const CoerceToDomainValue *) expr)->location;
			break;
		case T_SetToDefault:
			loc = ((const SetToDefault *) expr)->location;
			break;
		case T_TargetEntry:
			/* just use argument's location */
			loc = exprLocation((Node *) ((const TargetEntry *) expr)->expr);
			break;
		case T_IntoClause:
			/* use the contained RangeVar's location --- close enough */
			loc = exprLocation((Node *) ((const IntoClause *) expr)->rel);
			break;
		case T_List:
			{
				/* report location of first list member that has a location */
				ListCell   *lc;

				loc = -1;		/* just to suppress compiler warning */
				foreach(lc, (const List *) expr)
				{
					loc = exprLocation((Node *) lfirst(lc));
					if (loc >= 0)
						break;
				}
			}
			break;
		case T_A_Expr:
			{
				const A_Expr *aexpr = (const A_Expr *) expr;

				/* use leftmost of operator or left operand (if any) */
				/* we assume right operand can't be to left of operator */
				loc = leftmostLoc(aexpr->location,
								  exprLocation(aexpr->lexpr));
			}
			break;
		case T_ColumnRef:
			loc = ((const ColumnRef *) expr)->location;
			break;
		case T_ParamRef:
			loc = ((const ParamRef *) expr)->location;
			break;
		case T_A_Const:
			loc = ((const A_Const *) expr)->location;
			break;
		case T_FuncCall:
			{
				const FuncCall *fc = (const FuncCall *) expr;

				/* consider both function name and leftmost arg */
				/* (we assume any ORDER BY nodes must be to right of name) */
				loc = leftmostLoc(fc->location,
								  exprLocation((Node *) fc->args));
			}
			break;
		case T_A_ArrayExpr:
			/* the location points at ARRAY or [, which must be leftmost */
			loc = ((const A_ArrayExpr *) expr)->location;
			break;
		case T_ResTarget:
			/* we need not examine the contained expression (if any) */
			loc = ((const ResTarget *) expr)->location;
			break;
		case T_MultiAssignRef:
			loc = exprLocation(((const MultiAssignRef *) expr)->source);
			break;
		case T_TypeCast:
			{
				const TypeCast *tc = (const TypeCast *) expr;

				/*
				 * This could represent CAST(), ::, or TypeName 'literal', so
				 * any of the components might be leftmost.
				 */
				loc = exprLocation(tc->arg);
				loc = leftmostLoc(loc, tc->typeName->location);
				loc = leftmostLoc(loc, tc->location);
			}
			break;
		case T_CollateClause:
			/* just use argument's location */
			loc = exprLocation(((const CollateClause *) expr)->arg);
			break;
		case T_SortBy:
			/* just use argument's location (ignore operator, if any) */
			loc = exprLocation(((const SortBy *) expr)->node);
			break;
		case T_WindowDef:
			loc = ((const WindowDef *) expr)->location;
			break;
		case T_RangeTableSample:
			loc = ((const RangeTableSample *) expr)->location;
			break;
		case T_TypeName:
			loc = ((const TypeName *) expr)->location;
			break;
		case T_ColumnDef:
			loc = ((const ColumnDef *) expr)->location;
			break;
		case T_Constraint:
			loc = ((const Constraint *) expr)->location;
			break;
		case T_FunctionParameter:
			/* just use typename's location */
			loc = exprLocation((Node *) ((const FunctionParameter *) expr)->argType);
			break;
		case T_XmlSerialize:
			/* XMLSERIALIZE keyword should always be the first thing */
			loc = ((const XmlSerialize *) expr)->location;
			break;
		case T_GroupingSet:
			loc = ((const GroupingSet *) expr)->location;
			break;
		case T_WithClause:
			loc = ((const WithClause *) expr)->location;
			break;
		case T_InferClause:
			loc = ((const InferClause *) expr)->location;
			break;
		case T_OnConflictClause:
			loc = ((const OnConflictClause *) expr)->location;
			break;
		case T_CommonTableExpr:
			loc = ((const CommonTableExpr *) expr)->location;
			break;
		case T_PlaceHolderVar:
			/* just use argument's location */
			loc = exprLocation((Node *) ((const PlaceHolderVar *) expr)->phexpr);
			break;
		case T_InferenceElem:
			/* just use nested expr's location */
			loc = exprLocation((Node *) ((const InferenceElem *) expr)->expr);
			break;
		default:
			/* for any other node type it's just unknown... */
			loc = -1;
			break;
	}
	return loc;
}

/*
 * leftmostLoc - support for exprLocation
 *
 * Take the minimum of two parse location values, but ignore unknowns
 */
static int
leftmostLoc(int loc1, int loc2)
{
	if (loc1 < 0)
		return loc2;
	else if (loc2 < 0)
		return loc1;
	else
		return Min(loc1, loc2);
}


/*
 * Standard expression-tree walking support
 *
 * We used to have near-duplicate code in many different routines that
 * understood how to recurse through an expression node tree.  That was
 * a pain to maintain, and we frequently had bugs due to some particular
 * routine neglecting to support a particular node type.  In most cases,
 * these routines only actually care about certain node types, and don't
 * care about other types except insofar as they have to recurse through
 * non-primitive node types.  Therefore, we now provide generic tree-walking
 * logic to consolidate the redundant "boilerplate" code.  There are
 * two versions: expression_tree_walker() and expression_tree_mutator().
 */

/*
 * expression_tree_walker() is designed to support routines that traverse
 * a tree in a read-only fashion (although it will also work for routines
 * that modify nodes in-place but never add/delete/replace nodes).
 * A walker routine should look like this:
 *
 * bool my_walker (Node *node, my_struct *context)
 * {
 *		if (node == NULL)
 *			return false;
 *		// check for nodes that special work is required for, eg:
 *		if (IsA(node, Var))
 *		{
 *			... do special actions for Var nodes
 *		}
 *		else if (IsA(node, ...))
 *		{
 *			... do special actions for other node types
 *		}
 *		// for any node type not specially processed, do:
 *		return expression_tree_walker(node, my_walker, (void *) context);
 * }
 *
 * The "context" argument points to a struct that holds whatever context
 * information the walker routine needs --- it can be used to return data
 * gathered by the walker, too.  This argument is not touched by
 * expression_tree_walker, but it is passed down to recursive sub-invocations
 * of my_walker.  The tree walk is started from a setup routine that
 * fills in the appropriate context struct, calls my_walker with the top-level
 * node of the tree, and then examines the results.
 *
 * The walker routine should return "false" to continue the tree walk, or
 * "true" to abort the walk and immediately return "true" to the top-level
 * caller.  This can be used to short-circuit the traversal if the walker
 * has found what it came for.  "false" is returned to the top-level caller
 * iff no invocation of the walker returned "true".
 *
 * The node types handled by expression_tree_walker include all those
 * normally found in target lists and qualifier clauses during the planning
 * stage.  In particular, it handles List nodes since a cnf-ified qual clause
 * will have List structure at the top level, and it handles TargetEntry nodes
 * so that a scan of a target list can be handled without additional code.
 * Also, RangeTblRef, FromExpr, JoinExpr, and SetOperationStmt nodes are
 * handled, so that query jointrees and setOperation trees can be processed
 * without additional code.
 *
 * expression_tree_walker will handle SubLink nodes by recursing normally
 * into the "testexpr" subtree (which is an expression belonging to the outer
 * plan).  It will also call the walker on the sub-Query node; however, when
 * expression_tree_walker itself is called on a Query node, it does nothing
 * and returns "false".  The net effect is that unless the walker does
 * something special at a Query node, sub-selects will not be visited during
 * an expression tree walk. This is exactly the behavior wanted in many cases
 * --- and for those walkers that do want to recurse into sub-selects, special
 * behavior is typically needed anyway at the entry to a sub-select (such as
 * incrementing a depth counter). A walker that wants to examine sub-selects
 * should include code along the lines of:
 *
 *		if (IsA(node, Query))
 *		{
 *			adjust context for subquery;
 *			result = query_tree_walker((Query *) node, my_walker, context,
 *									   0); // adjust flags as needed
 *			restore context if needed;
 *			return result;
 *		}
 *
 * query_tree_walker is a convenience routine (see below) that calls the
 * walker on all the expression subtrees of the given Query node.
 *
 * expression_tree_walker will handle SubPlan nodes by recursing normally
 * into the "testexpr" and the "args" list (which are expressions belonging to
 * the outer plan).  It will not touch the completed subplan, however.  Since
 * there is no link to the original Query, it is not possible to recurse into
 * subselects of an already-planned expression tree.  This is OK for current
 * uses, but may need to be revisited in future.
 */



/*
 * query_tree_walker --- initiate a walk of a Query's expressions
 *
 * This routine exists just to reduce the number of places that need to know
 * where all the expression subtrees of a Query are.  Note it can be used
 * for starting a walk at top level of a Query regardless of whether the
 * walker intends to descend into subqueries.  It is also useful for
 * descending into subqueries within a walker.
 *
 * Some callers want to suppress visitation of certain items in the sub-Query,
 * typically because they need to process them specially, or don't actually
 * want to recurse into subqueries.  This is supported by the flags argument,
 * which is the bitwise OR of flag values to suppress visitation of
 * indicated items.  (More flag bits may be added as needed.)
 */


/*
 * range_table_walker is just the part of query_tree_walker that scans
 * a query's rangetable.  This is split out since it can be useful on
 * its own.
 */



/*
 * expression_tree_mutator() is designed to support routines that make a
 * modified copy of an expression tree, with some nodes being added,
 * removed, or replaced by new subtrees.  The original tree is (normally)
 * not changed.  Each recursion level is responsible for returning a copy of
 * (or appropriately modified substitute for) the subtree it is handed.
 * A mutator routine should look like this:
 *
 * Node * my_mutator (Node *node, my_struct *context)
 * {
 *		if (node == NULL)
 *			return NULL;
 *		// check for nodes that special work is required for, eg:
 *		if (IsA(node, Var))
 *		{
 *			... create and return modified copy of Var node
 *		}
 *		else if (IsA(node, ...))
 *		{
 *			... do special transformations of other node types
 *		}
 *		// for any node type not specially processed, do:
 *		return expression_tree_mutator(node, my_mutator, (void *) context);
 * }
 *
 * The "context" argument points to a struct that holds whatever context
 * information the mutator routine needs --- it can be used to return extra
 * data gathered by the mutator, too.  This argument is not touched by
 * expression_tree_mutator, but it is passed down to recursive sub-invocations
 * of my_mutator.  The tree walk is started from a setup routine that
 * fills in the appropriate context struct, calls my_mutator with the
 * top-level node of the tree, and does any required post-processing.
 *
 * Each level of recursion must return an appropriately modified Node.
 * If expression_tree_mutator() is called, it will make an exact copy
 * of the given Node, but invoke my_mutator() to copy the sub-node(s)
 * of that Node.  In this way, my_mutator() has full control over the
 * copying process but need not directly deal with expression trees
 * that it has no interest in.
 *
 * Just as for expression_tree_walker, the node types handled by
 * expression_tree_mutator include all those normally found in target lists
 * and qualifier clauses during the planning stage.
 *
 * expression_tree_mutator will handle SubLink nodes by recursing normally
 * into the "testexpr" subtree (which is an expression belonging to the outer
 * plan).  It will also call the mutator on the sub-Query node; however, when
 * expression_tree_mutator itself is called on a Query node, it does nothing
 * and returns the unmodified Query node.  The net effect is that unless the
 * mutator does something special at a Query node, sub-selects will not be
 * visited or modified; the original sub-select will be linked to by the new
 * SubLink node.  Mutators that want to descend into sub-selects will usually
 * do so by recognizing Query nodes and calling query_tree_mutator (below).
 *
 * expression_tree_mutator will handle a SubPlan node by recursing into the
 * "testexpr" and the "args" list (which belong to the outer plan), but it
 * will simply copy the link to the inner plan, since that's typically what
 * expression tree mutators want.  A mutator that wants to modify the subplan
 * can force appropriate behavior by recognizing SubPlan expression nodes
 * and doing the right thing.
 */

#define FLATCOPY(newnode, node, nodetype)  \
	( (newnode) = (nodetype *) palloc(sizeof(nodetype)), \
	  memcpy((newnode), (node), sizeof(nodetype)) )
#define CHECKFLATCOPY(newnode, node, nodetype)	\
	( AssertMacro(IsA((node), nodetype)), \
	  (newnode) = (nodetype *) palloc(sizeof(nodetype)), \
	  memcpy((newnode), (node), sizeof(nodetype)) )
#define MUTATE(newfield, oldfield, fieldtype)  \
		( (newfield) = (fieldtype) mutator((Node *) (oldfield), context) )


/*
 * query_tree_mutator --- initiate modification of a Query's expressions
 *
 * This routine exists just to reduce the number of places that need to know
 * where all the expression subtrees of a Query are.  Note it can be used
 * for starting a walk at top level of a Query regardless of whether the
 * mutator intends to descend into subqueries.  It is also useful for
 * descending into subqueries within a mutator.
 *
 * Some callers want to suppress mutating of certain items in the Query,
 * typically because they need to process them specially, or don't actually
 * want to recurse into subqueries.  This is supported by the flags argument,
 * which is the bitwise OR of flag values to suppress mutating of
 * indicated items.  (More flag bits may be added as needed.)
 *
 * Normally the Query node itself is copied, but some callers want it to be
 * modified in-place; they must pass QTW_DONT_COPY_QUERY in flags.  All
 * modified substructure is safely copied in any case.
 */


/*
 * range_table_mutator is just the part of query_tree_mutator that processes
 * a query's rangetable.  This is split out since it can be useful on
 * its own.
 */


/*
 * query_or_expression_tree_walker --- hybrid form
 *
 * This routine will invoke query_tree_walker if called on a Query node,
 * else will invoke the walker directly.  This is a useful way of starting
 * the recursion when the walker's normal change of state is not appropriate
 * for the outermost Query node.
 */


/*
 * query_or_expression_tree_mutator --- hybrid form
 *
 * This routine will invoke query_tree_mutator if called on a Query node,
 * else will invoke the mutator directly.  This is a useful way of starting
 * the recursion when the mutator's normal change of state is not appropriate
 * for the outermost Query node.
 */



/*
 * raw_expression_tree_walker --- walk raw parse trees
 *
 * This has exactly the same API as expression_tree_walker, but instead of
 * walking post-analysis parse trees, it knows how to walk the node types
 * found in raw grammar output.  (There is not currently any need for a
 * combined walker, so we keep them separate in the name of efficiency.)
 * Unlike expression_tree_walker, there is no special rule about query
 * boundaries: we descend to everything that's possibly interesting.
 *
 * Currently, the node type coverage extends to SelectStmt and everything
 * that could appear under it, but not other statement types.
 */
bool
raw_expression_tree_walker(Node *node,
						   bool (*walker) (),
						   void *context)
{
	ListCell   *temp;

	/*
	 * The walker has already visited the current node, and so we need only
	 * recurse into any sub-nodes it has.
	 */
	if (node == NULL)
		return false;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_SetToDefault:
		case T_CurrentOfExpr:
		case T_Integer:
		case T_Float:
		case T_String:
		case T_BitString:
		case T_Null:
		case T_ParamRef:
		case T_A_Const:
		case T_A_Star:
			/* primitive node types with no subnodes */
			break;
		case T_Alias:
			/* we assume the colnames list isn't interesting */
			break;
		case T_RangeVar:
			return walker(((RangeVar *) node)->alias, context);
		case T_GroupingFunc:
			return walker(((GroupingFunc *) node)->args, context);
		case T_SubLink:
			{
				SubLink    *sublink = (SubLink *) node;

				if (walker(sublink->testexpr, context))
					return true;
				/* we assume the operName is not interesting */
				if (walker(sublink->subselect, context))
					return true;
			}
			break;
		case T_CaseExpr:
			{
				CaseExpr   *caseexpr = (CaseExpr *) node;

				if (walker(caseexpr->arg, context))
					return true;
				/* we assume walker doesn't care about CaseWhens, either */
				foreach(temp, caseexpr->args)
				{
					CaseWhen   *when = (CaseWhen *) lfirst(temp);

					Assert(IsA(when, CaseWhen));
					if (walker(when->expr, context))
						return true;
					if (walker(when->result, context))
						return true;
				}
				if (walker(caseexpr->defresult, context))
					return true;
			}
			break;
		case T_RowExpr:
			/* Assume colnames isn't interesting */
			return walker(((RowExpr *) node)->args, context);
		case T_CoalesceExpr:
			return walker(((CoalesceExpr *) node)->args, context);
		case T_MinMaxExpr:
			return walker(((MinMaxExpr *) node)->args, context);
		case T_XmlExpr:
			{
				XmlExpr    *xexpr = (XmlExpr *) node;

				if (walker(xexpr->named_args, context))
					return true;
				/* we assume walker doesn't care about arg_names */
				if (walker(xexpr->args, context))
					return true;
			}
			break;
		case T_NullTest:
			return walker(((NullTest *) node)->arg, context);
		case T_BooleanTest:
			return walker(((BooleanTest *) node)->arg, context);
		case T_JoinExpr:
			{
				JoinExpr   *join = (JoinExpr *) node;

				if (walker(join->larg, context))
					return true;
				if (walker(join->rarg, context))
					return true;
				if (walker(join->quals, context))
					return true;
				if (walker(join->alias, context))
					return true;
				/* using list is deemed uninteresting */
			}
			break;
		case T_IntoClause:
			{
				IntoClause *into = (IntoClause *) node;

				if (walker(into->rel, context))
					return true;
				/* colNames, options are deemed uninteresting */
				/* viewQuery should be null in raw parsetree, but check it */
				if (walker(into->viewQuery, context))
					return true;
			}
			break;
		case T_List:
			foreach(temp, (List *) node)
			{
				if (walker((Node *) lfirst(temp), context))
					return true;
			}
			break;
		case T_InsertStmt:
			{
				InsertStmt *stmt = (InsertStmt *) node;

				if (walker(stmt->relation, context))
					return true;
				if (walker(stmt->cols, context))
					return true;
				if (walker(stmt->selectStmt, context))
					return true;
				if (walker(stmt->onConflictClause, context))
					return true;
				if (walker(stmt->returningList, context))
					return true;
				if (walker(stmt->withClause, context))
					return true;
			}
			break;
		case T_DeleteStmt:
			{
				DeleteStmt *stmt = (DeleteStmt *) node;

				if (walker(stmt->relation, context))
					return true;
				if (walker(stmt->usingClause, context))
					return true;
				if (walker(stmt->whereClause, context))
					return true;
				if (walker(stmt->returningList, context))
					return true;
				if (walker(stmt->withClause, context))
					return true;
			}
			break;
		case T_UpdateStmt:
			{
				UpdateStmt *stmt = (UpdateStmt *) node;

				if (walker(stmt->relation, context))
					return true;
				if (walker(stmt->targetList, context))
					return true;
				if (walker(stmt->whereClause, context))
					return true;
				if (walker(stmt->fromClause, context))
					return true;
				if (walker(stmt->returningList, context))
					return true;
				if (walker(stmt->withClause, context))
					return true;
			}
			break;
		case T_SelectStmt:
			{
				SelectStmt *stmt = (SelectStmt *) node;

				if (walker(stmt->distinctClause, context))
					return true;
				if (walker(stmt->intoClause, context))
					return true;
				if (walker(stmt->targetList, context))
					return true;
				if (walker(stmt->fromClause, context))
					return true;
				if (walker(stmt->whereClause, context))
					return true;
				if (walker(stmt->groupClause, context))
					return true;
				if (walker(stmt->havingClause, context))
					return true;
				if (walker(stmt->windowClause, context))
					return true;
				if (walker(stmt->valuesLists, context))
					return true;
				if (walker(stmt->sortClause, context))
					return true;
				if (walker(stmt->limitOffset, context))
					return true;
				if (walker(stmt->limitCount, context))
					return true;
				if (walker(stmt->lockingClause, context))
					return true;
				if (walker(stmt->withClause, context))
					return true;
				if (walker(stmt->larg, context))
					return true;
				if (walker(stmt->rarg, context))
					return true;
			}
			break;
		case T_A_Expr:
			{
				A_Expr	   *expr = (A_Expr *) node;

				if (walker(expr->lexpr, context))
					return true;
				if (walker(expr->rexpr, context))
					return true;
				/* operator name is deemed uninteresting */
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *expr = (BoolExpr *) node;

				if (walker(expr->args, context))
					return true;
			}
			break;
		case T_ColumnRef:
			/* we assume the fields contain nothing interesting */
			break;
		case T_FuncCall:
			{
				FuncCall   *fcall = (FuncCall *) node;

				if (walker(fcall->args, context))
					return true;
				if (walker(fcall->agg_order, context))
					return true;
				if (walker(fcall->agg_filter, context))
					return true;
				if (walker(fcall->over, context))
					return true;
				/* function name is deemed uninteresting */
			}
			break;
		case T_NamedArgExpr:
			return walker(((NamedArgExpr *) node)->arg, context);
		case T_A_Indices:
			{
				A_Indices  *indices = (A_Indices *) node;

				if (walker(indices->lidx, context))
					return true;
				if (walker(indices->uidx, context))
					return true;
			}
			break;
		case T_A_Indirection:
			{
				A_Indirection *indir = (A_Indirection *) node;

				if (walker(indir->arg, context))
					return true;
				if (walker(indir->indirection, context))
					return true;
			}
			break;
		case T_A_ArrayExpr:
			return walker(((A_ArrayExpr *) node)->elements, context);
		case T_ResTarget:
			{
				ResTarget  *rt = (ResTarget *) node;

				if (walker(rt->indirection, context))
					return true;
				if (walker(rt->val, context))
					return true;
			}
			break;
		case T_MultiAssignRef:
			return walker(((MultiAssignRef *) node)->source, context);
		case T_TypeCast:
			{
				TypeCast   *tc = (TypeCast *) node;

				if (walker(tc->arg, context))
					return true;
				if (walker(tc->typeName, context))
					return true;
			}
			break;
		case T_CollateClause:
			return walker(((CollateClause *) node)->arg, context);
		case T_SortBy:
			return walker(((SortBy *) node)->node, context);
		case T_WindowDef:
			{
				WindowDef  *wd = (WindowDef *) node;

				if (walker(wd->partitionClause, context))
					return true;
				if (walker(wd->orderClause, context))
					return true;
				if (walker(wd->startOffset, context))
					return true;
				if (walker(wd->endOffset, context))
					return true;
			}
			break;
		case T_RangeSubselect:
			{
				RangeSubselect *rs = (RangeSubselect *) node;

				if (walker(rs->subquery, context))
					return true;
				if (walker(rs->alias, context))
					return true;
			}
			break;
		case T_RangeFunction:
			{
				RangeFunction *rf = (RangeFunction *) node;

				if (walker(rf->functions, context))
					return true;
				if (walker(rf->alias, context))
					return true;
				if (walker(rf->coldeflist, context))
					return true;
			}
			break;
		case T_RangeTableSample:
			{
				RangeTableSample *rts = (RangeTableSample *) node;

				if (walker(rts->relation, context))
					return true;
				/* method name is deemed uninteresting */
				if (walker(rts->args, context))
					return true;
				if (walker(rts->repeatable, context))
					return true;
			}
			break;
		case T_TypeName:
			{
				TypeName   *tn = (TypeName *) node;

				if (walker(tn->typmods, context))
					return true;
				if (walker(tn->arrayBounds, context))
					return true;
				/* type name itself is deemed uninteresting */
			}
			break;
		case T_ColumnDef:
			{
				ColumnDef  *coldef = (ColumnDef *) node;

				if (walker(coldef->typeName, context))
					return true;
				if (walker(coldef->raw_default, context))
					return true;
				if (walker(coldef->collClause, context))
					return true;
				/* for now, constraints are ignored */
			}
			break;
		case T_GroupingSet:
			return walker(((GroupingSet *) node)->content, context);
		case T_LockingClause:
			return walker(((LockingClause *) node)->lockedRels, context);
		case T_XmlSerialize:
			{
				XmlSerialize *xs = (XmlSerialize *) node;

				if (walker(xs->expr, context))
					return true;
				if (walker(xs->typeName, context))
					return true;
			}
			break;
		case T_WithClause:
			return walker(((WithClause *) node)->ctes, context);
		case T_InferClause:
			{
				InferClause *stmt = (InferClause *) node;

				if (walker(stmt->indexElems, context))
					return true;
				if (walker(stmt->whereClause, context))
					return true;
			}
			break;
		case T_OnConflictClause:
			{
				OnConflictClause *stmt = (OnConflictClause *) node;

				if (walker(stmt->infer, context))
					return true;
				if (walker(stmt->targetList, context))
					return true;
				if (walker(stmt->whereClause, context))
					return true;
			}
			break;
		case T_CommonTableExpr:
			return walker(((CommonTableExpr *) node)->ctequery, context);
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(node));
			break;
	}
	return false;
}
