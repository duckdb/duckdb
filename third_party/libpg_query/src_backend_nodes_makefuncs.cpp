/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - makeDefElem
 * - makeTypeNameFromNameList
 * - makeDefElemExtended
 * - makeAlias
 * - makeSimpleAExpr
 * - makeGroupingSet
 * - makeTypeName
 * - makeFuncCall
 * - makeAExpr
 * - makeRangeVar
 * - makeBoolExpr
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * makefuncs.c
 *	  creator functions for primitive nodes. The functions here are for
 *	  the most frequently created nodes.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/makefuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "pg_functions.hpp"



#include "fmgr.hpp"
#include "nodes/makefuncs.hpp"
#include "nodes/nodeFuncs.hpp"

namespace duckdb_libpgquery {

/*
 * makeAExpr -
 *		makes an PGAExpr node
 */
PGAExpr *makeAExpr(PGAExpr_Kind kind, PGList *name, PGNode *lexpr, PGNode *rexpr, int location) {
	PGAExpr *a = makeNode(PGAExpr);

	a->kind = kind;
	a->name = name;
	a->lexpr = lexpr;
	a->rexpr = rexpr;
	a->location = location;
	return a;
}

/*
 * makeSimpleAExpr -
 *		As above, given a simple (unqualified) operator name
 */
PGAExpr *makeSimpleAExpr(PGAExpr_Kind kind, const char *name, PGNode *lexpr, PGNode *rexpr, int location) {
	PGAExpr *a = makeNode(PGAExpr);

	a->kind = kind;
	a->name = list_make1(makeString((char *)name));
	a->lexpr = lexpr;
	a->rexpr = rexpr;
	a->location = location;
	return a;
}

/*
 * makeVar -
 *	  creates a PGVar node
 */

/*
 * makeVarFromTargetEntry -
 *		convenience function to create a same-level PGVar node from a
 *		PGTargetEntry
 */

/*
 * makeWholeRowVar -
 *	  creates a PGVar node representing a whole row of the specified RTE
 *
 * A whole-row reference is a PGVar with varno set to the correct range
 * table entry, and varattno == 0 to signal that it references the whole
 * tuple.  (Use of zero here is unclean, since it could easily be confused
 * with error cases, but it's not worth changing now.)  The vartype indicates
 * a rowtype; either a named composite type, or RECORD.  This function
 * encapsulates the logic for determining the correct rowtype OID to use.
 *
 * If allowScalar is true, then for the case where the RTE is a single function
 * returning a non-composite result type, we produce a normal PGVar referencing
 * the function's result directly, instead of the single-column composite
 * value that the whole-row notation might otherwise suggest.
 */

/*
 * makeTargetEntry -
 *	  creates a PGTargetEntry node
 */

/*
 * flatCopyTargetEntry -
 *	  duplicate a PGTargetEntry, but don't copy substructure
 *
 * This is commonly used when we just want to modify the resno or substitute
 * a new expression.
 */

/*
 * makeFromExpr -
 *	  creates a PGFromExpr node
 */

/*
 * makeConst -
 *	  creates a PGConst node
 */

/*
 * makeNullConst -
 *	  creates a PGConst node representing a NULL of the specified type/typmod
 *
 * This is a convenience routine that just saves a lookup of the type's
 * storage properties.
 */

/*
 * makeBoolConst -
 *	  creates a PGConst node representing a boolean value (can be NULL too)
 */

/*
 * makeBoolExpr -
 *	  creates a PGBoolExpr node
 */
PGExpr *makeBoolExpr(PGBoolExprType boolop, PGList *args, int location) {
	PGBoolExpr *b = makeNode(PGBoolExpr);

	b->boolop = boolop;
	b->args = args;
	b->location = location;

	return (PGExpr *)b;
}

/*
 * makeAlias -
 *	  creates an PGAlias node
 *
 * NOTE: the given name is copied, but the colnames list (if any) isn't.
 */
PGAlias *makeAlias(const char *aliasname, PGList *colnames) {
	PGAlias *a = makeNode(PGAlias);

	a->aliasname = pstrdup(aliasname);
	a->colnames = colnames;

	return a;
}

/*
 * makeRelabelType -
 *	  creates a PGRelabelType node
 */

/*
 * makeRangeVar -
 *	  creates a PGRangeVar node (rather oversimplified case)
 */
PGRangeVar *makeRangeVar(char *schemaname, char *relname, int location) {
	PGRangeVar *r = makeNode(PGRangeVar);

	r->catalogname = NULL;
	r->schemaname = schemaname;
	r->relname = relname;
	r->inh = true;
	r->relpersistence = RELPERSISTENCE_PERMANENT;
	r->alias = NULL;
	r->location = location;
	r->sample = NULL;

	return r;
}

/*
 * makeTypeName -
 *	build a PGTypeName node for an unqualified name.
 *
 * typmod is defaulted, but can be changed later by caller.
 */
PGTypeName *makeTypeName(char *typnam) {
	return makeTypeNameFromNameList(list_make1(makeString(typnam)));
}

/*
 * makeTypeNameFromNameList -
 *	build a PGTypeName node for a String list representing a qualified name.
 *
 * typmod is defaulted, but can be changed later by caller.
 */
PGTypeName *makeTypeNameFromNameList(PGList *names) {
	PGTypeName *n = makeNode(PGTypeName);

	n->names = names;
	n->typmods = NIL;
	n->typemod = -1;
	n->location = -1;
	return n;
}

/*
 * makeTypeNameFromOid -
 *	build a PGTypeName node to represent a type already known by OID/typmod.
 */

/*
 * makeColumnDef -
 *	build a PGColumnDef node to represent a simple column definition.
 *
 * Type and collation are specified by OID.
 * Other properties are all basic to start with.
 */

/*
 * makeFuncExpr -
 *	build an expression tree representing a function call.
 *
 * The argument expressions must have been transformed already.
 */

/*
 * makeDefElem -
 *	build a PGDefElem node
 *
 * This is sufficient for the "typical" case with an unqualified option name
 * and no special action.
 */
PGDefElem *makeDefElem(const char *name, PGNode *arg, int location) {
	PGDefElem *res = makeNode(PGDefElem);

	res->defnamespace = NULL;
	res->defname = (char *)name;
	res->arg = arg;
	res->defaction = PG_DEFELEM_UNSPEC;
	res->location = location;

	return res;
}

/*
 * makeDefElemExtended -
 *	build a PGDefElem node with all fields available to be specified
 */
PGDefElem *makeDefElemExtended(const char *nameSpace, const char *name, PGNode *arg, PGDefElemAction defaction,
                               int location) {
	PGDefElem *res = makeNode(PGDefElem);

	res->defnamespace = (char *)nameSpace;
	res->defname = (char *)name;
	res->arg = arg;
	res->defaction = defaction;
	res->location = location;

	return res;
}

/*
 * makeFuncCall -
 *
 * Initialize a PGFuncCall struct with the information every caller must
 * supply.  Any non-default parameters have to be inserted by the caller.
 */
PGFuncCall *makeFuncCall(PGList *name, PGList *args, int location) {
	PGFuncCall *n = makeNode(PGFuncCall);

	n->funcname = name;
	n->args = args;
	n->agg_order = NIL;
	n->agg_filter = NULL;
	n->agg_within_group = false;
	n->agg_star = false;
	n->agg_distinct = false;
	n->func_variadic = false;
	n->over = NULL;
	n->location = location;
	return n;
}

/*
 * makeGroupingSet
 *
 */
PGGroupingSet *makeGroupingSet(GroupingSetKind kind, PGList *content, int location) {
	PGGroupingSet *n = makeNode(PGGroupingSet);

	n->kind = kind;
	n->content = content;
	n->location = location;
	return n;
}
}
