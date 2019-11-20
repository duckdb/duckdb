/*-------------------------------------------------------------------------
 *
 * parse_node.h
 *		Internal definitions for parser
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_node.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include "nodes/parsenodes.hpp"


/*
 * Expression kinds distinguished by transformExpr().  Many of these are not
 * semantically distinct so far as expression transformation goes; rather,
 * we distinguish them so that context-specific error messages can be printed.
 *
 * Note: PG_EXPR_KIND_OTHER is not used in the core code, but is left for use
 * by extension code that might need to call transformExpr().  The core code
 * will not enforce any context-driven restrictions on PG_EXPR_KIND_OTHER
 * expressions, so the caller would have to check for sub-selects, aggregates,
 * window functions, SRFs, etc if those need to be disallowed.
 */
typedef enum PGParseExprKind
{
	PG_EXPR_KIND_NONE = 0,			/* "not in an expression" */
	PG_EXPR_KIND_OTHER,			/* reserved for extensions */
	PG_EXPR_KIND_JOIN_ON,			/* JOIN ON */
	PG_EXPR_KIND_JOIN_USING,		/* JOIN USING */
	PG_EXPR_KIND_FROM_SUBSELECT,	/* sub-SELECT in FROM clause */
	PG_EXPR_KIND_FROM_FUNCTION,	/* function in FROM clause */
	PG_EXPR_KIND_WHERE,			/* WHERE */
	PG_EXPR_KIND_HAVING,			/* HAVING */
	PG_EXPR_KIND_FILTER,			/* FILTER */
	PG_EXPR_KIND_WINDOW_PARTITION, /* window definition PARTITION BY */
	PG_EXPR_KIND_WINDOW_ORDER,		/* window definition ORDER BY */
	PG_EXPR_KIND_WINDOW_FRAME_RANGE,	/* window frame clause with RANGE */
	PG_EXPR_KIND_WINDOW_FRAME_ROWS,	/* window frame clause with ROWS */
	PG_EXPR_KIND_SELECT_TARGET,	/* SELECT target list item */
	PG_EXPR_KIND_INSERT_TARGET,	/* INSERT target list item */
	PG_EXPR_KIND_UPDATE_SOURCE,	/* UPDATE assignment source item */
	PG_EXPR_KIND_UPDATE_TARGET,	/* UPDATE assignment target item */
	PG_EXPR_KIND_GROUP_BY,			/* GROUP BY */
	PG_EXPR_KIND_ORDER_BY,			/* ORDER BY */
	PG_EXPR_KIND_DISTINCT_ON,		/* DISTINCT ON */
	PG_EXPR_KIND_LIMIT,			/* LIMIT */
	PG_EXPR_KIND_OFFSET,			/* OFFSET */
	PG_EXPR_KIND_RETURNING,		/* RETURNING */
	PG_EXPR_KIND_VALUES,			/* VALUES */
	PG_EXPR_KIND_VALUES_SINGLE,	/* single-row VALUES (in INSERT only) */
	PG_EXPR_KIND_CHECK_CONSTRAINT, /* CHECK constraint for a table */
	PG_EXPR_KIND_DOMAIN_CHECK,		/* CHECK constraint for a domain */
	PG_EXPR_KIND_COLUMN_DEFAULT,	/* default value for a table column */
	PG_EXPR_KIND_FUNCTION_DEFAULT, /* default parameter value for function */
	PG_EXPR_KIND_INDEX_EXPRESSION, /* index expression */
	PG_EXPR_KIND_INDEX_PREDICATE,	/* index predicate */
	PG_EXPR_KIND_ALTER_COL_TRANSFORM,	/* transform expr in ALTER COLUMN TYPE */
	PG_EXPR_KIND_EXECUTE_PARAMETER,	/* parameter value in EXECUTE */
	PG_EXPR_KIND_TRIGGER_WHEN,		/* WHEN condition in CREATE TRIGGER */
	PG_EXPR_KIND_POLICY,			/* USING or WITH CHECK expr in policy */
	EXPR_KIND_PARTITION_EXPRESSION	/* PARTITION BY expression */
} PGParseExprKind;


/*
 * Function signatures for parser hooks
 */
typedef struct PGParseState ParseState;

typedef PGNode *(*PreParseColumnRefHook) (PGParseState *pstate, PGColumnRef *cref);
typedef PGNode *(*PostParseColumnRefHook) (PGParseState *pstate, PGColumnRef *cref, PGNode *var);
typedef PGNode *(*ParseParamRefHook) (PGParseState *pstate, PGParamRef *pref);
typedef PGNode *(*CoerceParamHook) (PGParseState *pstate, PGParam *param,
								  PGOid targetTypeId, int32_t targetTypeMod,
								  int location);


/*
 * State information used during parse analysis
 *
 * parentParseState: NULL in a top-level ParseState.  When parsing a subquery,
 * links to current parse state of outer query.
 *
 * p_sourcetext: source string that generated the raw parsetree being
 * analyzed, or NULL if not available.  (The string is used only to
 * generate cursor positions in error messages: we need it to convert
 * byte-wise locations in parse structures to character-wise cursor
 * positions.)
 *
 * p_rtable: list of RTEs that will become the rangetable of the query.
 * Note that neither relname nor refname of these entries are necessarily
 * unique; searching the rtable by name is a bad idea.
 *
 * p_joinexprs: list of PGJoinExpr nodes associated with p_rtable entries.
 * This is one-for-one with p_rtable, but contains NULLs for non-join
 * RTEs, and may be shorter than p_rtable if the last RTE(s) aren't joins.
 *
 * p_joinlist: list of join items (PGRangeTblRef and PGJoinExpr nodes) that
 * will become the fromlist of the query's top-level PGFromExpr node.
 *
 * p_namespace: list of ParseNamespaceItems that represents the current
 * namespace for table and column lookup.  (The RTEs listed here may be just
 * a subset of the whole rtable.  See PGParseNamespaceItem comments below.)
 *
 * p_lateral_active: true if we are currently parsing a LATERAL subexpression
 * of this parse level.  This makes p_lateral_only namespace items visible,
 * whereas they are not visible when p_lateral_active is false.
 *
 * p_ctenamespace: list of CommonTableExprs (WITH items) that are visible
 * at the moment.  This is entirely different from p_namespace because a CTE
 * is not an RTE, rather "visibility" means you could make an RTE from it.
 *
 * p_future_ctes: list of CommonTableExprs (WITH items) that are not yet
 * visible due to scope rules.  This is used to help improve error messages.
 *
 * p_parent_cte: PGCommonTableExpr that immediately contains the current query,
 * if any.
 *
 * p_target_relation: target relation, if query is INSERT, UPDATE, or DELETE.
 *
 * p_target_rangetblentry: target relation's entry in the rtable list.
 *
 * p_is_insert: true to process assignment expressions like INSERT, false
 * to process them like UPDATE.  (Note this can change intra-statement, for
 * cases like INSERT ON CONFLICT UPDATE.)
 *
 * p_windowdefs: list of WindowDefs representing WINDOW and OVER clauses.
 * We collect these while transforming expressions and then transform them
 * afterwards (so that any resjunk tlist items needed for the sort/group
 * clauses end up at the end of the query tlist).  A WindowDef's location in
 * this list, counting from 1, is the winref number to use to reference it.
 *
 * p_expr_kind: kind of expression we're currently parsing, as per enum above;
 * PG_EXPR_KIND_NONE when not in an expression.
 *
 * p_next_resno: next TargetEntry.resno to assign, starting from 1.
 *
 * p_multiassign_exprs: partially-processed PGMultiAssignRef source expressions.
 *
 * p_locking_clause: query's FOR UPDATE/FOR SHARE clause, if any.
 *
 * p_locked_from_parent: true if parent query level applies FOR UPDATE/SHARE
 * to this subquery as a whole.
 *
 * p_resolve_unknowns: resolve unknown-type SELECT output columns as type TEXT
 * (this is true by default).
 *
 * p_hasAggs, p_hasWindowFuncs, etc: true if we've found any of the indicated
 * constructs in the query.
 *
 * p_last_srf: the set-returning PGFuncExpr or PGOpExpr most recently found in
 * the query, or NULL if none.
 *
 * p_pre_columnref_hook, etc: optional parser hook functions for modifying the
 * interpretation of ColumnRefs and ParamRefs.
 *
 * p_ref_hook_state: passthrough state for the parser hook functions.
 */
struct PGParseState
{
	struct PGParseState *parentParseState;	/* stack link */
	const char *p_sourcetext;	/* source text, or NULL if not available */
	PGList	   *p_rtable;		/* range table so far */
	PGList	   *p_joinexprs;	/* JoinExprs for PG_RTE_JOIN p_rtable entries */
	PGList	   *p_joinlist;		/* join items so far (will become PGFromExpr
								 * node's fromlist) */
	PGList	   *p_namespace;	/* currently-referenceable RTEs (PGList of
								 * PGParseNamespaceItem) */
	bool		p_lateral_active;	/* p_lateral_only items visible? */
	PGList	   *p_ctenamespace; /* current namespace for common table exprs */
	PGList	   *p_future_ctes;	/* common table exprs not yet in namespace */
	PGCommonTableExpr *p_parent_cte;	/* this query's containing CTE */
	void*	p_target_relation;	/* INSERT/UPDATE/DELETE target rel */
	PGRangeTblEntry *p_target_rangetblentry;	/* target rel's RTE */
	bool		p_is_insert;	/* process assignment like INSERT not UPDATE */
	PGList	   *p_windowdefs;	/* raw representations of window clauses */
	PGParseExprKind p_expr_kind;	/* what kind of expression we're parsing */
	int			p_next_resno;	/* next targetlist resno to assign */
	PGList	   *p_multiassign_exprs;	/* junk tlist entries for multiassign */
	PGList	   *p_locking_clause;	/* raw FOR UPDATE/FOR SHARE info */
	bool		p_locked_from_parent;	/* parent has marked this subquery
										 * with FOR UPDATE/FOR SHARE */
	bool		p_resolve_unknowns; /* resolve unknown-type SELECT outputs as
									 * type text */

	void *p_queryEnv;	/* curr env, incl refs to enclosing env */

	/* Flags telling about things found in the query: */
	bool		p_hasAggs;
	bool		p_hasWindowFuncs;
	bool		p_hasTargetSRFs;
	bool		p_hasSubLinks;
	bool		p_hasModifyingCTE;

	PGNode	   *p_last_srf;		/* most recent set-returning func/op found */

	/*
	 * Optional hook functions for parser callbacks.  These are null unless
	 * set up by the caller of make_parsestate.
	 */
	PreParseColumnRefHook p_pre_columnref_hook;
	PostParseColumnRefHook p_post_columnref_hook;
	ParseParamRefHook p_paramref_hook;
	CoerceParamHook p_coerce_param_hook;
	void	   *p_ref_hook_state;	/* common passthrough link for above */
};

/*
 * An element of a namespace list.
 *
 * Namespace items with p_rel_visible set define which RTEs are accessible by
 * qualified names, while those with p_cols_visible set define which RTEs are
 * accessible by unqualified names.  These sets are different because a JOIN
 * without an alias does not hide the contained tables (so they must be
 * visible for qualified references) but it does hide their columns
 * (unqualified references to the columns refer to the JOIN, not the member
 * tables, so we must not complain that such a reference is ambiguous).
 * Various special RTEs such as NEW/OLD for rules may also appear with only
 * one flag set.
 *
 * While processing the FROM clause, namespace items may appear with
 * p_lateral_only set, meaning they are visible only to LATERAL
 * subexpressions.  (The pstate's p_lateral_active flag tells whether we are
 * inside such a subexpression at the moment.)	If p_lateral_ok is not set,
 * it's an error to actually use such a namespace item.  One might think it
 * would be better to just exclude such items from visibility, but the wording
 * of SQL:2008 requires us to do it this way.  We also use p_lateral_ok to
 * forbid LATERAL references to an UPDATE/DELETE target table.
 *
 * At no time should a namespace list contain two entries that conflict
 * according to the rules in checkNameSpaceConflicts; but note that those
 * are more complicated than "must have different alias names", so in practice
 * code searching a namespace list has to check for ambiguous references.
 */
typedef struct PGParseNamespaceItem
{
	PGRangeTblEntry *p_rte;		/* The relation's rangetable entry */
	bool		p_rel_visible;	/* Relation name is visible? */
	bool		p_cols_visible; /* Column names visible as unqualified refs? */
	bool		p_lateral_only; /* Is only visible to LATERAL expressions? */
	bool		p_lateral_ok;	/* If so, does join type allow use? */
} PGParseNamespaceItem;

/* Support for parser_errposition_callback function */
typedef struct PGParseCallbackState
{
	PGParseState *pstate;
	int			location;
	void* errcallback;
} PGParseCallbackState;


extern PGParseState *make_parsestate(PGParseState *parentParseState);
extern void free_parsestate(PGParseState *pstate);
extern int	parser_errposition(PGParseState *pstate, int location);

extern void setup_parser_errposition_callback(PGParseCallbackState *pcbstate,
								  PGParseState *pstate, int location);
extern void cancel_parser_errposition_callback(PGParseCallbackState *pcbstate);

extern PGVar *make_var(PGParseState *pstate, PGRangeTblEntry *rte, int attrno,
		 int location);
extern PGOid	transformArrayType(PGOid *arrayType, int32_t *arrayTypmod);
extern PGArrayRef *transformArraySubscripts(PGParseState *pstate,
						 PGNode *arrayBase,
						 PGOid arrayType,
						 PGOid elementType,
						 int32_t arrayTypMod,
						 PGList *indirection,
						 PGNode *assignFrom);
extern PGConst *make_const(PGParseState *pstate, PGValue *value, int location);
