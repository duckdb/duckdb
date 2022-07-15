/*-------------------------------------------------------------------------
 *
 * parsenodes.h
 *	  definitions for parse tree nodes
 *
 * Many of the node types used in parsetrees include a "location" field.
 * This is a byte (not character) offset in the original source text, to be
 * used for positioning an error cursor when there is an error related to
 * the node.  Access to the original source text is needed to make use of
 * the location.  At the topmost (statement) level, we also provide a
 * statement length, likewise measured in bytes, for convenience in
 * identifying statement boundaries in multi-statement source strings.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/parsenodes.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include "nodes/bitmapset.hpp"
#include "nodes/lockoptions.hpp"
#include "nodes/primnodes.hpp"
#include "nodes/value.hpp"

namespace duckdb_libpgquery {

typedef enum PGOverridingKind {
	PG_OVERRIDING_NOT_SET = 0,
	PG_OVERRIDING_USER_VALUE,
	OVERRIDING_SYSTEM_VALUE
} PGOverridingKind;

/* Possible sources of a PGQuery */
typedef enum PGQuerySource {
	PG_QSRC_ORIGINAL,          /* original parsetree (explicit query) */
	PG_QSRC_PARSER,            /* added by parse analysis (now unused) */
	PG_QSRC_INSTEAD_RULE,      /* added by unconditional INSTEAD rule */
	PG_QSRC_QUAL_INSTEAD_RULE, /* added by conditional INSTEAD rule */
	QSRC_NON_INSTEAD_RULE      /* added by non-INSTEAD rule */
} PGQuerySource;

/* PGSort ordering options for ORDER BY and CREATE INDEX */
typedef enum PGSortByDir {
	PG_SORTBY_DEFAULT,
	PG_SORTBY_ASC,
	PG_SORTBY_DESC,
	SORTBY_USING /* not allowed in CREATE INDEX ... */
} PGSortByDir;

typedef enum PGSortByNulls { PG_SORTBY_NULLS_DEFAULT, PG_SORTBY_NULLS_FIRST, PG_SORTBY_NULLS_LAST } PGSortByNulls;

/*****************************************************************************
 *	PGQuery Tree
 *****************************************************************************/

/*
 * PGQuery -
 *	  Parse analysis turns all statements into a PGQuery tree
 *	  for further processing by the rewriter and planner.
 *
 *	  Utility statements (i.e. non-optimizable statements) have the
 *	  utilityStmt field set, and the rest of the PGQuery is mostly dummy.
 *
 *	  Planning converts a PGQuery tree into a PGPlan tree headed by a PGPlannedStmt
 *	  node --- the PGQuery structure is not used by the executor.
 */
typedef struct PGQuery {
	PGNodeTag type;

	PGCmdType commandType; /* select|insert|update|delete|utility */

	PGQuerySource querySource; /* where did I come from? */

	uint32_t queryId; /* query identifier (can be set by plugins) */

	bool canSetTag; /* do I set the command result tag? */

	PGNode *utilityStmt; /* non-null if commandType == PG_CMD_UTILITY */

	int resultRelation; /* rtable index of target relation for
								 * INSERT/UPDATE/DELETE; 0 for SELECT */

	bool hasAggs;         /* has aggregates in tlist or havingQual */
	bool hasWindowFuncs;  /* has window functions in tlist */
	bool hasTargetSRFs;   /* has set-returning functions in tlist */
	bool hasSubLinks;     /* has subquery PGSubLink */
	bool hasDistinctOn;   /* distinctClause is from DISTINCT ON */
	bool hasRecursive;    /* WITH RECURSIVE was specified */
	bool hasModifyingCTE; /* has INSERT/UPDATE/DELETE in WITH */
	bool hasForUpdate;    /* FOR [KEY] UPDATE/SHARE was specified */
	bool hasRowSecurity;  /* rewriter has applied some RLS policy */

	PGList *cteList; /* WITH list (of CommonTableExpr's) */

	PGList *rtable;       /* list of range table entries */
	PGFromExpr *jointree; /* table join tree (FROM and WHERE clauses) */

	PGList *targetList; /* target list (of PGTargetEntry) */

	PGOverridingKind override; /* OVERRIDING clause */

	PGOnConflictExpr *onConflict; /* ON CONFLICT DO [NOTHING | UPDATE] */

	PGList *returningList; /* return-values list (of PGTargetEntry) */

	PGList *groupClause; /* a list of SortGroupClause's */

	PGList *groupingSets; /* a list of GroupingSet's if present */

	PGNode *havingQual; /* qualifications applied to groups */

	PGList *windowClause; /* a list of WindowClause's */

	PGList *distinctClause; /* a list of SortGroupClause's */

	PGList *sortClause; /* a list of SortGroupClause's */

	PGNode *limitOffset; /* # of result tuples to skip (int8_t expr) */
	PGNode *limitCount;  /* # of result tuples to return (int8_t expr) */

	PGList *rowMarks; /* a list of RowMarkClause's */

	PGNode *setOperations; /* set-operation tree if this is top level of
								 * a UNION/INTERSECT/EXCEPT query */

	PGList *constraintDeps; /* a list of pg_constraint OIDs that the query
								 * depends on to be semantically valid */

	PGList *withCheckOptions; /* a list of WithCheckOption's, which are
									 * only added during rewrite and therefore
									 * are not written out as part of Query. */

	/*
	 * The following two fields identify the portion of the source text string
	 * containing this query.  They are typically only populated in top-level
	 * Queries, not in sub-queries.  When not set, they might both be zero, or
	 * both be -1 meaning "unknown".
	 */
	int stmt_location; /* start location, or -1 if unknown */
	int stmt_len;      /* length in bytes; 0 means "rest of string" */
} PGQuery;

/****************************************************************************
 *	Supporting data structures for Parse Trees
 *
 *	Most of these node types appear in raw parsetrees output by the grammar,
 *	and get transformed to something else by the analyzer.  A few of them
 *	are used as-is in transformed querytrees.
 ****************************************************************************/

/*
 * PGTypeName - specifies a type in definitions
 *
 * For PGTypeName structures generated internally, it is often easier to
 * specify the type by OID than by name.  If "names" is NIL then the
 * actual type OID is given by typeOid, otherwise typeOid is unused.
 * Similarly, if "typmods" is NIL then the actual typmod is expected to
 * be prespecified in typemod, otherwise typemod is unused.
 *
 * If pct_type is true, then names is actually a field name and we look up
 * the type of that field.  Otherwise (the normal case), names is a type
 * name possibly qualified with schema and database name.
 */
typedef struct PGTypeName {
	PGNodeTag type;
	PGList *names;       /* qualified name (list of PGValue strings) */
	PGOid typeOid;       /* type identified by OID */
	bool setof;          /* is a set? */
	bool pct_type;       /* %TYPE specified? */
	PGList *typmods;     /* type modifier expression(s) */
	int32_t typemod;     /* prespecified type modifier */
	PGList *arrayBounds; /* array bounds */
	int location;        /* token location, or -1 if unknown */
} PGTypeName;

/*
 * PGColumnRef - specifies a reference to a column, or possibly a whole tuple
 *
 * The "fields" list must be nonempty.  It can contain string PGValue nodes
 * (representing names) and PGAStar nodes (representing occurrence of a '*').
 * Currently, PGAStar must appear only as the last list element --- the grammar
 * is responsible for enforcing this!
 *
 * Note: any array subscripting or selection of fields from composite columns
 * is represented by an PGAIndirection node above the ColumnRef.  However,
 * for simplicity in the normal case, initial field selection from a table
 * name is represented within PGColumnRef and not by adding AIndirection.
 */
typedef struct PGColumnRef {
	PGNodeTag type;
	PGList *fields;       /* field names (PGValue strings) or PGAStar */
	int location;         /* token location, or -1 if unknown */
} PGColumnRef;

/*
 * PGParamRef - specifies a $n parameter reference
 */
typedef struct PGParamRef {
	PGNodeTag type;
	int number;   /* the number of the parameter */
	int location; /* token location, or -1 if unknown */
} PGParamRef;

/*
 * PGAExpr - infix, prefix, and postfix expressions
 */
typedef enum PGAExpr_Kind {
	PG_AEXPR_OP,              /* normal operator */
	PG_AEXPR_OP_ANY,          /* scalar op ANY (array) */
	PG_AEXPR_OP_ALL,          /* scalar op ALL (array) */
	PG_AEXPR_DISTINCT,        /* IS DISTINCT FROM - name must be "=" */
	PG_AEXPR_NOT_DISTINCT,    /* IS NOT DISTINCT FROM - name must be "=" */
	PG_AEXPR_NULLIF,          /* NULLIF - name must be "=" */
	PG_AEXPR_OF,              /* IS [NOT] OF - name must be "=" or "<>" */
	PG_AEXPR_IN,              /* [NOT] IN - name must be "=" or "<>" */
	PG_AEXPR_LIKE,            /* [NOT] LIKE - name must be "~~" or "!~~" */
	PG_AEXPR_ILIKE,           /* [NOT] ILIKE - name must be "~~*" or "!~~*" */
	PG_AEXPR_GLOB,            /* [NOT] GLOB - name must be "~~~" or "!~~~" */
	PG_AEXPR_SIMILAR,         /* [NOT] SIMILAR - name must be "~" or "!~" */
	PG_AEXPR_BETWEEN,         /* name must be "BETWEEN" */
	PG_AEXPR_NOT_BETWEEN,     /* name must be "NOT BETWEEN" */
	PG_AEXPR_BETWEEN_SYM,     /* name must be "BETWEEN SYMMETRIC" */
	PG_AEXPR_NOT_BETWEEN_SYM, /* name must be "NOT BETWEEN SYMMETRIC" */
	AEXPR_PAREN               /* nameless dummy node for parentheses */
} PGAExpr_Kind;

typedef struct PGAExpr {
	PGNodeTag type;
	PGAExpr_Kind kind; /* see above */
	PGList *name;      /* possibly-qualified name of operator */
	PGNode *lexpr;     /* left argument, or NULL if none */
	PGNode *rexpr;     /* right argument, or NULL if none */
	int location;      /* token location, or -1 if unknown */
} PGAExpr;

/*
 * PGAConst - a literal constant
 */
typedef struct PGAConst {
	PGNodeTag type;
	PGValue val;  /* value (includes type info, see value.h) */
	int location; /* token location, or -1 if unknown */
} PGAConst;

/*
 * PGTypeCast - a CAST expression
 */
typedef struct PGTypeCast {
	PGNodeTag type;
	PGNode *arg;          /* the expression being casted */
	PGTypeName *typeName; /* the target type */
	int tryCast;          /* TRY_CAST or CAST */
	int location;         /* token location, or -1 if unknown */
} PGTypeCast;

/*
 * PGCollateClause - a COLLATE expression
 */
typedef struct PGCollateClause {
	PGNodeTag type;
	PGNode *arg;      /* input expression */
	PGList *collname; /* possibly-qualified collation name */
	int location;     /* token location, or -1 if unknown */
} PGCollateClause;

/*
 * PGFuncCall - a function or aggregate invocation
 *
 * agg_order (if not NIL) indicates we saw 'foo(... ORDER BY ...)', or if
 * agg_within_group is true, it was 'foo(...) WITHIN GROUP (ORDER BY ...)'.
 * agg_star indicates we saw a 'foo(*)' construct, while agg_distinct
 * indicates we saw 'foo(DISTINCT ...)'.  In any of these cases, the
 * construct *must* be an aggregate call.  Otherwise, it might be either an
 * aggregate or some other kind of function.  However, if FILTER or OVER is
 * present it had better be an aggregate or window function.
 *
 * Normally, you'd initialize this via makeFuncCall() and then only change the
 * parts of the struct its defaults don't match afterwards, as needed.
 */
typedef struct PGFuncCall {
	PGNodeTag type;
	PGList *funcname;         /* qualified name of function */
	PGList *args;             /* the arguments (list of exprs) */
	PGList *agg_order;        /* ORDER BY (list of PGSortBy) */
	PGNode *agg_filter;       /* FILTER clause, if any */
	bool export_state;        /* EXPORT_STATE clause, if any */
	bool agg_within_group;    /* ORDER BY appeared in WITHIN GROUP */
	bool agg_star;            /* argument was really '*' */
	bool agg_distinct;        /* arguments were labeled DISTINCT */
	bool agg_ignore_nulls;    /* arguments were labeled IGNORE NULLS */
	bool func_variadic;       /* last argument was labeled VARIADIC */
	struct PGWindowDef *over; /* OVER clause, if any */
	int location;             /* token location, or -1 if unknown */
} PGFuncCall;

/*
 * PGAStar - '*' representing all columns of a table or compound field
 *
 * This can appear within ColumnRef.fields, AIndirection.indirection, and
 * ResTarget.indirection lists.
 */
typedef struct PGAStar {
	PGNodeTag type;
	char *relation;       /* relation name (optional) */
	PGList *except_list;  /* optional: EXCLUDE list */
	PGList *replace_list; /* optional: REPLACE list */
} PGAStar;

/*
 * PGAIndices - array subscript or slice bounds ([idx] or [lidx:uidx])
 *
 * In slice case, either or both of lidx and uidx can be NULL (omitted).
 * In non-slice case, uidx holds the single subscript and lidx is always NULL.
 */
typedef struct PGAIndices {
	PGNodeTag type;
	bool is_slice; /* true if slice (i.e., colon present) */
	PGNode *lidx;  /* slice lower bound, if any */
	PGNode *uidx;  /* subscript, or slice upper bound if any */
} PGAIndices;

/*
 * PGAIndirection - select a field and/or array element from an expression
 *
 * The indirection list can contain PGAIndices nodes (representing
 * subscripting), string PGValue nodes (representing field selection --- the
 * string value is the name of the field to select), and PGAStar nodes
 * (representing selection of all fields of a composite type).
 * For example, a complex selection operation like
 *				(foo).field1[42][7].field2
 * would be represented with a single PGAIndirection node having a 4-element
 * indirection list.
 *
 * Currently, PGAStar must appear only as the last list element --- the grammar
 * is responsible for enforcing this!
 */
typedef struct PGAIndirection {
	PGNodeTag type;
	PGNode *arg;         /* the thing being selected from */
	PGList *indirection; /* subscripts and/or field names and/or * */
} PGAIndirection;

/*
 * PGAArrayExpr - an ARRAY[] construct
 */
typedef struct PGAArrayExpr {
	PGNodeTag type;
	PGList *elements; /* array element expressions */
	int location;     /* token location, or -1 if unknown */
} PGAArrayExpr;

/*
 * PGResTarget -
 *	  result target (used in target list of pre-transformed parse trees)
 *
 * In a SELECT target list, 'name' is the column label from an
 * 'AS ColumnLabel' clause, or NULL if there was none, and 'val' is the
 * value expression itself.  The 'indirection' field is not used.
 *
 * INSERT uses PGResTarget in its target-column-names list.  Here, 'name' is
 * the name of the destination column, 'indirection' stores any subscripts
 * attached to the destination, and 'val' is not used.
 *
 * In an UPDATE target list, 'name' is the name of the destination column,
 * 'indirection' stores any subscripts attached to the destination, and
 * 'val' is the expression to assign.
 *
 * See PGAIndirection for more info about what can appear in 'indirection'.
 */
typedef struct PGResTarget {
	PGNodeTag type;
	char *name;          /* column name or NULL */
	PGList *indirection; /* subscripts, field names, and '*', or NIL */
	PGNode *val;         /* the value expression to compute or assign */
	int location;        /* token location, or -1 if unknown */
} PGResTarget;

/*
 * PGMultiAssignRef - element of a row source expression for UPDATE
 *
 * In an UPDATE target list, when we have SET (a,b,c) = row-valued-expression,
 * we generate separate PGResTarget items for each of a,b,c.  Their "val" trees
 * are PGMultiAssignRef nodes numbered 1..n, linking to a common copy of the
 * row-valued-expression (which parse analysis will process only once, when
 * handling the PGMultiAssignRef with colno=1).
 */
typedef struct PGMultiAssignRef {
	PGNodeTag type;
	PGNode *source; /* the row-valued expression */
	int colno;      /* column number for this target (1..n) */
	int ncolumns;   /* number of targets in the construct */
} PGMultiAssignRef;

/*
 * PGSortBy - for ORDER BY clause
 */
typedef struct PGSortBy {
	PGNodeTag type;
	PGNode *node;               /* expression to sort on */
	PGSortByDir sortby_dir;     /* ASC/DESC/USING/default */
	PGSortByNulls sortby_nulls; /* NULLS FIRST/LAST */
	PGList *useOp;              /* name of op to use, if SORTBY_USING */
	int location;               /* operator location, or -1 if none/unknown */
} PGSortBy;

/*
 * PGWindowDef - raw representation of WINDOW and OVER clauses
 *
 * For entries in a WINDOW list, "name" is the window name being defined.
 * For OVER clauses, we use "name" for the "OVER window" syntax, or "refname"
 * for the "OVER (window)" syntax, which is subtly different --- the latter
 * implies overriding the window frame clause.
 */
typedef struct PGWindowDef {
	PGNodeTag type;
	char *name;              /* window's own name */
	char *refname;           /* referenced window name, if any */
	PGList *partitionClause; /* PARTITION BY expression list */
	PGList *orderClause;     /* ORDER BY (list of PGSortBy) */
	int frameOptions;        /* frame_clause options, see below */
	PGNode *startOffset;     /* expression for starting bound, if any */
	PGNode *endOffset;       /* expression for ending bound, if any */
	int location;            /* parse location, or -1 if none/unknown */
} PGWindowDef;

/*
 * frameOptions is an OR of these bits.  The NONDEFAULT and BETWEEN bits are
 * used so that ruleutils.c can tell which properties were specified and
 * which were defaulted; the correct behavioral bits must be set either way.
 * The START_foo and END_foo options must come in pairs of adjacent bits for
 * the convenience of gram.y, even though some of them are useless/invalid.
 * We will need more bits (and fields) to cover the full SQL:2008 option set.
 */
#define FRAMEOPTION_NONDEFAULT 0x00001 /* any specified? */
#define FRAMEOPTION_RANGE 0x00002 /* RANGE behavior */
#define FRAMEOPTION_ROWS 0x00004 /* ROWS behavior */
#define FRAMEOPTION_BETWEEN 0x00008 /* BETWEEN given? */
#define FRAMEOPTION_START_UNBOUNDED_PRECEDING 0x00010 /* start is U. P. */
#define FRAMEOPTION_END_UNBOUNDED_PRECEDING 0x00020 /* (disallowed) */
#define FRAMEOPTION_START_UNBOUNDED_FOLLOWING 0x00040 /* (disallowed) */
#define FRAMEOPTION_END_UNBOUNDED_FOLLOWING 0x00080 /* end is U. F. */
#define FRAMEOPTION_START_CURRENT_ROW 0x00100 /* start is C. R. */
#define FRAMEOPTION_END_CURRENT_ROW 0x00200 /* end is C. R. */
#define FRAMEOPTION_START_VALUE_PRECEDING 0x00400 /* start is V. P. */
#define FRAMEOPTION_END_VALUE_PRECEDING 0x00800 /* end is V. P. */
#define FRAMEOPTION_START_VALUE_FOLLOWING 0x01000 /* start is V. F. */
#define FRAMEOPTION_END_VALUE_FOLLOWING 0x02000 /* end is V. F. */

#define FRAMEOPTION_START_VALUE (FRAMEOPTION_START_VALUE_PRECEDING | FRAMEOPTION_START_VALUE_FOLLOWING)
#define FRAMEOPTION_END_VALUE (FRAMEOPTION_END_VALUE_PRECEDING | FRAMEOPTION_END_VALUE_FOLLOWING)

#define FRAMEOPTION_DEFAULTS (FRAMEOPTION_RANGE | FRAMEOPTION_START_UNBOUNDED_PRECEDING | FRAMEOPTION_END_CURRENT_ROW)

/*
 * PGRangeSubselect - subquery appearing in a FROM clause
 */
typedef struct PGRangeSubselect {
	PGNodeTag type;
	bool lateral;     /* does it have LATERAL prefix? */
	PGNode *subquery; /* the untransformed sub-select clause */
	PGAlias *alias;   /* table alias & optional column aliases */
	PGNode *sample;   /* sample options (if any) */
} PGRangeSubselect;

/*
 * PGRangeFunction - function call appearing in a FROM clause
 *
 * functions is a PGList because we use this to represent the construct
 * ROWS FROM(func1(...), func2(...), ...).  Each element of this list is a
 * two-element sublist, the first element being the untransformed function
 * call tree, and the second element being a possibly-empty list of PGColumnDef
 * nodes representing any columndef list attached to that function within the
 * ROWS FROM() syntax.
 *
 * alias and coldeflist represent any alias and/or columndef list attached
 * at the top level.  (We disallow coldeflist appearing both here and
 * per-function, but that's checked in parse analysis, not by the grammar.)
 */
typedef struct PGRangeFunction {
	PGNodeTag type;
	bool lateral;       /* does it have LATERAL prefix? */
	bool ordinality;    /* does it have WITH ORDINALITY suffix? */
	bool is_rowsfrom;   /* is result of ROWS FROM() syntax? */
	PGList *functions;  /* per-function information, see above */
	PGAlias *alias;     /* table alias & optional column aliases */
	PGList *coldeflist; /* list of PGColumnDef nodes to describe result
								 * of function returning RECORD */
	PGNode *sample;   /* sample options (if any) */
} PGRangeFunction;

/* Category of the column */
typedef enum ColumnCategory {
	COL_STANDARD,	/* regular column */
	COL_GENERATED	/* generated (VIRTUAL|STORED) */
}	ColumnCategory;

/*
 * PGColumnDef - column definition (used in various creates)
 *
 * If the column has a default value, we may have the value expression
 * in either "raw" form (an untransformed parse tree) or "cooked" form
 * (a post-parse-analysis, executable expression tree), depending on
 * how this PGColumnDef node was created (by parsing, or by inheritance
 * from an existing relation).  We should never have both in the same node!
 *
 * Similarly, we may have a COLLATE specification in either raw form
 * (represented as a PGCollateClause with arg==NULL) or cooked form
 * (the collation's OID).
 *
 * The constraints list may contain a PG_CONSTR_DEFAULT item in a raw
 * parsetree produced by gram.y, but transformCreateStmt will remove
 * the item and set raw_default instead.  PG_CONSTR_DEFAULT items
 * should not appear in any subsequent processing.
 */

typedef struct PGColumnDef {
	PGNodeTag type;               /* ENSURES COMPATIBILITY WITH 'PGNode' - has to be first line */
	char *colname;                /* name of column */
	PGTypeName *typeName;         /* type of column */
	int inhcount;                 /* number of times column is inherited */
	bool is_local;                /* column has local (non-inherited) def'n */
	bool is_not_null;             /* NOT NULL constraint specified? */
	bool is_from_type;            /* column definition came from table type */
	bool is_from_parent;          /* column def came from partition parent */
	char storage;                 /* attstorage setting, or 0 for default */
	PGNode *raw_default;          /* default value (untransformed parse tree) */
	PGNode *cooked_default;       /* default value (transformed expr tree) */
	char identity;                /* attidentity setting */
	PGRangeVar *identitySequence; /* to store identity sequence name for ALTER
								   * TABLE ... ADD COLUMN */
	PGCollateClause *collClause;  /* untransformed COLLATE spec, if any */
	PGOid collOid;                /* collation OID (InvalidOid if not set) */
	PGList *constraints;          /* other constraints on column */
	PGList *fdwoptions;           /* per-column FDW options */
	int location;                 /* parse location, or -1 if none/unknown */
	ColumnCategory category;	  /* category of the column */
} PGColumnDef;

/*
 * PGTableLikeClause - CREATE TABLE ( ... LIKE ... ) clause
 */
typedef struct PGTableLikeClause {
	PGNodeTag type;
	PGRangeVar *relation;
	uint32_t options; /* OR of PGTableLikeOption flags */
} PGTableLikeClause;

typedef enum PGTableLikeOption {
	PG_CREATE_TABLE_LIKE_DEFAULTS = 1 << 0,
	PG_CREATE_TABLE_LIKE_CONSTRAINTS = 1 << 1,
	PG_CREATE_TABLE_LIKE_IDENTITY = 1 << 2,
	PG_CREATE_TABLE_LIKE_INDEXES = 1 << 3,
	PG_CREATE_TABLE_LIKE_STORAGE = 1 << 4,
	PG_CREATE_TABLE_LIKE_COMMENTS = 1 << 5,
	PG_CREATE_TABLE_LIKE_STATISTICS = 1 << 6,
	PG_CREATE_TABLE_LIKE_ALL = INT_MAX
} PGTableLikeOption;

/*
 * PGIndexElem - index parameters (used in CREATE INDEX, and in ON CONFLICT)
 *
 * For a plain index attribute, 'name' is the name of the table column to
 * index, and 'expr' is NULL.  For an index expression, 'name' is NULL and
 * 'expr' is the expression tree.
 */
typedef struct PGIndexElem {
	PGNodeTag type;
	char *name;                   /* name of attribute to index, or NULL */
	PGNode *expr;                 /* expression to index, or NULL */
	char *indexcolname;           /* name for index column; NULL = default */
	PGList *collation;            /* name of collation; NIL = default */
	PGList *opclass;              /* name of desired opclass; NIL = default */
	PGSortByDir ordering;         /* ASC/DESC/default */
	PGSortByNulls nulls_ordering; /* FIRST/LAST/default */
} PGIndexElem;

/*
 * PGDefElem - a generic "name = value" option definition
 *
 * In some contexts the name can be qualified.  Also, certain SQL commands
 * allow a SET/ADD/DROP action to be attached to option settings, so it's
 * convenient to carry a field for that too.  (Note: currently, it is our
 * practice that the grammar allows namespace and action only in statements
 * where they are relevant; C code can just ignore those fields in other
 * statements.)
 */
typedef enum PGDefElemAction {
	PG_DEFELEM_UNSPEC, /* no action given */
	PG_DEFELEM_SET,
	PG_DEFELEM_ADD,
	DEFELEM_DROP
} PGDefElemAction;

typedef struct PGDefElem {
	PGNodeTag type;
	char *defnamespace; /* NULL if unqualified name */
	char *defname;
	PGNode *arg;               /* a (PGValue *) or a (PGTypeName *) */
	PGDefElemAction defaction; /* unspecified action, or SET/ADD/DROP */
	int location;              /* token location, or -1 if unknown */
} PGDefElem;

/*
 * PGLockingClause - raw representation of FOR [NO KEY] UPDATE/[KEY] SHARE
 *		options
 *
 * Note: lockedRels == NIL means "all relations in query".  Otherwise it
 * is a list of PGRangeVar nodes.  (We use PGRangeVar mainly because it carries
 * a location field --- currently, parse analysis insists on unqualified
 * names in LockingClause.)
 */
typedef struct PGLockingClause {
	PGNodeTag type;
	PGList *lockedRels; /* FOR [KEY] UPDATE/SHARE relations */
	PGLockClauseStrength strength;
	PGLockWaitPolicy waitPolicy; /* NOWAIT and SKIP LOCKED */
} PGLockingClause;

/****************************************************************************
 *	Nodes for a PGQuery tree
 ****************************************************************************/

/*--------------------
 * PGRangeTblEntry -
 *	  A range table is a PGList of PGRangeTblEntry nodes.
 *
 *	  A range table entry may represent a plain relation, a sub-select in
 *	  FROM, or the result of a JOIN clause.  (Only explicit JOIN syntax
 *	  produces an RTE, not the implicit join resulting from multiple FROM
 *	  items.  This is because we only need the RTE to deal with SQL features
 *	  like outer joins and join-output-column aliasing.)  Other special
 *	  RTE types also exist, as indicated by RTEKind.
 *
 *	  Note that we consider PG_RTE_RELATION to cover anything that has a pg_class
 *	  entry.  relkind distinguishes the sub-cases.
 *
 *	  alias is an PGAlias node representing the AS alias-clause attached to the
 *	  FROM expression, or NULL if no clause.
 *
 *	  eref is the table reference name and column reference names (either
 *	  real or aliases).  Note that system columns (OID etc) are not included
 *	  in the column list.
 *	  eref->aliasname is required to be present, and should generally be used
 *	  to identify the RTE for error messages etc.
 *
 *	  In RELATION RTEs, the colnames in both alias and eref are indexed by
 *	  physical attribute number; this means there must be colname entries for
 *	  dropped columns.  When building an RTE we insert empty strings ("") for
 *	  dropped columns.  Note however that a stored rule may have nonempty
 *	  colnames for columns dropped since the rule was created (and for that
 *	  matter the colnames might be out of date due to column renamings).
 *	  The same comments apply to FUNCTION RTEs when a function's return type
 *	  is a named composite type.
 *
 *	  In JOIN RTEs, the colnames in both alias and eref are one-to-one with
 *	  joinaliasvars entries.  A JOIN RTE will omit columns of its inputs when
 *	  those columns are known to be dropped at parse time.  Again, however,
 *	  a stored rule might contain entries for columns dropped since the rule
 *	  was created.  (This is only possible for columns not actually referenced
 *	  in the rule.)  When loading a stored rule, we replace the joinaliasvars
 *	  items for any such columns with null pointers.  (We can't simply delete
 *	  them from the joinaliasvars list, because that would affect the attnums
 *	  of Vars referencing the rest of the list.)
 *
 *	  inh is true for relation references that should be expanded to include
 *	  inheritance children, if the rel has any.  This *must* be false for
 *	  RTEs other than PG_RTE_RELATION entries.
 *
 *	  inFromCl marks those range variables that are listed in the FROM clause.
 *	  It's false for RTEs that are added to a query behind the scenes, such
 *	  as the NEW and OLD variables for a rule, or the subqueries of a UNION.
 *	  This flag is not used anymore during parsing, since the parser now uses
 *	  a separate "namespace" data structure to control visibility, but it is
 *	  needed by ruleutils.c to determine whether RTEs should be shown in
 *	  decompiled queries.
 *--------------------
 */
typedef enum PGRTEKind {
	PG_RTE_RELATION,    /* ordinary relation reference */
	PG_RTE_SUBQUERY,    /* subquery in FROM */
	PG_RTE_JOIN,        /* join */
	PG_RTE_FUNCTION,    /* function in FROM */
	PG_RTE_TABLEFUNC,   /* TableFunc(.., column list) */
	PG_RTE_VALUES,      /* VALUES (<exprlist>), (<exprlist>), ... */
	PG_RTE_CTE,         /* common table expr (WITH list element) */
	RTE_NAMEDTUPLESTORE /* tuplestore, e.g. for AFTER triggers */
} PGRTEKind;

typedef struct PGRangeTblEntry {
	PGNodeTag type;

	PGRTEKind rtekind; /* see above */

	/*
	 * XXX the fields applicable to only some rte kinds should be merged into
	 * a union.  I didn't do this yet because the diffs would impact a lot of
	 * code that is being actively worked on.  FIXME someday.
	 */

	/*
	 * Fields valid for a plain relation RTE (else zero):
	 *
	 * As a special case, RTE_NAMEDTUPLESTORE can also set relid to indicate
	 * that the tuple format of the tuplestore is the same as the referenced
	 * relation.  This allows plans referencing AFTER trigger transition
	 * tables to be invalidated if the underlying table is altered.
	 */
	PGOid relid;                             /* OID of the relation */
	char relkind;                            /* relation kind (see pg_class.relkind) */
	struct PGTableSampleClause *tablesample; /* sampling info, or NULL */

	/*
	 * Fields valid for a subquery RTE (else NULL):
	 */
	PGQuery *subquery; /* the sub-query */

	/*
	 * Fields valid for a join RTE (else NULL/zero):
	 *
	 * joinaliasvars is a list of (usually) Vars corresponding to the columns
	 * of the join result.  An alias PGVar referencing column K of the join
	 * result can be replaced by the K'th element of joinaliasvars --- but to
	 * simplify the task of reverse-listing aliases correctly, we do not do
	 * that until planning time.  In detail: an element of joinaliasvars can
	 * be a PGVar of one of the join's input relations, or such a PGVar with an
	 * implicit coercion to the join's output column type, or a COALESCE
	 * expression containing the two input column Vars (possibly coerced).
	 * Within a PGQuery loaded from a stored rule, it is also possible for
	 * joinaliasvars items to be null pointers, which are placeholders for
	 * (necessarily unreferenced) columns dropped since the rule was made.
	 * Also, once planning begins, joinaliasvars items can be almost anything,
	 * as a result of subquery-flattening substitutions.
	 */
	PGJoinType jointype;   /* type of join */
	PGList *joinaliasvars; /* list of alias-var expansions */

	/*
	 * Fields valid for a function RTE (else NIL/zero):
	 *
	 * When funcordinality is true, the eref->colnames list includes an alias
	 * for the ordinality column.  The ordinality column is otherwise
	 * implicit, and must be accounted for "by hand" in places such as
	 * expandRTE().
	 */
	PGList *functions;   /* list of PGRangeTblFunction nodes */
	bool funcordinality; /* is this called WITH ORDINALITY? */

	/*
	 * Fields valid for a PGTableFunc RTE (else NULL):
	 */
	PGTableFunc *tablefunc;

	/*
	 * Fields valid for a values RTE (else NIL):
	 */
	PGList *values_lists; /* list of expression lists */

	/*
	 * Fields valid for a CTE RTE (else NULL/zero):
	 */
	char *ctename;       /* name of the WITH list item */
	PGIndex ctelevelsup; /* number of query levels up */
	bool self_reference; /* is this a recursive self-reference? */

	/*
	 * Fields valid for table functions, values, CTE and ENR RTEs (else NIL):
	 *
	 * We need these for CTE RTEs so that the types of self-referential
	 * columns are well-defined.  For VALUES RTEs, storing these explicitly
	 * saves having to re-determine the info by scanning the values_lists. For
	 * ENRs, we store the types explicitly here (we could get the information
	 * from the catalogs if 'relid' was supplied, but we'd still need these
	 * for TupleDesc-based ENRs, so we might as well always store the type
	 * info here).
	 *
	 * For ENRs only, we have to consider the possibility of dropped columns.
	 * A dropped column is included in these lists, but it will have zeroes in
	 * all three lists (as well as an empty-string entry in eref).  Testing
	 * for zero coltype is the standard way to detect a dropped column.
	 */
	PGList *coltypes;      /* OID list of column type OIDs */
	PGList *coltypmods;    /* integer list of column typmods */
	PGList *colcollations; /* OID list of column collation OIDs */

	/*
	 * Fields valid for ENR RTEs (else NULL/zero):
	 */
	char *enrname;    /* name of ephemeral named relation */
	double enrtuples; /* estimated or actual from caller */

	/*
	 * Fields valid in all RTEs:
	 */
	PGAlias *alias; /* user-written alias clause, if any */
	PGAlias *eref;  /* expanded reference names */
	bool lateral;   /* subquery, function, or values is LATERAL? */
	bool inh;       /* inheritance requested? */
	bool inFromCl;  /* present in FROM clause? */
} PGRangeTblEntry;

/*
 * PGRangeTblFunction -
 *	  PGRangeTblEntry subsidiary data for one function in a FUNCTION RTE.
 *
 * If the function had a column definition list (required for an
 * otherwise-unspecified RECORD result), funccolnames lists the names given
 * in the definition list, funccoltypes lists their declared column types,
 * funccoltypmods lists their typmods, funccolcollations their collations.
 * Otherwise, those fields are NIL.
 *
 * Notice we don't attempt to store info about the results of functions
 * returning named composite types, because those can change from time to
 * time.  We do however remember how many columns we thought the type had
 * (including dropped columns!), so that we can successfully ignore any
 * columns added after the query was parsed.
 */
typedef struct PGRangeTblFunction {
	PGNodeTag type;

	PGNode *funcexpr; /* expression tree for func call */
	int funccolcount; /* number of columns it contributes to RTE */
	/* These fields record the contents of a column definition list, if any: */
	PGList *funccolnames;      /* column names (list of String) */
	PGList *funccoltypes;      /* OID list of column type OIDs */
	PGList *funccoltypmods;    /* integer list of column typmods */
	PGList *funccolcollations; /* OID list of column collation OIDs */
	/* This is set during planning for use by the executor: */
	PGBitmapset *funcparams; /* PG_PARAM_EXEC PGParam IDs affecting this func */
} PGRangeTblFunction;

/*
 * PGSortGroupClause -
 *		representation of ORDER BY, GROUP BY, PARTITION BY,
 *		DISTINCT, DISTINCT ON items
 *
 * You might think that ORDER BY is only interested in defining ordering,
 * and GROUP/DISTINCT are only interested in defining equality.  However,
 * one way to implement grouping is to sort and then apply a "uniq"-like
 * filter.  So it's also interesting to keep track of possible sort operators
 * for GROUP/DISTINCT, and in particular to try to sort for the grouping
 * in a way that will also yield a requested ORDER BY ordering.  So we need
 * to be able to compare ORDER BY and GROUP/DISTINCT lists, which motivates
 * the decision to give them the same representation.
 *
 * tleSortGroupRef must match ressortgroupref of exactly one entry of the
 *		query's targetlist; that is the expression to be sorted or grouped by.
 * eqop is the OID of the equality operator.
 * sortop is the OID of the ordering operator (a "<" or ">" operator),
 *		or InvalidOid if not available.
 * nulls_first means about what you'd expect.  If sortop is InvalidOid
 *		then nulls_first is meaningless and should be set to false.
 * hashable is true if eqop is hashable (note this condition also depends
 *		on the datatype of the input expression).
 *
 * In an ORDER BY item, all fields must be valid.  (The eqop isn't essential
 * here, but it's cheap to get it along with the sortop, and requiring it
 * to be valid eases comparisons to grouping items.)  Note that this isn't
 * actually enough information to determine an ordering: if the sortop is
 * collation-sensitive, a collation OID is needed too.  We don't store the
 * collation in PGSortGroupClause because it's not available at the time the
 * parser builds the PGSortGroupClause; instead, consult the exposed collation
 * of the referenced targetlist expression to find out what it is.
 *
 * In a grouping item, eqop must be valid.  If the eqop is a btree equality
 * operator, then sortop should be set to a compatible ordering operator.
 * We prefer to set eqop/sortop/nulls_first to match any ORDER BY item that
 * the query presents for the same tlist item.  If there is none, we just
 * use the default ordering op for the datatype.
 *
 * If the tlist item's type has a hash opclass but no btree opclass, then
 * we will set eqop to the hash equality operator, sortop to InvalidOid,
 * and nulls_first to false.  A grouping item of this kind can only be
 * implemented by hashing, and of course it'll never match an ORDER BY item.
 *
 * The hashable flag is provided since we generally have the requisite
 * information readily available when the PGSortGroupClause is constructed,
 * and it's relatively expensive to get it again later.  Note there is no
 * need for a "sortable" flag since OidIsValid(sortop) serves the purpose.
 *
 * A query might have both ORDER BY and DISTINCT (or DISTINCT ON) clauses.
 * In SELECT DISTINCT, the distinctClause list is as long or longer than the
 * sortClause list, while in SELECT DISTINCT ON it's typically shorter.
 * The two lists must match up to the end of the shorter one --- the parser
 * rearranges the distinctClause if necessary to make this true.  (This
 * restriction ensures that only one sort step is needed to both satisfy the
 * ORDER BY and set up for the PGUnique step.  This is semantically necessary
 * for DISTINCT ON, and presents no real drawback for DISTINCT.)
 */
typedef struct PGSortGroupClause {
	PGNodeTag type;
	PGIndex tleSortGroupRef; /* reference into targetlist */
	PGOid eqop;              /* the equality operator ('=' op) */
	PGOid sortop;            /* the ordering operator ('<' op), or 0 */
	bool nulls_first;        /* do NULLs come before normal values? */
	bool hashable;           /* can eqop be implemented by hashing? */
} PGSortGroupClause;

/*
 * PGGroupingSet -
 *		representation of CUBE, ROLLUP and GROUPING SETS clauses
 *
 * In a PGQuery with grouping sets, the groupClause contains a flat list of
 * PGSortGroupClause nodes for each distinct expression used.  The actual
 * structure of the GROUP BY clause is given by the groupingSets tree.
 *
 * In the raw parser output, PGGroupingSet nodes (of all types except SIMPLE
 * which is not used) are potentially mixed in with the expressions in the
 * groupClause of the SelectStmt.  (An expression can't contain a PGGroupingSet,
 * but a list may mix PGGroupingSet and expression nodes.)  At this stage, the
 * content of each node is a list of expressions, some of which may be RowExprs
 * which represent sublists rather than actual row constructors, and nested
 * PGGroupingSet nodes where legal in the grammar.  The structure directly
 * reflects the query syntax.
 *
 * In parse analysis, the transformed expressions are used to build the tlist
 * and groupClause list (of PGSortGroupClause nodes), and the groupingSets tree
 * is eventually reduced to a fixed format:
 *
 * EMPTY nodes represent (), and obviously have no content
 *
 * SIMPLE nodes represent a list of one or more expressions to be treated as an
 * atom by the enclosing structure; the content is an integer list of
 * ressortgroupref values (see PGSortGroupClause)
 *
 * CUBE and ROLLUP nodes contain a list of one or more SIMPLE nodes.
 *
 * SETS nodes contain a list of EMPTY, SIMPLE, CUBE or ROLLUP nodes, but after
 * parse analysis they cannot contain more SETS nodes; enough of the syntactic
 * transforms of the spec have been applied that we no longer have arbitrarily
 * deep nesting (though we still preserve the use of cube/rollup).
 *
 * Note that if the groupingSets tree contains no SIMPLE nodes (only EMPTY
 * nodes at the leaves), then the groupClause will be empty, but this is still
 * an aggregation query (similar to using aggs or HAVING without GROUP BY).
 *
 * As an example, the following clause:
 *
 * GROUP BY GROUPING SETS ((a,b), CUBE(c,(d,e)))
 *
 * looks like this after raw parsing:
 *
 * SETS( RowExpr(a,b) , CUBE( c, RowExpr(d,e) ) )
 *
 * and parse analysis converts it to:
 *
 * SETS( SIMPLE(1,2), CUBE( SIMPLE(3), SIMPLE(4,5) ) )
 */
typedef enum {
	GROUPING_SET_EMPTY,
	GROUPING_SET_SIMPLE,
	GROUPING_SET_ROLLUP,
	GROUPING_SET_CUBE,
	GROUPING_SET_SETS,
	GROUPING_SET_ALL
} GroupingSetKind;

typedef struct PGGroupingSet {
	PGNodeTag type;
	GroupingSetKind kind;
	PGList *content;
	int location;
} PGGroupingSet;

/*
 * PGWindowClause -
 *		transformed representation of WINDOW and OVER clauses
 *
 * A parsed Query's windowClause list contains these structs.  "name" is set
 * if the clause originally came from WINDOW, and is NULL if it originally
 * was an OVER clause (but note that we collapse out duplicate OVERs).
 * partitionClause and orderClause are lists of PGSortGroupClause structs.
 * winref is an ID number referenced by PGWindowFunc nodes; it must be unique
 * among the members of a Query's windowClause list.
 * When refname isn't null, the partitionClause is always copied from there;
 * the orderClause might or might not be copied (see copiedOrder); the framing
 * options are never copied, per spec.
 */
typedef struct PGWindowClause {
	PGNodeTag type;
	char *name;              /* window name (NULL in an OVER clause) */
	char *refname;           /* referenced window name, if any */
	PGList *partitionClause; /* PARTITION BY list */
	PGList *orderClause;     /* ORDER BY list */
	int frameOptions;        /* frame_clause options, see PGWindowDef */
	PGNode *startOffset;     /* expression for starting bound, if any */
	PGNode *endOffset;       /* expression for ending bound, if any */
	PGIndex winref;          /* ID referenced by window functions */
	bool copiedOrder;        /* did we copy orderClause from refname? */
} PGWindowClause;

/*
 * RowMarkClause -
 *	   parser output representation of FOR [KEY] UPDATE/SHARE clauses
 *
 * Query.rowMarks contains a separate RowMarkClause node for each relation
 * identified as a FOR [KEY] UPDATE/SHARE target.  If one of these clauses
 * is applied to a subquery, we generate RowMarkClauses for all normal and
 * subquery rels in the subquery, but they are marked pushedDown = true to
 * distinguish them from clauses that were explicitly written at this query
 * level.  Also, Query.hasForUpdate tells whether there were explicit FOR
 * UPDATE/SHARE/KEY SHARE clauses in the current query level.
 */

/*
 * PGWithClause -
 *	   representation of WITH clause
 *
 * Note: PGWithClause does not propagate into the PGQuery representation;
 * but PGCommonTableExpr does.
 */
typedef struct PGWithClause {
	PGNodeTag type;
	PGList *ctes;   /* list of CommonTableExprs */
	bool recursive; /* true = WITH RECURSIVE */
	int location;   /* token location, or -1 if unknown */
} PGWithClause;

/*
 * PGInferClause -
 *		ON CONFLICT unique index inference clause
 *
 * Note: PGInferClause does not propagate into the PGQuery representation.
 */
typedef struct PGInferClause {
	PGNodeTag type;
	PGList *indexElems;  /* IndexElems to infer unique index */
	PGNode *whereClause; /* qualification (partial-index predicate) */
	char *conname;       /* PGConstraint name, or NULL if unnamed */
	int location;        /* token location, or -1 if unknown */
} PGInferClause;

/*
 * PGOnConflictClause -
 *		representation of ON CONFLICT clause
 *
 * Note: PGOnConflictClause does not propagate into the PGQuery representation.
 */
typedef struct PGOnConflictClause {
	PGNodeTag type;
	PGOnConflictAction action; /* DO NOTHING or UPDATE? */
	PGInferClause *infer;      /* Optional index inference clause */
	PGList *targetList;        /* the target list (of PGResTarget) */
	PGNode *whereClause;       /* qualifications */
	int location;              /* token location, or -1 if unknown */
} PGOnConflictClause;

/*
 * PGCommonTableExpr -
 *	   representation of WITH list element
 *
 * We don't currently support the SEARCH or CYCLE clause.
 */
typedef struct PGCommonTableExpr {
	PGNodeTag type;
	char *ctename;         /* query name (never qualified) */
	PGList *aliascolnames; /* optional list of column names */
	/* SelectStmt/InsertStmt/etc before parse analysis, PGQuery afterwards: */
	PGNode *ctequery; /* the CTE's subquery */
	int location;     /* token location, or -1 if unknown */
	/* These fields are set during parse analysis: */
	bool cterecursive;        /* is this CTE actually recursive? */
	int cterefcount;          /* number of RTEs referencing this CTE
								 * (excluding internal self-references) */
	PGList *ctecolnames;      /* list of output column names */
	PGList *ctecoltypes;      /* OID list of output column type OIDs */
	PGList *ctecoltypmods;    /* integer list of output column typmods */
	PGList *ctecolcollations; /* OID list of column collation OIDs */
} PGCommonTableExpr;

/* Convenience macro to get the output tlist of a CTE's query */
#define GetCTETargetList(cte) \
	(AssertMacro(IsA((cte)->ctequery, PGQuery)), ((PGQuery *)(cte)->ctequery)->commandType == PG_CMD_SELECT ? ((PGQuery *)(cte)->ctequery)->targetList : ((PGQuery *)(cte)->ctequery)->returningList)

/*
 * TriggerTransition -
 *	   representation of transition row or table naming clause
 *
 * Only transition tables are initially supported in the syntax, and only for
 * AFTER triggers, but other permutations are accepted by the parser so we can
 * give a meaningful message from C code.
 */

/*****************************************************************************
 *		Raw Grammar Output Statements
 *****************************************************************************/

/*
 *		PGRawStmt --- container for any one statement's raw parse tree
 *
 * Parse analysis converts a raw parse tree headed by a PGRawStmt node into
 * an analyzed statement headed by a PGQuery node.  For optimizable statements,
 * the conversion is complex.  For utility statements, the parser usually just
 * transfers the raw parse tree (sans PGRawStmt) into the utilityStmt field of
 * the PGQuery node, and all the useful work happens at execution time.
 *
 * stmt_location/stmt_len identify the portion of the source text string
 * containing this raw statement (useful for multi-statement strings).
 */
typedef struct PGRawStmt {
	PGNodeTag type;
	PGNode *stmt;      /* raw parse tree */
	int stmt_location; /* start location, or -1 if unknown */
	int stmt_len;      /* length in bytes; 0 means "rest of string" */
} PGRawStmt;

/*****************************************************************************
 *		Optimizable Statements
 *****************************************************************************/

/* ----------------------
 *		Insert Statement
 *
 * The source expression is represented by PGSelectStmt for both the
 * SELECT and VALUES cases.  If selectStmt is NULL, then the query
 * is INSERT ... DEFAULT VALUES.
 * ----------------------
 */
typedef struct PGInsertStmt {
	PGNodeTag type;
	PGRangeVar *relation;                 /* relation to insert into */
	PGList *cols;                         /* optional: names of the target columns */
	PGNode *selectStmt;                   /* the source SELECT/VALUES, or NULL */
	PGOnConflictClause *onConflictClause; /* ON CONFLICT clause */
	PGList *returningList;                /* list of expressions to return */
	PGWithClause *withClause;             /* WITH clause */
	PGOverridingKind override;            /* OVERRIDING clause */
} PGInsertStmt;

/* ----------------------
 *		Delete Statement
 * ----------------------
 */
typedef struct PGDeleteStmt {
	PGNodeTag type;
	PGRangeVar *relation;     /* relation to delete from */
	PGList *usingClause;      /* optional using clause for more tables */
	PGNode *whereClause;      /* qualifications */
	PGList *returningList;    /* list of expressions to return */
	PGWithClause *withClause; /* WITH clause */
} PGDeleteStmt;

/* ----------------------
 *		Update Statement
 * ----------------------
 */
typedef struct PGUpdateStmt {
	PGNodeTag type;
	PGRangeVar *relation;     /* relation to update */
	PGList *targetList;       /* the target list (of PGResTarget) */
	PGNode *whereClause;      /* qualifications */
	PGList *fromClause;       /* optional from clause for more tables */
	PGList *returningList;    /* list of expressions to return */
	PGWithClause *withClause; /* WITH clause */
} PGUpdateStmt;

/* ----------------------
 *		Select Statement
 *
 * A "simple" SELECT is represented in the output of gram.y by a single
 * PGSelectStmt node; so is a VALUES construct.  A query containing set
 * operators (UNION, INTERSECT, EXCEPT) is represented by a tree of PGSelectStmt
 * nodes, in which the leaf nodes are component SELECTs and the internal nodes
 * represent UNION, INTERSECT, or EXCEPT operators.  Using the same node
 * type for both leaf and internal nodes allows gram.y to stick ORDER BY,
 * LIMIT, etc, clause values into a SELECT statement without worrying
 * whether it is a simple or compound SELECT.
 * ----------------------
 */
typedef enum PGSetOperation { PG_SETOP_NONE = 0, PG_SETOP_UNION, PG_SETOP_INTERSECT, PG_SETOP_EXCEPT } PGSetOperation;

typedef struct PGSelectStmt {
	PGNodeTag type;

	/*
	 * These fields are used only in "leaf" SelectStmts.
	 */
	PGList *distinctClause;   /* NULL, list of DISTINCT ON exprs, or
								 * lcons(NIL,NIL) for all (SELECT DISTINCT) */
	PGIntoClause *intoClause; /* target for SELECT INTO */
	PGList *targetList;       /* the target list (of PGResTarget) */
	PGList *fromClause;       /* the FROM clause */
	PGNode *whereClause;      /* WHERE qualification */
	PGList *groupClause;      /* GROUP BY clauses */
	PGNode *havingClause;     /* HAVING conditional-expression */
	PGList *windowClause;     /* WINDOW window_name AS (...), ... */
	PGNode *qualifyClause;    /* QUALIFY conditional-expression */

	/*
	 * In a "leaf" node representing a VALUES list, the above fields are all
	 * null, and instead this field is set.  Note that the elements of the
	 * sublists are just expressions, without PGResTarget decoration. Also note
	 * that a list element can be DEFAULT (represented as a PGSetToDefault
	 * node), regardless of the context of the VALUES list. It's up to parse
	 * analysis to reject that where not valid.
	 */
	PGList *valuesLists; /* untransformed list of expression lists */

	/*
	 * These fields are used in both "leaf" SelectStmts and upper-level
	 * SelectStmts.
	 */
	PGList *sortClause;       /* sort clause (a list of SortBy's) */
	PGNode *limitOffset;      /* # of result tuples to skip */
	PGNode *limitCount;       /* # of result tuples to return */
	PGNode *sampleOptions;    /* sample options (if any) */
	PGList *lockingClause;    /* FOR UPDATE (list of LockingClause's) */
	PGWithClause *withClause; /* WITH clause */

	/*
	 * These fields are used only in upper-level SelectStmts.
	 */
	PGSetOperation op;         /* type of set op */
	bool all;                  /* ALL specified? */
	struct PGSelectStmt *larg; /* left child */
	struct PGSelectStmt *rarg; /* right child */
	                           /* Eventually add fields for CORRESPONDING spec here */
} PGSelectStmt;

/* ----------------------
 *		Set Operation node for post-analysis query trees
 *
 * After parse analysis, a SELECT with set operations is represented by a
 * top-level PGQuery node containing the leaf SELECTs as subqueries in its
 * range table.  Its setOperations field shows the tree of set operations,
 * with leaf PGSelectStmt nodes replaced by PGRangeTblRef nodes, and internal
 * nodes replaced by SetOperationStmt nodes.  Information about the output
 * column types is added, too.  (Note that the child nodes do not necessarily
 * produce these types directly, but we've checked that their output types
 * can be coerced to the output column type.)  Also, if it's not UNION ALL,
 * information about the types' sort/group semantics is provided in the form
 * of a PGSortGroupClause list (same representation as, eg, DISTINCT).
 * The resolved common column collations are provided too; but note that if
 * it's not UNION ALL, it's okay for a column to not have a common collation,
 * so a member of the colCollations list could be InvalidOid even though the
 * column has a collatable type.
 * ----------------------
 */

/*****************************************************************************
 *		Other Statements (no optimizations required)
 *
 *		These are not touched by parser/analyze.c except to put them into
 *		the utilityStmt field of a Query.  This is eventually passed to
 *		ProcessUtility (by-passing rewriting and planning).  Some of the
 *		statements do need attention from parse analysis, and this is
 *		done by routines in parser/parse_utilcmd.c after ProcessUtility
 *		receives the command for execution.
 *		DECLARE CURSOR, EXPLAIN, and CREATE TABLE AS are special cases:
 *		they contain optimizable statements, which get processed normally
 *		by parser/analyze.c.
 *****************************************************************************/

/*
 * When a command can act on several kinds of objects with only one
 * parse structure required, use these constants to designate the
 * object type.  Note that commands typically don't support all the types.
 */

typedef enum PGObjectType {
	PG_OBJECT_ACCESS_METHOD,
	PG_OBJECT_AGGREGATE,
	PG_OBJECT_AMOP,
	PG_OBJECT_AMPROC,
	PG_OBJECT_ATTRIBUTE, /* type's attribute, when distinct from column */
	PG_OBJECT_CAST,
	PG_OBJECT_COLUMN,
	PG_OBJECT_COLLATION,
	PG_OBJECT_CONVERSION,
	PG_OBJECT_DATABASE,
	PG_OBJECT_DEFAULT,
	PG_OBJECT_DEFACL,
	PG_OBJECT_DOMAIN,
	PG_OBJECT_DOMCONSTRAINT,
	PG_OBJECT_EVENT_TRIGGER,
	PG_OBJECT_EXTENSION,
	PG_OBJECT_FDW,
	PG_OBJECT_FOREIGN_SERVER,
	PG_OBJECT_FOREIGN_TABLE,
	PG_OBJECT_FUNCTION,
	PG_OBJECT_TABLE_MACRO,
	PG_OBJECT_INDEX,
	PG_OBJECT_LANGUAGE,
	PG_OBJECT_LARGEOBJECT,
	PG_OBJECT_MATVIEW,
	PG_OBJECT_OPCLASS,
	PG_OBJECT_OPERATOR,
	PG_OBJECT_OPFAMILY,
	PG_OBJECT_POLICY,
	PG_OBJECT_PUBLICATION,
	PG_OBJECT_PUBLICATION_REL,
	PG_OBJECT_ROLE,
	PG_OBJECT_RULE,
	PG_OBJECT_SCHEMA,
	PG_OBJECT_SEQUENCE,
	PG_OBJECT_SUBSCRIPTION,
	PG_OBJECT_STATISTIC_EXT,
	PG_OBJECT_TABCONSTRAINT,
	PG_OBJECT_TABLE,
	PG_OBJECT_TABLESPACE,
	PG_OBJECT_TRANSFORM,
	PG_OBJECT_TRIGGER,
	PG_OBJECT_TSCONFIGURATION,
	PG_OBJECT_TSDICTIONARY,
	PG_OBJECT_TSPARSER,
	PG_OBJECT_TSTEMPLATE,
	PG_OBJECT_TYPE,
	PG_OBJECT_USER_MAPPING,
	PG_OBJECT_VIEW
} PGObjectType;

/* ----------------------
 *		Create Schema Statement
 *
 * NOTE: the schemaElts list contains raw parsetrees for component statements
 * of the schema, such as CREATE TABLE, GRANT, etc.  These are analyzed and
 * executed after the schema itself is created.
 * ----------------------
 */
typedef struct PGCreateSchemaStmt {
	PGNodeTag type;
	char *schemaname;                     /* the name of the schema to create */
	PGList *schemaElts;                   /* schema components (list of parsenodes) */
	PGOnCreateConflict onconflict;        /* what to do on create conflict */
} PGCreateSchemaStmt;

typedef enum PGDropBehavior {
	PG_DROP_RESTRICT, /* drop fails if any dependent objects */
	PG_DROP_CASCADE   /* remove dependent objects too */
} PGDropBehavior;

/* ----------------------
 *	Alter Table
 * ----------------------
 */
typedef struct PGAlterTableStmt {
	PGNodeTag type;
	PGRangeVar *relation; /* table to work on */
	PGList *cmds;         /* list of subcommands */
	PGObjectType relkind; /* type of object */
	bool missing_ok;      /* skip error if table missing */
} PGAlterTableStmt;

typedef enum PGAlterTableType {
	PG_AT_AddColumn,                 /* add column */
	PG_AT_AddColumnRecurse,          /* internal to commands/tablecmds.c */
	PG_AT_AddColumnToView,           /* implicitly via CREATE OR REPLACE VIEW */
	PG_AT_ColumnDefault,             /* alter column default */
	PG_AT_DropNotNull,               /* alter column drop not null */
	PG_AT_SetNotNull,                /* alter column set not null */
	PG_AT_SetStatistics,             /* alter column set statistics */
	PG_AT_SetOptions,                /* alter column set ( options ) */
	PG_AT_ResetOptions,              /* alter column reset ( options ) */
	PG_AT_SetStorage,                /* alter column set storage */
	PG_AT_DropColumn,                /* drop column */
	PG_AT_DropColumnRecurse,         /* internal to commands/tablecmds.c */
	PG_AT_AddIndex,                  /* add index */
	PG_AT_ReAddIndex,                /* internal to commands/tablecmds.c */
	PG_AT_AddConstraint,             /* add constraint */
	PG_AT_AddConstraintRecurse,      /* internal to commands/tablecmds.c */
	PG_AT_ReAddConstraint,           /* internal to commands/tablecmds.c */
	PG_AT_AlterConstraint,           /* alter constraint */
	PG_AT_ValidateConstraint,        /* validate constraint */
	PG_AT_ValidateConstraintRecurse, /* internal to commands/tablecmds.c */
	PG_AT_ProcessedConstraint,       /* pre-processed add constraint (local in
								 * parser/parse_utilcmd.c) */
	PG_AT_AddIndexConstraint,        /* add constraint using existing index */
	PG_AT_DropConstraint,            /* drop constraint */
	PG_AT_DropConstraintRecurse,     /* internal to commands/tablecmds.c */
	PG_AT_ReAddComment,              /* internal to commands/tablecmds.c */
	PG_AT_AlterColumnType,           /* alter column type */
	PG_AT_AlterColumnGenericOptions, /* alter column OPTIONS (...) */
	PG_AT_ChangeOwner,               /* change owner */
	PG_AT_ClusterOn,                 /* CLUSTER ON */
	PG_AT_DropCluster,               /* SET WITHOUT CLUSTER */
	PG_AT_SetLogged,                 /* SET LOGGED */
	PG_AT_SetUnLogged,               /* SET UNLOGGED */
	PG_AT_AddOids,                   /* SET WITH OIDS */
	PG_AT_AddOidsRecurse,            /* internal to commands/tablecmds.c */
	PG_AT_DropOids,                  /* SET WITHOUT OIDS */
	PG_AT_SetTableSpace,             /* SET TABLESPACE */
	PG_AT_SetRelOptions,             /* SET (...) -- AM specific parameters */
	PG_AT_ResetRelOptions,           /* RESET (...) -- AM specific parameters */
	PG_AT_ReplaceRelOptions,         /* replace reloption list in its entirety */
	PG_AT_EnableTrig,                /* ENABLE TRIGGER name */
	PG_AT_EnableAlwaysTrig,          /* ENABLE ALWAYS TRIGGER name */
	PG_AT_EnableReplicaTrig,         /* ENABLE REPLICA TRIGGER name */
	PG_AT_DisableTrig,               /* DISABLE TRIGGER name */
	PG_AT_EnableTrigAll,             /* ENABLE TRIGGER ALL */
	PG_AT_DisableTrigAll,            /* DISABLE TRIGGER ALL */
	PG_AT_EnableTrigUser,            /* ENABLE TRIGGER USER */
	PG_AT_DisableTrigUser,           /* DISABLE TRIGGER USER */
	PG_AT_EnableRule,                /* ENABLE RULE name */
	PG_AT_EnableAlwaysRule,          /* ENABLE ALWAYS RULE name */
	PG_AT_EnableReplicaRule,         /* ENABLE REPLICA RULE name */
	PG_AT_DisableRule,               /* DISABLE RULE name */
	PG_AT_AddInherit,                /* INHERIT parent */
	PG_AT_DropInherit,               /* NO INHERIT parent */
	PG_AT_AddOf,                     /* OF <type_name> */
	PG_AT_DropOf,                    /* NOT OF */
	PG_AT_ReplicaIdentity,           /* REPLICA IDENTITY */
	PG_AT_EnableRowSecurity,         /* ENABLE ROW SECURITY */
	PG_AT_DisableRowSecurity,        /* DISABLE ROW SECURITY */
	PG_AT_ForceRowSecurity,          /* FORCE ROW SECURITY */
	PG_AT_NoForceRowSecurity,        /* NO FORCE ROW SECURITY */
	PG_AT_GenericOptions,            /* OPTIONS (...) */
	PG_AT_AttachPartition,           /* ATTACH PARTITION */
	PG_AT_DetachPartition,           /* DETACH PARTITION */
	PG_AT_AddIdentity,               /* ADD IDENTITY */
	PG_AT_SetIdentity,               /* SET identity column options */
	AT_DropIdentity                  /* DROP IDENTITY */
} PGAlterTableType;

typedef struct PGAlterTableCmd /* one subcommand of an ALTER TABLE */
{
	PGNodeTag type;
	PGAlterTableType subtype; /* Type of table alteration to apply */
	char *name;               /* column, constraint, or trigger to act on,
								 * or tablespace */
	PGNode *def;              /* definition of new column, index,
								 * constraint, or parent table */
	PGDropBehavior behavior;  /* RESTRICT or CASCADE for DROP cases */
	bool missing_ok;          /* skip error if missing? */
} PGAlterTableCmd;

/*
 * Note: PGObjectWithArgs carries only the types of the input parameters of the
 * function.  So it is sufficient to identify an existing function, but it
 * is not enough info to define a function nor to call it.
 */
typedef struct PGObjectWithArgs {
	PGNodeTag type;
	PGList *objname;       /* qualified name of function/operator */
	PGList *objargs;       /* list of Typename nodes */
	bool args_unspecified; /* argument list was omitted, so name must
									 * be unique (note that objargs == NIL
									 * means zero args) */
} PGObjectWithArgs;

/* ----------------------
 *		Copy Statement
 *
 * We support "COPY relation FROM file", "COPY relation TO file", and
 * "COPY (query) TO file".  In any given PGCopyStmt, exactly one of "relation"
 * and "query" must be non-NULL.
 * ----------------------
 */
typedef struct PGCopyStmt {
	PGNodeTag type;
	PGRangeVar *relation; /* the relation to copy */
	PGNode *query;        /* the query (SELECT or DML statement with
								 * RETURNING) to copy, as a raw parse tree */
	PGList *attlist;      /* PGList of column names (as Strings), or NIL
								 * for all columns */
	bool is_from;         /* TO or FROM */
	bool is_program;      /* is 'filename' a program to popen? */
	char *filename;       /* filename, or NULL for STDIN/STDOUT */
	PGList *options;      /* PGList of PGDefElem nodes */
} PGCopyStmt;

/* ----------------------
 * SET Statement (includes RESET)
 *
 * "SET var TO DEFAULT" and "RESET var" are semantically equivalent, but we
 * preserve the distinction in VariableSetKind for CreateCommandTag().
 * ----------------------
 */
typedef enum {
	VAR_SET_VALUE,   /* SET var = value */
	VAR_SET_DEFAULT, /* SET var TO DEFAULT */
	VAR_SET_CURRENT, /* SET var FROM CURRENT */
	VAR_SET_MULTI,   /* special case for SET TRANSACTION ... */
	VAR_RESET,       /* RESET var */
	VAR_RESET_ALL    /* RESET ALL */
} VariableSetKind;

typedef enum {
	VAR_SET_SCOPE_LOCAL,   /* SET LOCAL var */
	VAR_SET_SCOPE_SESSION, /* SET SESSION var */
	VAR_SET_SCOPE_GLOBAL,  /* SET GLOBAL var */
	VAR_SET_SCOPE_DEFAULT  /* SET var (same as SET_SESSION) */
} VariableSetScope;

typedef struct PGVariableSetStmt {
	PGNodeTag type;
	VariableSetKind kind;
	VariableSetScope scope;
	char *name;    /* variable to be set */
	PGList *args;  /* PGList of PGAConst nodes */
} PGVariableSetStmt;

/* ----------------------
 * Show Statement
 * ----------------------
 */
typedef struct PGVariableShowStmt {
	PGNodeTag   type;
	char       *name;
	int         is_summary; // whether or not this is a DESCRIBE or a SUMMARIZE
} PGVariableShowStmt;

/* ----------------------
 * Show Statement with Select Statement
 * ----------------------
 */
typedef struct PGVariableShowSelectStmt
{
	PGNodeTag   type;
	PGNode     *stmt;
	char       *name;
	int         is_summary; // whether or not this is a DESCRIBE or a SUMMARIZE
} PGVariableShowSelectStmt;


/* ----------------------
 *		Create Table Statement
 *
 * NOTE: in the raw gram.y output, PGColumnDef and PGConstraint nodes are
 * intermixed in tableElts, and constraints is NIL.  After parse analysis,
 * tableElts contains just ColumnDefs, and constraints contains just
 * PGConstraint nodes (in fact, only PG_CONSTR_CHECK nodes, in the present
 * implementation).
 * ----------------------
 */

typedef struct PGCreateStmt {
	PGNodeTag type;
	PGRangeVar *relation;                 /* relation to create */
	PGList *tableElts;                    /* column definitions (list of PGColumnDef) */
	PGList *inhRelations;                 /* relations to inherit from (list of
										* inhRelation) */
	PGTypeName *ofTypename;               /* OF typename */
	PGList *constraints;                  /* constraints (list of PGConstraint nodes) */
	PGList *options;                      /* options from WITH clause */
	PGOnCommitAction oncommit;            /* what do we do at COMMIT? */
	char *tablespacename;                 /* table space to use, or NULL */
	PGOnCreateConflict onconflict;        /* what to do on create conflict */
} PGCreateStmt;

/* ----------
 * Definitions for constraints in PGCreateStmt
 *
 * Note that column defaults are treated as a type of constraint,
 * even though that's a bit odd semantically.
 *
 * For constraints that use expressions (CONSTR_CHECK, PG_CONSTR_DEFAULT)
 * we may have the expression in either "raw" form (an untransformed
 * parse tree) or "cooked" form (the nodeToString representation of
 * an executable expression tree), depending on how this PGConstraint
 * node was created (by parsing, or by inheritance from an existing
 * relation).  We should never have both in the same node!
 *
 * PG_FKCONSTR_ACTION_xxx values are stored into pg_constraint.confupdtype
 * and pg_constraint.confdeltype columns; PG_FKCONSTR_MATCH_xxx values are
 * stored into pg_constraint.confmatchtype.  Changing the code values may
 * require an initdb!
 *
 * If skip_validation is true then we skip checking that the existing rows
 * in the table satisfy the constraint, and just install the catalog entries
 * for the constraint.  A new FK constraint is marked as valid iff
 * initially_valid is true.  (Usually skip_validation and initially_valid
 * are inverses, but we can set both true if the table is known empty.)
 *
 * PGConstraint attributes (DEFERRABLE etc) are initially represented as
 * separate PGConstraint nodes for simplicity of parsing.  parse_utilcmd.c makes
 * a pass through the constraints list to insert the info into the appropriate
 * PGConstraint node.
 * ----------
 */

typedef enum PGConstrType /* types of constraints */
{ PG_CONSTR_NULL,         /* not standard SQL, but a lot of people
								 * expect it */
  PG_CONSTR_NOTNULL,
  PG_CONSTR_DEFAULT,
  PG_CONSTR_IDENTITY,
  PG_CONSTR_CHECK,
  PG_CONSTR_PRIMARY,
  PG_CONSTR_UNIQUE,
  PG_CONSTR_EXCLUSION,
  PG_CONSTR_FOREIGN,
  PG_CONSTR_ATTR_DEFERRABLE, /* attributes for previous constraint node */
  PG_CONSTR_ATTR_NOT_DEFERRABLE,
  PG_CONSTR_ATTR_DEFERRED,
  PG_CONSTR_ATTR_IMMEDIATE,
  PG_CONSTR_COMPRESSION,
  PG_CONSTR_GENERATED_VIRTUAL,
  PG_CONSTR_GENERATED_STORED,
  } PGConstrType;

/* Foreign key action codes */
#define PG_FKCONSTR_ACTION_NOACTION 'a'
#define PG_FKCONSTR_ACTION_RESTRICT 'r'
#define PG_FKCONSTR_ACTION_CASCADE 'c'
#define PG_FKCONSTR_ACTION_SETNULL 'n'
#define PG_FKCONSTR_ACTION_SETDEFAULT 'd'

/* Foreign key matchtype codes */
#define PG_FKCONSTR_MATCH_FULL 'f'
#define PG_FKCONSTR_MATCH_PARTIAL 'p'
#define PG_FKCONSTR_MATCH_SIMPLE 's'

typedef struct PGConstraint {
	PGNodeTag type;
	PGConstrType contype; /* see above */

	/* Fields used for most/all constraint types: */
	char *conname;     /* PGConstraint name, or NULL if unnamed */
	bool deferrable;   /* DEFERRABLE? */
	bool initdeferred; /* INITIALLY DEFERRED? */
	int location;      /* token location, or -1 if unknown */

	/* Fields used for constraints with expressions (CHECK and DEFAULT): */
	bool is_no_inherit; /* is constraint non-inheritable? */
	PGNode *raw_expr;   /* expr, as untransformed parse tree */
	char *cooked_expr;  /* expr, as nodeToString representation */
	char generated_when;

	/* Fields used for unique constraints (UNIQUE and PRIMARY KEY): */
	PGList *keys; /* String nodes naming referenced column(s) */

	/* Fields used for EXCLUSION constraints: */
	PGList *exclusions; /* list of (PGIndexElem, operator name) pairs */

	/* Fields used for index constraints (UNIQUE, PRIMARY KEY, EXCLUSION): */
	PGList *options;  /* options from WITH clause */
	char *indexname;  /* existing index to use; otherwise NULL */
	char *indexspace; /* index tablespace; NULL for default */
	/* These could be, but currently are not, used for UNIQUE/PKEY: */
	char *access_method;  /* index access method; NULL for default */
	PGNode *where_clause; /* partial index predicate */

	/* Fields used for FOREIGN KEY constraints: */
	PGRangeVar *pktable;   /* Primary key table */
	PGList *fk_attrs;      /* Attributes of foreign key */
	PGList *pk_attrs;      /* Corresponding attrs in PK table */
	char fk_matchtype;     /* FULL, PARTIAL, SIMPLE */
	char fk_upd_action;    /* ON UPDATE action */
	char fk_del_action;    /* ON DELETE action */
	PGList *old_conpfeqop; /* pg_constraint.conpfeqop of my former self */
	PGOid old_pktable_oid; /* pg_constraint.confrelid of my former
									 * self */

	/* Fields used for constraints that allow a NOT VALID specification */
	bool skip_validation; /* skip validation of existing rows? */
	bool initially_valid; /* mark the new constraint as valid? */


	/* Field Used for COMPRESSION constraint */
	char *compression_name;  /* existing index to use; otherwise NULL */

} PGConstraint;

/* ----------------------
 *		{Create|Alter} SEQUENCE Statement
 * ----------------------
 */

typedef struct PGCreateSeqStmt {
	PGNodeTag type;
	PGRangeVar *sequence; /* the sequence to create */
	PGList *options;
	PGOid ownerId; /* ID of owner, or InvalidOid for default */
	bool for_identity;
	PGOnCreateConflict onconflict;        /* what to do on create conflict */
} PGCreateSeqStmt;

typedef struct PGAlterSeqStmt {
	PGNodeTag type;
	PGRangeVar *sequence; /* the sequence to alter */
	PGList *options;
	bool for_identity;
	bool missing_ok; /* skip error if a role is missing? */
} PGAlterSeqStmt;

/* ----------------------
 *		CREATE FUNCTION Statement
 * ----------------------
 */

typedef struct PGCreateFunctionStmt {
	PGNodeTag type;
	PGRangeVar *name;
	PGList *params;
	PGNode *function;
  	PGNode *query;
	char relpersistence;
} PGCreateFunctionStmt;

/* ----------------------
 *		Drop Table|Sequence|View|Index|Type|Domain|Conversion|Schema Statement
 * ----------------------
 */

typedef struct PGDropStmt {
	PGNodeTag type;
	PGList *objects;         /* list of names */
	PGObjectType removeType; /* object type */
	PGDropBehavior behavior; /* RESTRICT or CASCADE behavior */
	bool missing_ok;         /* skip error if object is missing? */
	bool concurrent;         /* drop index concurrently? */
} PGDropStmt;

/* ----------------------
 *		Create PGIndex Statement
 *
 * This represents creation of an index and/or an associated constraint.
 * If isconstraint is true, we should create a pg_constraint entry along
 * with the index.  But if indexOid isn't InvalidOid, we are not creating an
 * index, just a UNIQUE/PKEY constraint using an existing index.  isconstraint
 * must always be true in this case, and the fields describing the index
 * properties are empty.
 * ----------------------
 */
typedef struct PGIndexStmt {
	PGNodeTag type;
	char *idxname;          /* name of new index, or NULL for default */
	PGRangeVar *relation;   /* relation to build index on */
	char *accessMethod;     /* name of access method (eg. btree) */
	char *tableSpace;       /* tablespace, or NULL for default */
	PGList *indexParams;    /* columns to index: a list of PGIndexElem */
	PGList *options;        /* WITH clause options: a list of PGDefElem */
	PGNode *whereClause;    /* qualification (partial-index predicate) */
	PGList *excludeOpNames; /* exclusion operator names, or NIL if none */
	char *idxcomment;       /* comment to apply to index, or NULL */
	PGOid indexOid;         /* OID of an existing index, if any */
	PGOid oldNode;          /* relfilenode of existing storage, if any */
	bool unique;            /* is index unique? */
	bool primary;           /* is index a primary key? */
	bool isconstraint;      /* is it for a pkey/unique constraint? */
	bool deferrable;        /* is the constraint DEFERRABLE? */
	bool initdeferred;      /* is the constraint INITIALLY DEFERRED? */
	bool transformed;       /* true when transformIndexStmt is finished */
	bool concurrent;        /* should this be a concurrent index build? */
	PGOnCreateConflict onconflict;        /* what to do on create conflict */
} PGIndexStmt;

/* ----------------------
 *		Alter Object Rename Statement
 * ----------------------
 */
typedef struct PGRenameStmt {
	PGNodeTag type;
	PGObjectType renameType;   /* PG_OBJECT_TABLE, PG_OBJECT_COLUMN, etc */
	PGObjectType relationType; /* if column name, associated relation type */
	PGRangeVar *relation;      /* in case it's a table */
	PGNode *object;            /* in case it's some other object */
	char *subname;             /* name of contained object (column, rule,
								 * trigger, etc) */
	char *newname;             /* the new name */
	PGDropBehavior behavior;   /* RESTRICT or CASCADE behavior */
	bool missing_ok;           /* skip error if missing? */
} PGRenameStmt;

/* ----------------------
 *		ALTER object SET SCHEMA Statement
 * ----------------------
 */
typedef struct PGAlterObjectSchemaStmt {
	PGNodeTag type;
	PGObjectType objectType; /* PG_OBJECT_TABLE, PG_OBJECT_TYPE, etc */
	PGRangeVar *relation;    /* in case it's a table */
	PGNode *object;          /* in case it's some other object */
	char *newschema;         /* the new schema */
	bool missing_ok;         /* skip error if missing? */
} PGAlterObjectSchemaStmt;

/* ----------------------
 *		{Begin|Commit|Rollback} Transaction Statement
 * ----------------------
 */
typedef enum PGTransactionStmtKind {
	PG_TRANS_STMT_BEGIN,
	PG_TRANS_STMT_START, /* semantically identical to BEGIN */
	PG_TRANS_STMT_COMMIT,
	PG_TRANS_STMT_ROLLBACK,
	PG_TRANS_STMT_SAVEPOINT,
	PG_TRANS_STMT_RELEASE,
	PG_TRANS_STMT_ROLLBACK_TO,
	PG_TRANS_STMT_PREPARE,
	PG_TRANS_STMT_COMMIT_PREPARED,
	TRANS_STMT_ROLLBACK_PREPARED
} PGTransactionStmtKind;

typedef struct PGTransactionStmt {
	PGNodeTag type;
	PGTransactionStmtKind kind; /* see above */
	PGList *options;            /* for BEGIN/START and savepoint commands */
	char *gid;                  /* for two-phase-commit related commands */
} PGTransactionStmt;

/* ----------------------
 *		Create View Statement
 * ----------------------
 */
typedef enum PGViewCheckOption { PG_NO_CHECK_OPTION, PG_LOCAL_CHECK_OPTION, CASCADED_CHECK_OPTION } PGViewCheckOption;

typedef struct PGViewStmt {
	PGNodeTag type;
	PGRangeVar *view;                  /* the view to be created */
	PGList *aliases;                   /* target column names */
	PGNode *query;                     /* the SELECT query (as a raw parse tree) */
	PGOnCreateConflict onconflict;     /* what to do on create conflict */
	PGList *options;                   /* options from WITH clause */
	PGViewCheckOption withCheckOption; /* WITH CHECK OPTION */
} PGViewStmt;

/* ----------------------
 *		Load Statement
 * ----------------------
 */

typedef enum PGLoadInstallType { PG_LOAD_TYPE_LOAD,  PG_LOAD_TYPE_INSTALL, PG_LOAD_TYPE_FORCE_INSTALL } PGLoadInstallType;


typedef struct PGLoadStmt {
	PGNodeTag type;
	const char *filename; /* file to load */
	PGLoadInstallType load_type;
} PGLoadStmt;

/* ----------------------
 *		Vacuum and Analyze Statements
 *
 * Even though these are nominally two statements, it's convenient to use
 * just one node type for both.  Note that at least one of PG_VACOPT_VACUUM
 * and PG_VACOPT_ANALYZE must be set in options.
 * ----------------------
 */
typedef enum PGVacuumOption {
	PG_VACOPT_VACUUM = 1 << 0,               /* do VACUUM */
	PG_VACOPT_ANALYZE = 1 << 1,              /* do ANALYZE */
	PG_VACOPT_VERBOSE = 1 << 2,              /* print progress info */
	PG_VACOPT_FREEZE = 1 << 3,               /* FREEZE option */
	PG_VACOPT_FULL = 1 << 4,                 /* FULL (non-concurrent) vacuum */
	PG_VACOPT_NOWAIT = 1 << 5,               /* don't wait to get lock (autovacuum only) */
	PG_VACOPT_SKIPTOAST = 1 << 6,            /* don't process the TOAST table, if any */
	PG_VACOPT_DISABLE_PAGE_SKIPPING = 1 << 7 /* don't skip any pages */
} PGVacuumOption;

typedef struct PGVacuumStmt {
	PGNodeTag type;
	int options;          /* OR of PGVacuumOption flags */
	PGRangeVar *relation; /* single table to process, or NULL */
	PGList *va_cols;      /* list of column names, or NIL for all */
} PGVacuumStmt;

/* ----------------------
 *		Explain Statement
 *
 * The "query" field is initially a raw parse tree, and is converted to a
 * PGQuery node during parse analysis.  Note that rewriting and planning
 * of the query are always postponed until execution.
 * ----------------------
 */
typedef struct PGExplainStmt {
	PGNodeTag type;
	PGNode *query;   /* the query (see comments above) */
	PGList *options; /* list of PGDefElem nodes */
} PGExplainStmt;

/* ----------------------
 *		CREATE TABLE AS Statement (a/k/a SELECT INTO)
 *
 * A query written as CREATE TABLE AS will produce this node type natively.
 * A query written as SELECT ... INTO will be transformed to this form during
 * parse analysis.
 * A query written as CREATE MATERIALIZED view will produce this node type,
 * during parse analysis, since it needs all the same data.
 *
 * The "query" field is handled similarly to EXPLAIN, though note that it
 * can be a SELECT or an EXECUTE, but not other DML statements.
 * ----------------------
 */
typedef struct PGCreateTableAsStmt {
	PGNodeTag type;
	PGNode *query;        /* the query (see comments above) */
	PGIntoClause *into;   /* destination table */
	PGObjectType relkind; /* PG_OBJECT_TABLE or PG_OBJECT_MATVIEW */
	bool is_select_into;  /* it was written as SELECT INTO */
	PGOnCreateConflict onconflict;        /* what to do on create conflict */
} PGCreateTableAsStmt;

/* ----------------------
 * Checkpoint Statement
 * ----------------------
 */
typedef struct PGCheckPointStmt {
	PGNodeTag type;
	bool force;
} PGCheckPointStmt;

/* ----------------------
 *		PREPARE Statement
 * ----------------------
 */
typedef struct PGPrepareStmt {
	PGNodeTag type;
	char *name;       /* Name of plan, arbitrary */
	PGList *argtypes; /* Types of parameters (PGList of PGTypeName) */
	PGNode *query;    /* The query itself (as a raw parsetree) */
} PGPrepareStmt;

/* ----------------------
 *		EXECUTE Statement
 * ----------------------
 */

typedef struct PGExecuteStmt {
	PGNodeTag type;
	char *name;     /* The name of the plan to execute */
	PGList *params; /* Values to assign to parameters */
} PGExecuteStmt;

/* ----------------------
 *		DEALLOCATE Statement
 * ----------------------
 */
typedef struct PGDeallocateStmt {
	PGNodeTag type;
	char *name; /* The name of the plan to remove */
	            /* NULL means DEALLOCATE ALL */
} PGDeallocateStmt;

/* ----------------------
 * PRAGMA statements
 * Three types of pragma statements:
 * PRAGMA pragma_name;          (NOTHING)
 * PRAGMA pragma_name='param';  (ASSIGNMENT)
 * PRAGMA pragma_name('param'); (CALL)
 * ----------------------
 */
typedef enum { PG_PRAGMA_TYPE_NOTHING, PG_PRAGMA_TYPE_ASSIGNMENT, PG_PRAGMA_TYPE_CALL } PGPragmaKind;

typedef struct PGPragmaStmt {
	PGNodeTag type;
	PGPragmaKind kind;
	char *name;   /* variable to be set */
	PGList *args; /* PGList of PGAConst nodes */
} PGPragmaStmt;

/* ----------------------
 *		CALL Statement
 * ----------------------
 */

typedef struct PGCallStmt {
	PGNodeTag type;
	PGNode *func;
} PGCallStmt;

/* ----------------------
 *		EXPORT/IMPORT Statements
 * ----------------------
 */

typedef struct PGExportStmt {
	PGNodeTag type;
	char *filename;       /* filename */
	PGList *options;      /* PGList of PGDefElem nodes */
} PGExportStmt;

typedef struct PGImportStmt {
	PGNodeTag type;
	char *filename;       /* filename */
} PGImportStmt;

/* ----------------------
 *		Interval Constant
 * ----------------------
 */
typedef struct PGIntervalConstant {
	PGNodeTag type;
	int val_type;         /* interval constant type, either duckdb_libpgquery::T_PGString, duckdb_libpgquery::T_PGInteger or duckdb_libpgquery::T_PGAExpr */
	char *sval;           /* duckdb_libpgquery::T_PGString */
	int ival;             /* duckdb_libpgquery::T_PGString */
	PGNode *eval;         /* duckdb_libpgquery::T_PGAExpr */
	PGList *typmods;      /* how to interpret the interval constant (year, month, day, etc)  */
	int location;         /* token location, or -1 if unknown */
} PGIntervalConstant;

/* ----------------------
 *		Sample Options
 * ----------------------
 */
typedef struct PGSampleSize {
	PGNodeTag type;
	bool is_percentage;   /* whether or not the sample size is expressed in row numbers or a percentage */
	PGValue sample_size;  /* sample size */
} PGSampleSize;

typedef struct PGSampleOptions {
	PGNodeTag type;
	PGNode *sample_size;      /* the size of the sample to take */
	char *method;             /* sample method, or NULL for default */
	int seed;                 /* seed, or NULL for default; */
	int location;             /* token location, or -1 if unknown */
} PGSampleOptions;

/* ----------------------
 *      Limit Percentage
 * ----------------------
 */
typedef struct PGLimitPercent {
	PGNodeTag type;
    PGNode* limit_percent;  /* limit percent */
} PGLimitPercent;

/* ----------------------
 *		Lambda Function (or Arrow Operator)
 * ----------------------
 */
typedef struct PGLambdaFunction {
	PGNodeTag type;
	PGNode *lhs;                 /* parameter expression */
	PGNode *rhs;                 /* lambda expression */
	int location;                /* token location, or -1 if unknown */
} PGLambdaFunction;

/* ----------------------
 *		Positional Reference
 * ----------------------
 */
typedef struct PGPositionalReference {
	PGNodeTag type;
	int position;
	int location;                /* token location, or -1 if unknown */
} PGPositionalReference;

/* ----------------------
 *		Type Statement
 * ----------------------
 */

typedef enum { PG_NEWTYPE_NONE, PG_NEWTYPE_ENUM, PG_NEWTYPE_ALIAS } PGNewTypeKind;

typedef struct PGCreateTypeStmt
{
	PGNodeTag		type;
	PGNewTypeKind	kind;
	PGList	   *typeName;		/* qualified name (list of Value strings) */
	PGList	   *vals;			/* enum values (list of Value strings) */
	PGTypeName *ofType;			/* original type of alias name */
} PGCreateTypeStmt;





}
