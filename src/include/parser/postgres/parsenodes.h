//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parsenodes.h
//
// Identification: src/include/parser/parsenodes.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

#include "nodes.h"
#include "pg_list.h"

typedef enum SetOperation {
	SETOP_NONE = 0,
	SETOP_UNION,
	SETOP_INTERSECT,
	SETOP_EXCEPT
} SetOperation;

typedef struct Alias {
	NodeTag type;
	char *aliasname; /* aliased rel name (never qualified) */
	List *colnames;  /* optional list of column aliases */
} Alias;

typedef enum InhOption {
	INH_NO,     /* Do NOT scan child tables */
	INH_YES,    /* DO scan child tables */
	INH_DEFAULT /* Use current SQL_inheritance option */
} InhOption;

typedef enum BoolExprType { AND_EXPR, OR_EXPR, NOT_EXPR } BoolExprType;

typedef struct Expr {
	NodeTag type;
} Expr;

/*
 * SubLink
 *
 * A SubLink represents a subselect appearing in an expression, and in some
 * cases also the combining operator(s) just above it.  The subLinkType
 * indicates the form of the expression represented:
 *	EXISTS_SUBLINK		EXISTS(SELECT ...)
 *	ALL_SUBLINK			(lefthand) op ALL (SELECT ...)
 *	ANY_SUBLINK			(lefthand) op ANY (SELECT ...)
 *	ROWCOMPARE_SUBLINK	(lefthand) op (SELECT ...)
 *	EXPR_SUBLINK		(SELECT with single targetlist item ...)
 *	MULTIEXPR_SUBLINK	(SELECT with multiple targetlist items ...)
 *	ARRAY_SUBLINK		ARRAY(SELECT with single targetlist item ...)
 *	CTE_SUBLINK			WITH query (never actually part of an expression)
 * For ALL, ANY, and ROWCOMPARE, the lefthand is a list of expressions of the
 * same length as the subselect's targetlist.  ROWCOMPARE will *always* have
 * a list with more than one entry; if the subselect has just one target
 * then the parser will create an EXPR_SUBLINK instead (and any operator
 * above the subselect will be represented separately).
 * ROWCOMPARE, EXPR, and MULTIEXPR require the subselect to deliver at most
 * one row (if it returns no rows, the result is NULL).
 * ALL, ANY, and ROWCOMPARE require the combining operators to deliver boolean
 * results.  ALL and ANY combine the per-row results using AND and OR
 * semantics respectively.
 * ARRAY requires just one target column, and creates an array of the target
 * column's type using any number of rows resulting from the subselect.
 *
 * SubLink is classed as an Expr node, but it is not actually executable;
 * it must be replaced in the expression tree by a SubPlan node during
 * planning.
 *
 * NOTE: in the raw output of gram.y, testexpr contains just the raw form
 * of the lefthand expression (if any), and operName is the String name of
 * the combining operator.  Also, subselect is a raw parsetree.  During parse
 * analysis, the parser transforms testexpr into a complete boolean expression
 * that compares the lefthand value(s) to PARAM_SUBLINK nodes representing
 *the
 * output columns of the subselect.  And subselect is transformed to a Query.
 * This is the representation seen in saved rules and in the rewriter.
 *
 * In EXISTS, EXPR, MULTIEXPR, and ARRAY SubLinks, testexpr and operName
 * are unused and are always null.
 *
 * subLinkId is currently used only for MULTIEXPR SubLinks, and is zero in
 * other SubLinks.  This number identifies different multiple-assignment
 * subqueries within an UPDATE statement's SET list.  It is unique only
 * within a particular targetlist.  The output column(s) of the MULTIEXPR
 * are referenced by PARAM_MULTIEXPR Params appearing elsewhere in the tlist.
 *
 * The CTE_SUBLINK case never occurs in actual SubLink nodes, but it is used
 * in SubPlans generated for WITH subqueries.
 */
typedef enum SubLinkType {
	EXISTS_SUBLINK,
	ALL_SUBLINK,
	ANY_SUBLINK,
	ROWCOMPARE_SUBLINK,
	EXPR_SUBLINK,
	MULTIEXPR_SUBLINK,
	ARRAY_SUBLINK,
	CTE_SUBLINK /* for SubPlans only */
} SubLinkType;

typedef struct SubLink {
	Expr xpr;
	SubLinkType subLinkType; /* see above */
	int subLinkId;           /* ID (1..n); 0 if not MULTIEXPR */
	Node *testexpr;          /* outer-query test for ALL/ANY/ROWCOMPARE */
	List *operName;          /* originally specified operator name */
	Node *subselect;         /* subselect as Query* or raw parsetree */
	int location;            /* token location, or -1 if unknown */
} SubLink;

typedef struct BoolExpr {
	Expr xpr;
	BoolExprType boolop;
	List *args;   /* arguments to this expression */
	int location; /* token location, or -1 if unknown */
} BoolExpr;

typedef enum A_Expr_Kind {
	AEXPR_OP,              /* normal operator */
	AEXPR_OP_ANY,          /* scalar op ANY (array) */
	AEXPR_OP_ALL,          /* scalar op ALL (array) */
	AEXPR_DISTINCT,        /* IS DISTINCT FROM - name must be "=" */
	AEXPR_NULLIF,          /* NULLIF - name must be "=" */
	AEXPR_OF,              /* IS [NOT] OF - name must be "=" or "<>" */
	AEXPR_IN,              /* [NOT] IN - name must be "=" or "<>" */
	AEXPR_LIKE,            /* [NOT] LIKE - name must be "~~" or "!~~" */
	AEXPR_ILIKE,           /* [NOT] ILIKE - name must be "~~*" or "!~~*" */
	AEXPR_SIMILAR,         /* [NOT] SIMILAR - name must be "~" or "!~" */
	AEXPR_BETWEEN,         /* name must be "BETWEEN" */
	AEXPR_NOT_BETWEEN,     /* name must be "NOT BETWEEN" */
	AEXPR_BETWEEN_SYM,     /* name must be "BETWEEN SYMMETRIC" */
	AEXPR_NOT_BETWEEN_SYM, /* name must be "NOT BETWEEN SYMMETRIC" */
	AEXPR_PAREN            /* nameless dummy node for parentheses */
} A_Expr_Kind;

typedef struct A_Expr {
	NodeTag type;
	A_Expr_Kind kind; /* see above */
	List *name;       /* possibly-qualified name of operator */
	Node *lexpr;      /* left argument, or NULL if none */
	Node *rexpr;      /* right argument, or NULL if none */
	int location;     /* token location, or -1 if unknown */
} A_Expr;

typedef enum NullTestType { IS_NULL, IS_NOT_NULL } NullTestType;

typedef struct NullTest {
	Expr xpr;
	Expr *arg;                 /* input expression */
	NullTestType nulltesttype; /* IS NULL, IS NOT NULL */
	bool argisrow;             /* T if input is of a composite type */
	int location;              /* token location, or -1 if unknown */
} NullTest;

typedef struct JoinExpr {
	NodeTag type;
	JoinType jointype; /* type of join */
	bool isNatural;    /* Natural join? Will need to shape table */
	Node *larg;        /* left subtree */
	Node *rarg;        /* right subtree */
	List *usingClause; /* USING clause, if any (list of String) */
	Node *quals;       /* qualifiers on join, if any */
	Alias *alias;      /* user-written alias clause, if any */
	int rtindex;       /* RT index assigned for join, or 0 */
} JoinExpr;

typedef struct CaseExpr {
	Expr xpr;
	Oid casetype;    /* type of expression result */
	Oid casecollid;  /* OID of collation, or InvalidOid if none */
	Expr *arg;       /* implicit equality comparison argument */
	List *args;      /* the arguments (list of WHEN clauses) */
	Expr *defresult; /* the default result (ELSE clause) */
	int location;    /* token location, or -1 if unknown */
} CaseExpr;

typedef struct CaseWhen {
	Expr xpr;
	Expr *expr;   /* condition expression */
	Expr *result; /* substitution result */
	int location; /* token location, or -1 if unknown */
} CaseWhen;

typedef struct RangeSubselect {
	NodeTag type;
	bool lateral;   /* does it have LATERAL prefix? */
	Node *subquery; /* the untransformed sub-select clause */
	Alias *alias;   /* table alias & optional column aliases */
} RangeSubselect;

typedef struct RangeFunction {
	NodeTag type;
	bool lateral;     /* does it have LATERAL prefix? */
	bool ordinality;  /* does it have WITH ORDINALITY suffix? */
	bool is_rowsfrom; /* is result of ROWS FROM() syntax? */
	List *functions;  /* per-function information, see above */
	Alias *alias;     /* table alias & optional column aliases */
	List *coldeflist; /* list of ColumnDef nodes to describe result
	                   * of function returning RECORD */
} RangeFunction;

typedef struct RangeVar {
	NodeTag type;
	char *catalogname;   /* the catalog (database) name, or NULL */
	char *schemaname;    /* the schema name, or NULL */
	char *relname;       /* the relation/sequence name */
	InhOption inhOpt;    /* expand rel by inheritance? recursively act
	                      * on children? */
	char relpersistence; /* see RELPERSISTENCE_* in pg_class.h */
	Alias *alias;        /* table alias & optional column aliases */
	int location;        /* token location, or -1 if unknown */
} RangeVar;

typedef struct WithClause {
	NodeTag type;
	List *ctes;     /* list of CommonTableExprs */
	bool recursive; /* true = WITH RECURSIVE */
	int location;   /* token location, or -1 if unknown */
} WithClause;

typedef struct CommonTableExpr {
	NodeTag type;
	char *ctename;       /* query name (never qualified) */
	List *aliascolnames; /* optional list of column names */
	/* SelectStmt/InsertStmt/etc before parse analysis, Query afterwards: */
	Node *ctequery; /* the CTE's subquery */
	int location;   /* token location, or -1 if unknown */
	/* These fields are set during parse analysis: */
	bool cterecursive;      /* is this CTE actually recursive? */
	int cterefcount;        /* number of RTEs referencing this CTE
	                         * (excluding internal self-references) */
	List *ctecolnames;      /* list of output column names */
	List *ctecoltypes;      /* OID list of output column type OIDs */
	List *ctecoltypmods;    /* integer list of output column typmods */
	List *ctecolcollations; /* OID list of column collation OIDs */
} CommonTableExpr;

typedef enum OnCommitAction {
	ONCOMMIT_NOOP,          /* No ON COMMIT clause (do nothing) */
	ONCOMMIT_PRESERVE_ROWS, /* ON COMMIT PRESERVE ROWS (do nothing) */
	ONCOMMIT_DELETE_ROWS,   /* ON COMMIT DELETE ROWS */
	ONCOMMIT_DROP           /* ON COMMIT DROP */
} OnCommitAction;

typedef struct IntoClause {
	NodeTag type;

	RangeVar *rel;           /* target relation name */
	List *colNames;          /* column names to assign, or NIL */
	List *options;           /* options from WITH clause */
	OnCommitAction onCommit; /* what do we do at COMMIT? */
	char *tableSpaceName;    /* table space to use, or NULL */
	Node *viewQuery;         /* materialized view's SELECT query */
	bool skipData;           /* true for WITH NO DATA */
} IntoClause;

typedef enum SortByDir {
	SORTBY_DEFAULT,
	SORTBY_ASC,
	SORTBY_DESC,
	SORTBY_USING /* not allowed in CREATE INDEX ... */
} SortByDir;

typedef enum SortByNulls {
	SORTBY_NULLS_DEFAULT,
	SORTBY_NULLS_FIRST,
	SORTBY_NULLS_LAST
} SortByNulls;

typedef struct SortBy {
	NodeTag type;
	Node *node;               /* expression to sort on */
	SortByDir sortby_dir;     /* ASC/DESC/USING/default */
	SortByNulls sortby_nulls; /* NULLS FIRST/LAST */
	List *useOp;              /* name of op to use, if SORTBY_USING */
	int location;             /* operator location, or -1 if none/unknown */
} SortBy;

typedef struct InferClause {
	NodeTag type;
	List *indexElems;  /* IndexElems to infer unique index */
	Node *whereClause; /* qualification (partial-index predicate) */
	char *conname;     /* Constraint name, or NULL if unnamed */
	int location;      /* token location, or -1 if unknown */
} InferClause;

typedef struct OnConflictClause {
	NodeTag type;
	OnConflictAction action; /* DO NOTHING or UPDATE? */
	InferClause *infer;      /* Optional index inference clause */
	List *targetList;        /* the target list (of ResTarget) */
	Node *whereClause;       /* qualifications */
	int location;            /* token location, or -1 if unknown */
} OnConflictClause;

typedef struct InsertStmt {
	NodeTag type;
	RangeVar *relation; /* relation to insert into */
	List *cols;         /* optional: names of the target columns */
	Node *selectStmt;   /* the source SELECT/VALUES, or NULL */
	OnConflictClause *onConflictClause; /* ON CONFLICT clause */
	List *returningList;                /* list of expressions to return */
	WithClause *withClause;             /* WITH clause */
} InsertStmt;

typedef struct SelectStmt {
	NodeTag type;

	/*
	 * These fields are used only in "leaf" SelectStmts.
	 */
	List *distinctClause;   /* NULL, list of DISTINCT ON exprs, or
	                         * lcons(NIL,NIL) for all (SELECT DISTINCT) */
	IntoClause *intoClause; /* target for SELECT INTO */
	List *targetList;       /* the target list (of ResTarget) */
	List *fromClause;       /* the FROM clause */
	Node *whereClause;      /* WHERE qualification */
	List *groupClause;      /* GROUP BY clauses */
	Node *havingClause;     /* HAVING conditional-expression */
	List *windowClause;     /* WINDOW window_name AS (...), ... */

	/*
	 * In a "leaf" node representing a VALUES list, the above fields are all
	 * null, and instead this field is set.  Note that the elements of the
	 * sublists are just expressions, without ResTarget decoration. Also note
	 * that a list element can be DEFAULT (represented as a SetToDefault
	 * node), regardless of the context of the VALUES list. It's up to parse
	 * analysis to reject that where not valid.
	 */
	List *valuesLists; /* untransformed list of expression lists */

	/*
	 * These fields are used in both "leaf" SelectStmts and upper-level
	 * SelectStmts.
	 */
	List *sortClause;       /* sort clause (a list of SortBy's) */
	Node *limitOffset;      /* # of result tuples to skip */
	Node *limitCount;       /* # of result tuples to return */
	List *lockingClause;    /* FOR UPDATE (list of LockingClause's) */
	WithClause *withClause; /* WITH clause */

	/*
	 * These fields are used only in upper-level SelectStmts.
	 */
	SetOperation op;         /* type of set op */
	bool all;                /* ALL specified? */
	struct SelectStmt *larg; /* left child */
	struct SelectStmt *rarg; /* right child */
	/* Eventually add fields for CORRESPONDING spec here */
} SelectStmt;

/*
 * Explain Statement
 *
 * The "query" field is initially a raw parse tree, and is converted to a
 * Query node during parse analysis.  Note that rewriting and planning
 * of the query are always postponed until execution.
 */
typedef struct ExplainStmt {
	NodeTag type;
	Node *query;   /* the query (see comments above) */
	List *options; /* list of DefElem nodes */
} ExplainStmt;

typedef struct TypeName {
	NodeTag type;
	List *names;       /* qualified name (list of Value strings) */
	Oid typeOid;       /* type identified by OID */
	bool setof;        /* is a set? */
	bool pct_type;     /* %TYPE specified? */
	List *typmods;     /* type modifier expression(s) */
	int typemod;       /* prespecified type modifier */
	List *arrayBounds; /* array bounds */
	int location;      /* token location, or -1 if unknown */
} TypeName;

typedef struct IndexElem {
	NodeTag type;
	char *name;                 /* name of attribute to index, or NULL */
	Node *expr;                 /* expression to index, or NULL */
	char *indexcolname;         /* name for index column; NULL = default */
	List *collation;            /* name of collation; NIL = default */
	List *opclass;              /* name of desired opclass; NIL = default */
	SortByDir ordering;         /* ASC/DESC/default */
	SortByNulls nulls_ordering; /* FIRST/LAST/default */
} IndexElem;

typedef struct IndexStmt {
	NodeTag type;
	char *idxname;        /* name of new index, or NULL for default */
	RangeVar *relation;   /* relation to build index on */
	char *accessMethod;   /* name of access method (eg. btree) */
	char *tableSpace;     /* tablespace, or NULL for default */
	List *indexParams;    /* columns to index: a list of IndexElem */
	List *options;        /* WITH clause options: a list of DefElem */
	Node *whereClause;    /* qualification (partial-index predicate) */
	List *excludeOpNames; /* exclusion operator names, or NIL if none */
	char *idxcomment;     /* comment to apply to index, or NULL */
	Oid indexOid;         /* OID of an existing index, if any */
	Oid oldNode;          /* relfilenode of existing storage, if any */
	bool unique;          /* is index unique? */
	bool primary;         /* is index a primary key? */
	bool isconstraint;    /* is it for a pkey/unique constraint? */
	bool deferrable;      /* is the constraint DEFERRABLE? */
	bool initdeferred;    /* is the constraint INITIALLY DEFERRED? */
	bool transformed;     /* true when transformIndexStmt is finished */
	bool concurrent;      /* should this be a concurrent index build? */
	bool if_not_exists;   /* just do nothing if index already exists? */
} IndexStmt;

typedef struct CreateTrigStmt {
	NodeTag type;
	char *trigname;     /* TRIGGER's name */
	RangeVar *relation; /* relation trigger is on */
	List *funcname;     /* qual. name of function to call */
	List *args;         /* list of (T_String) Values or NIL */
	bool row;           /* ROW/STATEMENT */
	/* timing uses the TRIGGER_TYPE bits defined in catalog/pg_trigger.h */
	int16_t timing; /* BEFORE, AFTER, or INSTEAD */
	/* events uses the TRIGGER_TYPE bits defined in catalog/pg_trigger.h */
	int16_t events;    /* "OR" of INSERT/UPDATE/DELETE/TRUNCATE */
	List *columns;     /* column names, or NIL for all columns */
	Node *whenClause;  /* qual expression, or NULL if none */
	bool isconstraint; /* This is a constraint trigger */
	/* The remaining fields are only used for constraint triggers */
	bool deferrable;     /* [NOT] DEFERRABLE */
	bool initdeferred;   /* INITIALLY {DEFERRED|IMMEDIATE} */
	RangeVar *constrrel; /* opposite relation, if RI trigger */
} CreateTrigStmt;

typedef struct ColumnDef {
	NodeTag type;
	char *colname;        /* name of column */
	TypeName *typeName;   /* type of column */
	int inhcount;         /* number of times column is inherited */
	bool is_local;        /* column has local (non-inherited) def'n */
	bool is_not_null;     /* NOT NULL constraint specified? */
	bool is_from_type;    /* column definition came from table type */
	char storage;         /* attstorage setting, or 0 for default */
	Node *raw_default;    /* default value (untransformed parse tree) */
	Node *cooked_default; /* default value (transformed expr tree) */
	Node *collClause;     /* untransformed COLLATE spec, if any */
	Oid collOid;          /* collation OID (InvalidOid if not set) */
	List *constraints;    /* other constraints on column */
	List *fdwoptions;     /* per-column FDW options */
	int location;         /* parse location, or -1 if none/unknown */
} ColumnDef;

typedef struct CreateStmt {
	NodeTag type;
	RangeVar *relation;      /* relation to create */
	List *tableElts;         /* column definitions (list of ColumnDef) */
	List *inhRelations;      /* relations to inherit from (list of
	                          * inhRelation) */
	TypeName *ofTypename;    /* OF typename */
	List *constraints;       /* constraints (list of Constraint nodes) */
	List *options;           /* options from WITH clause */
	OnCommitAction oncommit; /* what do we do at COMMIT? */
	char *tablespacename;    /* table space to use, or NULL */
	bool if_not_exists;      /* just do nothing if it already exists? */
} CreateStmt;

typedef enum ConstrType /* types of constraints */
{ CONSTR_NULL,          /* not standard SQL, but a lot of people
	                     * expect it */
  CONSTR_NOTNULL,
  CONSTR_DEFAULT,
  CONSTR_CHECK,
  CONSTR_PRIMARY,
  CONSTR_UNIQUE,
  CONSTR_EXCLUSION,
  CONSTR_FOREIGN,
  CONSTR_ATTR_DEFERRABLE, /* attributes for previous constraint node */
  CONSTR_ATTR_NOT_DEFERRABLE,
  CONSTR_ATTR_DEFERRED,
  CONSTR_ATTR_IMMEDIATE } ConstrType;

/* Foreign key action codes */
#define FKCONSTR_ACTION_NOACTION 'a'
#define FKCONSTR_ACTION_RESTRICT 'r'
#define FKCONSTR_ACTION_CASCADE 'c'
#define FKCONSTR_ACTION_SETNULL 'n'
#define FKCONSTR_ACTION_SETDEFAULT 'd'

/* Foreign key matchtype codes */
#define FKCONSTR_MATCH_FULL 'f'
#define FKCONSTR_MATCH_PARTIAL 'p'
#define FKCONSTR_MATCH_SIMPLE 's'

typedef struct Constraint {
	NodeTag type;
	ConstrType contype; /* see above */

	/* Fields used for most/all constraint types: */
	char *conname;     /* Constraint name, or NULL if unnamed */
	bool deferrable;   /* DEFERRABLE? */
	bool initdeferred; /* INITIALLY DEFERRED? */
	int location;      /* token location, or -1 if unknown */

	/* Fields used for constraints with expressions (CHECK and DEFAULT): */
	bool is_no_inherit; /* is constraint non-inheritable? */
	Node *raw_expr;     /* expr, as untransformed parse tree */
	char *cooked_expr;  /* expr, as nodeToString representation */

	/* Fields used for unique constraints (UNIQUE and PRIMARY KEY): */
	List *keys; /* String nodes naming referenced column(s) */

	/* Fields used for EXCLUSION constraints: */
	List *exclusions; /* list of (IndexElem, operator name) pairs */

	/* Fields used for index constraints (UNIQUE, PRIMARY KEY, EXCLUSION): */
	List *options;    /* options from WITH clause */
	char *indexname;  /* existing index to use; otherwise NULL */
	char *indexspace; /* index tablespace; NULL for default */
	/* These could be, but currently are not, used for UNIQUE/PKEY: */
	char *access_method; /* index access method; NULL for default */
	Node *where_clause;  /* partial index predicate */

	/* Fields used for FOREIGN KEY constraints: */
	RangeVar *pktable;   /* Primary key table */
	List *fk_attrs;      /* Attributes of foreign key */
	List *pk_attrs;      /* Corresponding attrs in PK table */
	char fk_matchtype;   /* FULL, PARTIAL, SIMPLE */
	char fk_upd_action;  /* ON UPDATE action */
	char fk_del_action;  /* ON DELETE action */
	List *old_conpfeqop; /* pg_constraint.conpfeqop of my former self */
	Oid old_pktable_oid; /* pg_constraint.confrelid of my former self */

	/* Fields used for constraints that allow a NOT VALID specification */
	bool skip_validation; /* skip validation of existing rows? */
	bool initially_valid; /* mark the new constraint as valid? */
} Constraint;

typedef struct DeleteStmt {
	NodeTag type;
	RangeVar *relation;     /* relation to delete from */
	List *usingClause;      /* optional using clause for more tables */
	Node *whereClause;      /* qualifications */
	List *returningList;    /* list of expressions to return */
	WithClause *withClause; /* WITH clause */
} DeleteStmt;

typedef struct ResTarget {
	NodeTag type;
	char *name;        /* column name or NULL */
	List *indirection; /* subscripts, field names, and '*', or NIL */
	Node *val;         /* the value expression to compute or assign */
	int location;      /* token location, or -1 if unknown */
} ResTarget;

typedef struct ColumnRef {
	NodeTag type;
	List *fields; /* field names (Value strings) or A_Star */
	int location; /* token location, or -1 if unknown */
} ColumnRef;

typedef struct A_Const {
	NodeTag type;
	value val;    /* value (includes type info, see value.h) */
	int location; /* token location, or -1 if unknown */
} A_Const;

typedef struct TypeCast {
	NodeTag type;
	Node *arg;          /* the expression being casted */
	TypeName *typeName; /* the target type */
	int location;       /* token location, or -1 if unknown */
} TypeCast;

typedef struct WindowDef {
	NodeTag type;
	char *name;            /* window's own name */
	char *refname;         /* referenced window name, if any */
	List *partitionClause; /* PARTITION BY expression list */
	List *orderClause;     /* ORDER BY (list of SortBy) */
	int frameOptions;      /* frame_clause options, see below */
	Node *startOffset;     /* expression for starting bound, if any */
	Node *endOffset;       /* expression for ending bound, if any */
	int location;          /* parse location, or -1 if none/unknown */
} WindowDef;

typedef struct FuncCall {
	NodeTag type;
	List *funcname;         /* qualified name of function */
	List *args;             /* the arguments (list of exprs) */
	List *agg_order;        /* ORDER BY (list of SortBy) */
	Node *agg_filter;       /* FILTER clause, if any */
	bool agg_within_group;  /* ORDER BY appeared in WITHIN GROUP */
	bool agg_star;          /* argument was really '*' */
	bool agg_distinct;      /* arguments were labeled DISTINCT */
	bool func_variadic;     /* last argument was labeled VARIADIC */
	struct WindowDef *over; /* OVER clause, if any */
	int location;           /* token location, or -1 if unknown */
} FuncCall;

typedef struct UpdateStmt {
	NodeTag type;
	RangeVar *relation;     /* relation to update */
	List *targetList;       /* the target list (of ResTarget) */
	Node *whereClause;      /* qualifications */
	List *fromClause;       /* optional from clause for more tables */
	List *returningList;    /* list of expressions to return */
	WithClause *withClause; /* WITH clause */
} UpdateStmt;

typedef enum TransactionStmtKind {
	TRANS_STMT_BEGIN,
	TRANS_STMT_START, /* semantically identical to BEGIN */
	TRANS_STMT_COMMIT,
	TRANS_STMT_ROLLBACK,
	TRANS_STMT_SAVEPOINT,
	TRANS_STMT_RELEASE,
	TRANS_STMT_ROLLBACK_TO,
	TRANS_STMT_PREPARE,
	TRANS_STMT_COMMIT_PREPARED,
	TRANS_STMT_ROLLBACK_PREPARED
} TransactionStmtKind;

typedef struct TransactionStmt {
	NodeTag type;
	TransactionStmtKind kind; /* see above */
	List *options;            /* for BEGIN/START and savepoint commands */
	char *gid;                /* for two-phase-commit related commands */
} TransactionStmt;

typedef enum ObjectType {
	OBJECT_AGGREGATE,
	OBJECT_AMOP,
	OBJECT_AMPROC,
	OBJECT_ATTRIBUTE, /* type's attribute, when distinct from column */
	OBJECT_CAST,
	OBJECT_COLUMN,
	OBJECT_COLLATION,
	OBJECT_CONVERSION,
	OBJECT_DATABASE,
	OBJECT_DEFAULT,
	OBJECT_DEFACL,
	OBJECT_DOMAIN,
	OBJECT_DOMCONSTRAINT,
	OBJECT_EVENT_TRIGGER,
	OBJECT_EXTENSION,
	OBJECT_FDW,
	OBJECT_FOREIGN_SERVER,
	OBJECT_FOREIGN_TABLE,
	OBJECT_FUNCTION,
	OBJECT_INDEX,
	OBJECT_LANGUAGE,
	OBJECT_LARGEOBJECT,
	OBJECT_MATVIEW,
	OBJECT_OPCLASS,
	OBJECT_OPERATOR,
	OBJECT_OPFAMILY,
	OBJECT_POLICY,
	OBJECT_ROLE,
	OBJECT_RULE,
	OBJECT_SCHEMA,
	OBJECT_SEQUENCE,
	OBJECT_TABCONSTRAINT,
	OBJECT_TABLE,
	OBJECT_TABLESPACE,
	OBJECT_TRANSFORM,
	OBJECT_TRIGGER,
	OBJECT_TSCONFIGURATION,
	OBJECT_TSDICTIONARY,
	OBJECT_TSPARSER,
	OBJECT_TSTEMPLATE,
	OBJECT_TYPE,
	OBJECT_USER_MAPPING,
	OBJECT_VIEW
} ObjectType;

typedef enum DropBehavior {
	DROP_RESTRICT, /* drop fails if any dependent objects */
	DROP_CASCADE   /* remove dependent objects too */
} DropBehavior;

typedef struct DropStmt {
	NodeTag type;
	List *objects;         /* list of sublists of names (as Values) */
	List *arguments;       /* list of sublists of arguments (as Values) */
	ObjectType removeType; /* object type */
	DropBehavior behavior; /* RESTRICT or CASCADE behavior */
	bool missing_ok;       /* skip error if object is missing? */
	bool concurrent;       /* drop index concurrently? */
} DropStmt;

typedef struct DropDatabaseStmt {
	NodeTag type;
	char *dbname;    /* name of database to drop */
	bool missing_ok; /* skip error if object is missing? */
} DropDatabaseStmt;

typedef struct TruncateStmt {
	NodeTag type;
	List *relations;       /* relations (RangeVars) to be truncated */
	bool restart_seqs;     /* restart owned sequences? */
	DropBehavior behavior; /* RESTRICT or CASCADE behavior */
} TruncateStmt;

typedef struct ExecuteStmt {
	NodeTag type;
	char *name;   /* The name of the plan to execute */
	List *params; /* Values to assign to parameters */
} ExecuteStmt;

typedef struct PrepareStmt {
	NodeTag type;
	char *name;     /* Name of plan, arbitrary */
	List *argtypes; /* Types of parameters (List of TypeName) */
	Node *query;    /* The query itself (as a raw parsetree) */
} PrepareStmt;

typedef enum DefElemAction {
	DEFELEM_UNSPEC, /* no action given */
	DEFELEM_SET,
	DEFELEM_ADD,
	DEFELEM_DROP
} DefElemAction;

typedef struct DefElem {
	NodeTag type;
	char *defnamespace; /* NULL if unqualified name */
	char *defname;
	Node *arg;               /* a (Value *) or a (TypeName *) */
	DefElemAction defaction; /* unspecified action, or SET/ADD/DROP */
	int location;            /* parse location, or -1 if none/unknown */
} DefElem;

typedef struct CopyStmt {
	NodeTag type;
	RangeVar *relation; /* the relation to copy */
	Node *query;        /* the SELECT query to copy */
	List *attlist;      /* List of column names (as Strings), or NIL
	                     * for all columns */
	bool is_from;       /* TO or FROM */
	bool is_program;    /* is 'filename' a program to popen? */
	char *filename;     /* filename, or NULL for STDIN/STDOUT */
	List *options;      /* List of DefElem nodes */
} CopyStmt;

typedef struct CreateDatabaseStmt {
	NodeTag type;
	char *dbname;  /* name of database to create */
	List *options; /* List of DefElem nodes */
} CreateDatabaseStmt;

typedef struct CreateSchemaStmt {
	NodeTag type;
	char *schemaname;   /* the name of the schema to create */
	Node *authrole;     /* the owner of the created schema */
	List *schemaElts;   /* schema components (list of parsenodes) */
	bool if_not_exists; /* just do nothing if schema already exists? */
} CreateSchemaStmt;

/* ----------------------
 *	Alter Table
 * ----------------------
 */
typedef struct AlterTableStmt {
	NodeTag type;
	RangeVar *relation; /* table to work on */
	List *cmds;         /* list of subcommands */
	ObjectType relkind; /* type of object */
	bool missing_ok;    /* skip error if table missing */
} AlterTableStmt;

typedef enum AlterTableType {
	AT_AddColumn,                 /* add column */
	AT_AddColumnRecurse,          /* internal to commands/tablecmds.c */
	AT_AddColumnToView,           /* implicitly via CREATE OR REPLACE VIEW */
	AT_ColumnDefault,             /* alter column default */
	AT_DropNotNull,               /* alter column drop not null */
	AT_SetNotNull,                /* alter column set not null */
	AT_SetStatistics,             /* alter column set statistics */
	AT_SetOptions,                /* alter column set ( options ) */
	AT_ResetOptions,              /* alter column reset ( options ) */
	AT_SetStorage,                /* alter column set storage */
	AT_DropColumn,                /* drop column */
	AT_DropColumnRecurse,         /* internal to commands/tablecmds.c */
	AT_AddIndex,                  /* add index */
	AT_ReAddIndex,                /* internal to commands/tablecmds.c */
	AT_AddConstraint,             /* add constraint */
	AT_AddConstraintRecurse,      /* internal to commands/tablecmds.c */
	AT_ReAddConstraint,           /* internal to commands/tablecmds.c */
	AT_AlterConstraint,           /* alter constraint */
	AT_ValidateConstraint,        /* validate constraint */
	AT_ValidateConstraintRecurse, /* internal to commands/tablecmds.c */
	AT_ProcessedConstraint,       /* pre-processed add constraint (local in
	                               * parser/parse_utilcmd.c) */
	AT_AddIndexConstraint,        /* add constraint using existing index */
	AT_DropConstraint,            /* drop constraint */
	AT_DropConstraintRecurse,     /* internal to commands/tablecmds.c */
	AT_ReAddComment,              /* internal to commands/tablecmds.c */
	AT_AlterColumnType,           /* alter column type */
	AT_AlterColumnGenericOptions, /* alter column OPTIONS (...) */
	AT_ChangeOwner,               /* change owner */
	AT_ClusterOn,                 /* CLUSTER ON */
	AT_DropCluster,               /* SET WITHOUT CLUSTER */
	AT_SetLogged,                 /* SET LOGGED */
	AT_SetUnLogged,               /* SET UNLOGGED */
	AT_AddOids,                   /* SET WITH OIDS */
	AT_AddOidsRecurse,            /* internal to commands/tablecmds.c */
	AT_DropOids,                  /* SET WITHOUT OIDS */
	AT_SetTableSpace,             /* SET TABLESPACE */
	AT_SetRelOptions,             /* SET (...) -- AM specific parameters */
	AT_ResetRelOptions,           /* RESET (...) -- AM specific parameters */
	AT_ReplaceRelOptions,         /* replace reloption list in its entirety */
	AT_EnableTrig,                /* ENABLE TRIGGER name */
	AT_EnableAlwaysTrig,          /* ENABLE ALWAYS TRIGGER name */
	AT_EnableReplicaTrig,         /* ENABLE REPLICA TRIGGER name */
	AT_DisableTrig,               /* DISABLE TRIGGER name */
	AT_EnableTrigAll,             /* ENABLE TRIGGER ALL */
	AT_DisableTrigAll,            /* DISABLE TRIGGER ALL */
	AT_EnableTrigUser,            /* ENABLE TRIGGER USER */
	AT_DisableTrigUser,           /* DISABLE TRIGGER USER */
	AT_EnableRule,                /* ENABLE RULE name */
	AT_EnableAlwaysRule,          /* ENABLE ALWAYS RULE name */
	AT_EnableReplicaRule,         /* ENABLE REPLICA RULE name */
	AT_DisableRule,               /* DISABLE RULE name */
	AT_AddInherit,                /* INHERIT parent */
	AT_DropInherit,               /* NO INHERIT parent */
	AT_AddOf,                     /* OF <type_name> */
	AT_DropOf,                    /* NOT OF */
	AT_ReplicaIdentity,           /* REPLICA IDENTITY */
	AT_EnableRowSecurity,         /* ENABLE ROW SECURITY */
	AT_DisableRowSecurity,        /* DISABLE ROW SECURITY */
	AT_ForceRowSecurity,          /* FORCE ROW SECURITY */
	AT_NoForceRowSecurity,        /* NO FORCE ROW SECURITY */
	AT_GenericOptions             /* OPTIONS (...) */
} AlterTableType;

typedef struct ReplicaIdentityStmt {
	NodeTag type;
	char identity_type;
	char *name;
} ReplicaIdentityStmt;
/* one subcommand of an ALTER TABLE */
typedef struct AlterTableCmd {
	NodeTag type;
	AlterTableType subtype; /* Type of table alteration to apply */
	char *name;             /* column, constraint, or trigger to act on,
	                         * or tablespace */
	Node *newowner;         /* RoleSpec */
	Node *def;              /* definition of new column, index,
	                         * constraint, or parent table */
	DropBehavior behavior;  /* RESTRICT or CASCADE for DROP cases */
	bool missing_ok;        /* skip error if missing? */
} AlterTableCmd;

/* ----------------------
 *	Alter Domain
 *
 * The fields are used in different ways by the different variants of
 * this command.
 * ----------------------
 */
typedef struct AlterDomainStmt {
	NodeTag type;
	char subtype;          /*------------
	                        *	T = alter column default
	                        *	N = alter column drop not null
	                        *	O = alter column set not null
	                        *	C = add constraint
	                        *	X = drop constraint
	                        *------------
	                        */
	List *typeName;        /* domain to work on */
	char *name;            /* column or constraint name to act on */
	Node *def;             /* definition of default or constraint */
	DropBehavior behavior; /* RESTRICT or CASCADE for DROP cases */
	bool missing_ok;       /* skip error if missing? */
} AlterDomainStmt;

typedef enum RoleSpecType {
	ROLESPEC_CSTRING,      /* role name is stored as a C string */
	ROLESPEC_CURRENT_USER, /* role spec is CURRENT_USER */
	ROLESPEC_SESSION_USER, /* role spec is SESSION_USER */
	ROLESPEC_PUBLIC        /* role name is "public" */
} RoleSpecType;

typedef struct RoleSpec {
	NodeTag type;
	RoleSpecType roletype; /* Type of this rolespec */
	char *rolename;        /* filled only for ROLESPEC_CSTRING */
	int location;          /* token location, or -1 if unknown */
} RoleSpec;

typedef enum ViewCheckOption {
	NO_CHECK_OPTION,
	LOCAL_CHECK_OPTION,
	CASCADED_CHECK_OPTION
} ViewCheckOption;

typedef struct ViewStmt {
	NodeTag type;
	RangeVar *view;                  /* the view to be created */
	List *aliases;                   /* target column names */
	Node *query;                     /* the SELECT query */
	bool replace;                    /* replace an existing view? */
	List *options;                   /* options from WITH clause */
	ViewCheckOption withCheckOption; /* WITH CHECK OPTION */
} ViewStmt;

typedef struct ParamRef {
	NodeTag type;
	int number;   /* the number of the parameter */
	int location; /* token location, or -1 if unknown */
} ParamRef;

typedef enum VacuumOption {
	VACOPT_VACUUM = 1 << 0,   /* do VACUUM */
	VACOPT_ANALYZE = 1 << 1,  /* do ANALYZE */
	VACOPT_VERBOSE = 1 << 2,  /* print progress info */
	VACOPT_FREEZE = 1 << 3,   /* FREEZE option */
	VACOPT_FULL = 1 << 4,     /* FULL (non-concurrent) vacuum */
	VACOPT_NOWAIT = 1 << 5,   /* don't wait to get lock (autovacuum only) */
	VACOPT_SKIPTOAST = 1 << 6 /* don't process the TOAST table, if any */
} VacuumOption;

typedef struct VacuumStmt {
	NodeTag type;
	int options;        /* OR of VacuumOption flags */
	RangeVar *relation; /* single table to process, or NULL */
	List *va_cols;      /* list of column names, or NIL for all */
} VacuumStmt;

typedef enum {
	VAR_SET_VALUE,   /* SET var = value */
	VAR_SET_DEFAULT, /* SET var TO DEFAULT */
	VAR_SET_CURRENT, /* SET var FROM CURRENT */
	VAR_SET_MULTI,   /* special case for SET TRANSACTION ... */
	VAR_RESET,       /* RESET var */
	VAR_RESET_ALL    /* RESET ALL */
} VariableSetKind;

typedef struct VariableSetStmt {
	NodeTag type;
	VariableSetKind kind;
	char *name;    /* variable to be set */
	List *args;    /* List of A_Const nodes */
	bool is_local; /* SET LOCAL? */
} VariableSetStmt;

typedef struct VariableShowStmt {
	NodeTag type;
	char *name;
} VariableShowStmt;

/// **********  For UDFs *********** ///

typedef struct CreateFunctionStmt {
	NodeTag type;
	bool replace;         /* T => replace if already exists */
	List *funcname;       /* qualified name of function to create */
	List *parameters;     /* a list of FunctionParameter */
	TypeName *returnType; /* the return type */
	List *options;        /* a list of DefElem */
	List *withClause;     /* a list of DefElem */
} CreateFunctionStmt;

typedef enum FunctionParameterMode {
	/* the assigned enum values appear in pg_proc, don't change 'em! */
	FUNC_PARAM_IN = 'i',       /* input only */
	FUNC_PARAM_OUT = 'o',      /* output only */
	FUNC_PARAM_INOUT = 'b',    /* both */
	FUNC_PARAM_VARIADIC = 'v', /* variadic (always input) */
	FUNC_PARAM_TABLE = 't'     /* table function output column */
} FunctionParameterMode;

typedef struct FunctionParameter {
	NodeTag type;
	char *name;                 /* parameter name, or NULL if not given */
	TypeName *argType;          /* TypeName for parameter type */
	FunctionParameterMode mode; /* IN/OUT/etc */
	Node *defexpr;              /* raw default expr, or NULL if not given */
} FunctionParameter;
