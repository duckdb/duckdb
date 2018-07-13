/*-------------------------------------------------------------------------
 *
 * plannodes.h
 *	  definitions for query plan nodes
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/plannodes.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLANNODES_H
#define PLANNODES_H

#include "access/sdir.h"
#include "lib/stringinfo.h"
#include "nodes/bitmapset.h"
#include "nodes/lockoptions.h"
#include "nodes/primnodes.h"


/* ----------------------------------------------------------------
 *						node definitions
 * ----------------------------------------------------------------
 */

/* ----------------
 *		PlannedStmt node
 *
 * The output of the planner is a Plan tree headed by a PlannedStmt node.
 * PlannedStmt holds the "one time" information needed by the executor.
 * ----------------
 */
typedef struct PlannedStmt
{
	NodeTag		type;

	CmdType		commandType;	/* select|insert|update|delete */

	uint32		queryId;		/* query identifier (copied from Query) */

	bool		hasReturning;	/* is it insert|update|delete RETURNING? */

	bool		hasModifyingCTE;	/* has insert|update|delete in WITH? */

	bool		canSetTag;		/* do I set the command result tag? */

	bool		transientPlan;	/* redo plan when TransactionXmin changes? */

	struct Plan *planTree;		/* tree of Plan nodes */

	List	   *rtable;			/* list of RangeTblEntry nodes */

	/* rtable indexes of target relations for INSERT/UPDATE/DELETE */
	List	   *resultRelations;	/* integer list of RT indexes, or NIL */

	Node	   *utilityStmt;	/* non-null if this is DECLARE CURSOR */

	List	   *subplans;		/* Plan trees for SubPlan expressions */

	Bitmapset  *rewindPlanIDs;	/* indices of subplans that require REWIND */

	List	   *rowMarks;		/* a list of PlanRowMark's */

	List	   *relationOids;	/* OIDs of relations the plan depends on */

	List	   *invalItems;		/* other dependencies, as PlanInvalItems */

	int			nParamExec;		/* number of PARAM_EXEC Params used */

	bool		hasRowSecurity; /* row security applied? */
} PlannedStmt;

/* macro for fetching the Plan associated with a SubPlan node */
#define exec_subplan_get_plan(plannedstmt, subplan) \
	((Plan *) list_nth((plannedstmt)->subplans, (subplan)->plan_id - 1))


/* ----------------
 *		Plan node
 *
 * All plan nodes "derive" from the Plan structure by having the
 * Plan structure as the first field.  This ensures that everything works
 * when nodes are cast to Plan's.  (node pointers are frequently cast to Plan*
 * when passed around generically in the executor)
 *
 * We never actually instantiate any Plan nodes; this is just the common
 * abstract superclass for all Plan-type nodes.
 * ----------------
 */
typedef struct Plan
{
	NodeTag		type;

	/*
	 * estimated execution costs for plan (see costsize.c for more info)
	 */
	Cost		startup_cost;	/* cost expended before fetching any tuples */
	Cost		total_cost;		/* total cost (assuming all tuples fetched) */

	/*
	 * planner's estimate of result size of this plan step
	 */
	double		plan_rows;		/* number of rows plan is expected to emit */
	int			plan_width;		/* average row width in bytes */

	/*
	 * Common structural data for all Plan types.
	 */
	List	   *targetlist;		/* target list to be computed at this node */
	List	   *qual;			/* implicitly-ANDed qual conditions */
	struct Plan *lefttree;		/* input plan tree(s) */
	struct Plan *righttree;
	List	   *initPlan;		/* Init Plan nodes (un-correlated expr
								 * subselects) */

	/*
	 * Information for management of parameter-change-driven rescanning
	 *
	 * extParam includes the paramIDs of all external PARAM_EXEC params
	 * affecting this plan node or its children.  setParam params from the
	 * node's initPlans are not included, but their extParams are.
	 *
	 * allParam includes all the extParam paramIDs, plus the IDs of local
	 * params that affect the node (i.e., the setParams of its initplans).
	 * These are _all_ the PARAM_EXEC params that affect this node.
	 */
	Bitmapset  *extParam;
	Bitmapset  *allParam;
} Plan;

/* ----------------
 *	these are defined to avoid confusion problems with "left"
 *	and "right" and "inner" and "outer".  The convention is that
 *	the "left" plan is the "outer" plan and the "right" plan is
 *	the inner plan, but these make the code more readable.
 * ----------------
 */
#define innerPlan(node)			(((Plan *)(node))->righttree)
#define outerPlan(node)			(((Plan *)(node))->lefttree)


/* ----------------
 *	 Result node -
 *		If no outer plan, evaluate a variable-free targetlist.
 *		If outer plan, return tuples from outer plan (after a level of
 *		projection as shown by targetlist).
 *
 * If resconstantqual isn't NULL, it represents a one-time qualification
 * test (i.e., one that doesn't depend on any variables from the outer plan,
 * so needs to be evaluated only once).
 * ----------------
 */
typedef struct Result
{
	Plan		plan;
	Node	   *resconstantqual;
} Result;

/* ----------------
 *	 ModifyTable node -
 *		Apply rows produced by subplan(s) to result table(s),
 *		by inserting, updating, or deleting.
 *
 * Note that rowMarks and epqParam are presumed to be valid for all the
 * subplan(s); they can't contain any info that varies across subplans.
 * ----------------
 */
typedef struct ModifyTable
{
	Plan		plan;
	CmdType		operation;		/* INSERT, UPDATE, or DELETE */
	bool		canSetTag;		/* do we set the command tag/es_processed? */
	Index		nominalRelation;	/* Parent RT index for use of EXPLAIN */
	List	   *resultRelations;	/* integer list of RT indexes */
	int			resultRelIndex; /* index of first resultRel in plan's list */
	List	   *plans;			/* plan(s) producing source data */
	List	   *withCheckOptionLists;	/* per-target-table WCO lists */
	List	   *returningLists; /* per-target-table RETURNING tlists */
	List	   *fdwPrivLists;	/* per-target-table FDW private data lists */
	List	   *rowMarks;		/* PlanRowMarks (non-locking only) */
	int			epqParam;		/* ID of Param for EvalPlanQual re-eval */
	OnConflictAction onConflictAction;	/* ON CONFLICT action */
	List	   *arbiterIndexes; /* List of ON CONFLICT arbiter index OIDs  */
	List	   *onConflictSet;	/* SET for INSERT ON CONFLICT DO UPDATE */
	Node	   *onConflictWhere;	/* WHERE for ON CONFLICT UPDATE */
	Index		exclRelRTI;		/* RTI of the EXCLUDED pseudo relation */
	List	   *exclRelTlist;	/* tlist of the EXCLUDED pseudo relation */
} ModifyTable;

/* ----------------
 *	 Append node -
 *		Generate the concatenation of the results of sub-plans.
 * ----------------
 */
typedef struct Append
{
	Plan		plan;
	List	   *appendplans;
} Append;

/* ----------------
 *	 MergeAppend node -
 *		Merge the results of pre-sorted sub-plans to preserve the ordering.
 * ----------------
 */
typedef struct MergeAppend
{
	Plan		plan;
	List	   *mergeplans;
	/* remaining fields are just like the sort-key info in struct Sort */
	int			numCols;		/* number of sort-key columns */
	AttrNumber *sortColIdx;		/* their indexes in the target list */
	Oid		   *sortOperators;	/* OIDs of operators to sort them by */
	Oid		   *collations;		/* OIDs of collations */
	bool	   *nullsFirst;		/* NULLS FIRST/LAST directions */
} MergeAppend;

/* ----------------
 *	RecursiveUnion node -
 *		Generate a recursive union of two subplans.
 *
 * The "outer" subplan is always the non-recursive term, and the "inner"
 * subplan is the recursive term.
 * ----------------
 */
typedef struct RecursiveUnion
{
	Plan		plan;
	int			wtParam;		/* ID of Param representing work table */
	/* Remaining fields are zero/null in UNION ALL case */
	int			numCols;		/* number of columns to check for
								 * duplicate-ness */
	AttrNumber *dupColIdx;		/* their indexes in the target list */
	Oid		   *dupOperators;	/* equality operators to compare with */
	long		numGroups;		/* estimated number of groups in input */
} RecursiveUnion;

/* ----------------
 *	 BitmapAnd node -
 *		Generate the intersection of the results of sub-plans.
 *
 * The subplans must be of types that yield tuple bitmaps.  The targetlist
 * and qual fields of the plan are unused and are always NIL.
 * ----------------
 */
typedef struct BitmapAnd
{
	Plan		plan;
	List	   *bitmapplans;
} BitmapAnd;

/* ----------------
 *	 BitmapOr node -
 *		Generate the union of the results of sub-plans.
 *
 * The subplans must be of types that yield tuple bitmaps.  The targetlist
 * and qual fields of the plan are unused and are always NIL.
 * ----------------
 */
typedef struct BitmapOr
{
	Plan		plan;
	List	   *bitmapplans;
} BitmapOr;

/*
 * ==========
 * Scan nodes
 * ==========
 */
typedef struct Scan
{
	Plan		plan;
	Index		scanrelid;		/* relid is index into the range table */
} Scan;

/* ----------------
 *		sequential scan node
 * ----------------
 */
typedef Scan SeqScan;

/* ----------------
 *		table sample scan node
 * ----------------
 */
typedef struct SampleScan
{
	Scan		scan;
	/* use struct pointer to avoid including parsenodes.h here */
	struct TableSampleClause *tablesample;
} SampleScan;

/* ----------------
 *		index scan node
 *
 * indexqualorig is an implicitly-ANDed list of index qual expressions, each
 * in the same form it appeared in the query WHERE condition.  Each should
 * be of the form (indexkey OP comparisonval) or (comparisonval OP indexkey).
 * The indexkey is a Var or expression referencing column(s) of the index's
 * base table.  The comparisonval might be any expression, but it won't use
 * any columns of the base table.  The expressions are ordered by index
 * column position (but items referencing the same index column can appear
 * in any order).  indexqualorig is used at runtime only if we have to recheck
 * a lossy indexqual.
 *
 * indexqual has the same form, but the expressions have been commuted if
 * necessary to put the indexkeys on the left, and the indexkeys are replaced
 * by Var nodes identifying the index columns (their varno is INDEX_VAR and
 * their varattno is the index column number).
 *
 * indexorderbyorig is similarly the original form of any ORDER BY expressions
 * that are being implemented by the index, while indexorderby is modified to
 * have index column Vars on the left-hand side.  Here, multiple expressions
 * must appear in exactly the ORDER BY order, and this is not necessarily the
 * index column order.  Only the expressions are provided, not the auxiliary
 * sort-order information from the ORDER BY SortGroupClauses; it's assumed
 * that the sort ordering is fully determinable from the top-level operators.
 * indexorderbyorig is used at runtime to recheck the ordering, if the index
 * cannot calculate an accurate ordering.  It is also needed for EXPLAIN.
 *
 * indexorderbyops is a list of the OIDs of the operators used to sort the
 * ORDER BY expressions.  This is used together with indexorderbyorig to
 * recheck ordering at run time.  (Note that indexorderby, indexorderbyorig,
 * and indexorderbyops are used for amcanorderbyop cases, not amcanorder.)
 *
 * indexorderdir specifies the scan ordering, for indexscans on amcanorder
 * indexes (for other indexes it should be "don't care").
 * ----------------
 */
typedef struct IndexScan
{
	Scan		scan;
	Oid			indexid;		/* OID of index to scan */
	List	   *indexqual;		/* list of index quals (usually OpExprs) */
	List	   *indexqualorig;	/* the same in original form */
	List	   *indexorderby;	/* list of index ORDER BY exprs */
	List	   *indexorderbyorig;		/* the same in original form */
	List	   *indexorderbyops;	/* OIDs of sort ops for ORDER BY exprs */
	ScanDirection indexorderdir;	/* forward or backward or don't care */
} IndexScan;

/* ----------------
 *		index-only scan node
 *
 * IndexOnlyScan is very similar to IndexScan, but it specifies an
 * index-only scan, in which the data comes from the index not the heap.
 * Because of this, *all* Vars in the plan node's targetlist, qual, and
 * index expressions reference index columns and have varno = INDEX_VAR.
 * Hence we do not need separate indexqualorig and indexorderbyorig lists,
 * since their contents would be equivalent to indexqual and indexorderby.
 *
 * To help EXPLAIN interpret the index Vars for display, we provide
 * indextlist, which represents the contents of the index as a targetlist
 * with one TLE per index column.  Vars appearing in this list reference
 * the base table, and this is the only field in the plan node that may
 * contain such Vars.
 * ----------------
 */
typedef struct IndexOnlyScan
{
	Scan		scan;
	Oid			indexid;		/* OID of index to scan */
	List	   *indexqual;		/* list of index quals (usually OpExprs) */
	List	   *indexorderby;	/* list of index ORDER BY exprs */
	List	   *indextlist;		/* TargetEntry list describing index's cols */
	ScanDirection indexorderdir;	/* forward or backward or don't care */
} IndexOnlyScan;

/* ----------------
 *		bitmap index scan node
 *
 * BitmapIndexScan delivers a bitmap of potential tuple locations;
 * it does not access the heap itself.  The bitmap is used by an
 * ancestor BitmapHeapScan node, possibly after passing through
 * intermediate BitmapAnd and/or BitmapOr nodes to combine it with
 * the results of other BitmapIndexScans.
 *
 * The fields have the same meanings as for IndexScan, except we don't
 * store a direction flag because direction is uninteresting.
 *
 * In a BitmapIndexScan plan node, the targetlist and qual fields are
 * not used and are always NIL.  The indexqualorig field is unused at
 * run time too, but is saved for the benefit of EXPLAIN.
 * ----------------
 */
typedef struct BitmapIndexScan
{
	Scan		scan;
	Oid			indexid;		/* OID of index to scan */
	List	   *indexqual;		/* list of index quals (OpExprs) */
	List	   *indexqualorig;	/* the same in original form */
} BitmapIndexScan;

/* ----------------
 *		bitmap sequential scan node
 *
 * This needs a copy of the qual conditions being used by the input index
 * scans because there are various cases where we need to recheck the quals;
 * for example, when the bitmap is lossy about the specific rows on a page
 * that meet the index condition.
 * ----------------
 */
typedef struct BitmapHeapScan
{
	Scan		scan;
	List	   *bitmapqualorig; /* index quals, in standard expr form */
} BitmapHeapScan;

/* ----------------
 *		tid scan node
 *
 * tidquals is an implicitly OR'ed list of qual expressions of the form
 * "CTID = pseudoconstant" or "CTID = ANY(pseudoconstant_array)".
 * ----------------
 */
typedef struct TidScan
{
	Scan		scan;
	List	   *tidquals;		/* qual(s) involving CTID = something */
} TidScan;

/* ----------------
 *		subquery scan node
 *
 * SubqueryScan is for scanning the output of a sub-query in the range table.
 * We often need an extra plan node above the sub-query's plan to perform
 * expression evaluations (which we can't push into the sub-query without
 * risking changing its semantics).  Although we are not scanning a physical
 * relation, we make this a descendant of Scan anyway for code-sharing
 * purposes.
 *
 * Note: we store the sub-plan in the type-specific subplan field, not in
 * the generic lefttree field as you might expect.  This is because we do
 * not want plan-tree-traversal routines to recurse into the subplan without
 * knowing that they are changing Query contexts.
 * ----------------
 */
typedef struct SubqueryScan
{
	Scan		scan;
	Plan	   *subplan;
} SubqueryScan;

/* ----------------
 *		FunctionScan node
 * ----------------
 */
typedef struct FunctionScan
{
	Scan		scan;
	List	   *functions;		/* list of RangeTblFunction nodes */
	bool		funcordinality; /* WITH ORDINALITY */
} FunctionScan;

/* ----------------
 *		ValuesScan node
 * ----------------
 */
typedef struct ValuesScan
{
	Scan		scan;
	List	   *values_lists;	/* list of expression lists */
} ValuesScan;

/* ----------------
 *		CteScan node
 * ----------------
 */
typedef struct CteScan
{
	Scan		scan;
	int			ctePlanId;		/* ID of init SubPlan for CTE */
	int			cteParam;		/* ID of Param representing CTE output */
} CteScan;

/* ----------------
 *		WorkTableScan node
 * ----------------
 */
typedef struct WorkTableScan
{
	Scan		scan;
	int			wtParam;		/* ID of Param representing work table */
} WorkTableScan;

/* ----------------
 *		ForeignScan node
 *
 * fdw_exprs and fdw_private are both under the control of the foreign-data
 * wrapper, but fdw_exprs is presumed to contain expression trees and will
 * be post-processed accordingly by the planner; fdw_private won't be.
 * Note that everything in both lists must be copiable by copyObject().
 * One way to store an arbitrary blob of bytes is to represent it as a bytea
 * Const.  Usually, though, you'll be better off choosing a representation
 * that can be dumped usefully by nodeToString().
 *
 * fdw_scan_tlist is a targetlist describing the contents of the scan tuple
 * returned by the FDW; it can be NIL if the scan tuple matches the declared
 * rowtype of the foreign table, which is the normal case for a simple foreign
 * table scan.  (If the plan node represents a foreign join, fdw_scan_tlist
 * is required since there is no rowtype available from the system catalogs.)
 * When fdw_scan_tlist is provided, Vars in the node's tlist and quals must
 * have varno INDEX_VAR, and their varattnos correspond to resnos in the
 * fdw_scan_tlist (which are also column numbers in the actual scan tuple).
 * fdw_scan_tlist is never actually executed; it just holds expression trees
 * describing what is in the scan tuple's columns.
 *
 * fdw_recheck_quals should contain any quals which the core system passed to
 * the FDW but which were not added to scan.plan.qual; that is, it should
 * contain the quals being checked remotely.  This is needed for correct
 * behavior during EvalPlanQual rechecks.
 *
 * When the plan node represents a foreign join, scan.scanrelid is zero and
 * fs_relids must be consulted to identify the join relation.  (fs_relids
 * is valid for simple scans as well, but will always match scan.scanrelid.)
 * ----------------
 */
typedef struct ForeignScan
{
	Scan		scan;
	Oid			fs_server;		/* OID of foreign server */
	List	   *fdw_exprs;		/* expressions that FDW may evaluate */
	List	   *fdw_private;	/* private data for FDW */
	List	   *fdw_scan_tlist; /* optional tlist describing scan tuple */
	List	   *fdw_recheck_quals;	/* original quals not in scan.plan.qual */
	Bitmapset  *fs_relids;		/* RTIs generated by this scan */
	bool		fsSystemCol;	/* true if any "system column" is needed */
} ForeignScan;

/* ----------------
 *	   CustomScan node
 *
 * The comments for ForeignScan's fdw_exprs, fdw_private, fdw_scan_tlist,
 * and fs_relids fields apply equally to CustomScan's custom_exprs,
 * custom_private, custom_scan_tlist, and custom_relids fields.  The
 * convention of setting scan.scanrelid to zero for joins applies as well.
 *
 * Note that since Plan trees can be copied, custom scan providers *must*
 * fit all plan data they need into those fields; embedding CustomScan in
 * a larger struct will not work.
 * ----------------
 */
struct CustomScan;

typedef struct CustomScanMethods
{
	const char *CustomName;

	/* Create execution state (CustomScanState) from a CustomScan plan node */
	Node	   *(*CreateCustomScanState) (struct CustomScan *cscan);
	/* Optional: print custom_xxx fields in some special way */
	void		(*TextOutCustomScan) (StringInfo str,
											  const struct CustomScan *node);
} CustomScanMethods;

typedef struct CustomScan
{
	Scan		scan;
	uint32		flags;			/* mask of CUSTOMPATH_* flags, see relation.h */
	List	   *custom_plans;	/* list of Plan nodes, if any */
	List	   *custom_exprs;	/* expressions that custom code may evaluate */
	List	   *custom_private; /* private data for custom code */
	List	   *custom_scan_tlist;		/* optional tlist describing scan
										 * tuple */
	Bitmapset  *custom_relids;	/* RTIs generated by this scan */
	const CustomScanMethods *methods;
} CustomScan;

/*
 * ==========
 * Join nodes
 * ==========
 */

/* ----------------
 *		Join node
 *
 * jointype:	rule for joining tuples from left and right subtrees
 * joinqual:	qual conditions that came from JOIN/ON or JOIN/USING
 *				(plan.qual contains conditions that came from WHERE)
 *
 * When jointype is INNER, joinqual and plan.qual are semantically
 * interchangeable.  For OUTER jointypes, the two are *not* interchangeable;
 * only joinqual is used to determine whether a match has been found for
 * the purpose of deciding whether to generate null-extended tuples.
 * (But plan.qual is still applied before actually returning a tuple.)
 * For an outer join, only joinquals are allowed to be used as the merge
 * or hash condition of a merge or hash join.
 * ----------------
 */
typedef struct Join
{
	Plan		plan;
	JoinType	jointype;
	List	   *joinqual;		/* JOIN quals (in addition to plan.qual) */
} Join;

/* ----------------
 *		nest loop join node
 *
 * The nestParams list identifies any executor Params that must be passed
 * into execution of the inner subplan carrying values from the current row
 * of the outer subplan.  Currently we restrict these values to be simple
 * Vars, but perhaps someday that'd be worth relaxing.  (Note: during plan
 * creation, the paramval can actually be a PlaceHolderVar expression; but it
 * must be a Var with varno OUTER_VAR by the time it gets to the executor.)
 * ----------------
 */
typedef struct NestLoop
{
	Join		join;
	List	   *nestParams;		/* list of NestLoopParam nodes */
} NestLoop;

typedef struct NestLoopParam
{
	NodeTag		type;
	int			paramno;		/* number of the PARAM_EXEC Param to set */
	Var		   *paramval;		/* outer-relation Var to assign to Param */
} NestLoopParam;

/* ----------------
 *		merge join node
 *
 * The expected ordering of each mergeable column is described by a btree
 * opfamily OID, a collation OID, a direction (BTLessStrategyNumber or
 * BTGreaterStrategyNumber) and a nulls-first flag.  Note that the two sides
 * of each mergeclause may be of different datatypes, but they are ordered the
 * same way according to the common opfamily and collation.  The operator in
 * each mergeclause must be an equality operator of the indicated opfamily.
 * ----------------
 */
typedef struct MergeJoin
{
	Join		join;
	List	   *mergeclauses;	/* mergeclauses as expression trees */
	/* these are arrays, but have the same length as the mergeclauses list: */
	Oid		   *mergeFamilies;	/* per-clause OIDs of btree opfamilies */
	Oid		   *mergeCollations;	/* per-clause OIDs of collations */
	int		   *mergeStrategies;	/* per-clause ordering (ASC or DESC) */
	bool	   *mergeNullsFirst;	/* per-clause nulls ordering */
} MergeJoin;

/* ----------------
 *		hash join node
 * ----------------
 */
typedef struct HashJoin
{
	Join		join;
	List	   *hashclauses;
} HashJoin;

/* ----------------
 *		materialization node
 * ----------------
 */
typedef struct Material
{
	Plan		plan;
} Material;

/* ----------------
 *		sort node
 * ----------------
 */
typedef struct Sort
{
	Plan		plan;
	int			numCols;		/* number of sort-key columns */
	AttrNumber *sortColIdx;		/* their indexes in the target list */
	Oid		   *sortOperators;	/* OIDs of operators to sort them by */
	Oid		   *collations;		/* OIDs of collations */
	bool	   *nullsFirst;		/* NULLS FIRST/LAST directions */
} Sort;

/* ---------------
 *	 group node -
 *		Used for queries with GROUP BY (but no aggregates) specified.
 *		The input must be presorted according to the grouping columns.
 * ---------------
 */
typedef struct Group
{
	Plan		plan;
	int			numCols;		/* number of grouping columns */
	AttrNumber *grpColIdx;		/* their indexes in the target list */
	Oid		   *grpOperators;	/* equality operators to compare with */
} Group;

/* ---------------
 *		aggregate node
 *
 * An Agg node implements plain or grouped aggregation.  For grouped
 * aggregation, we can work with presorted input or unsorted input;
 * the latter strategy uses an internal hashtable.
 *
 * Notice the lack of any direct info about the aggregate functions to be
 * computed.  They are found by scanning the node's tlist and quals during
 * executor startup.  (It is possible that there are no aggregate functions;
 * this could happen if they get optimized away by constant-folding, or if
 * we are using the Agg node to implement hash-based grouping.)
 * ---------------
 */
typedef enum AggStrategy
{
	AGG_PLAIN,					/* simple agg across all input rows */
	AGG_SORTED,					/* grouped agg, input must be sorted */
	AGG_HASHED					/* grouped agg, use internal hashtable */
} AggStrategy;

typedef struct Agg
{
	Plan		plan;
	AggStrategy aggstrategy;
	int			numCols;		/* number of grouping columns */
	AttrNumber *grpColIdx;		/* their indexes in the target list */
	Oid		   *grpOperators;	/* equality operators to compare with */
	long		numGroups;		/* estimated number of groups in input */
	List	   *groupingSets;	/* grouping sets to use */
	List	   *chain;			/* chained Agg/Sort nodes */
} Agg;

/* ----------------
 *		window aggregate node
 * ----------------
 */
typedef struct WindowAgg
{
	Plan		plan;
	Index		winref;			/* ID referenced by window functions */
	int			partNumCols;	/* number of columns in partition clause */
	AttrNumber *partColIdx;		/* their indexes in the target list */
	Oid		   *partOperators;	/* equality operators for partition columns */
	int			ordNumCols;		/* number of columns in ordering clause */
	AttrNumber *ordColIdx;		/* their indexes in the target list */
	Oid		   *ordOperators;	/* equality operators for ordering columns */
	int			frameOptions;	/* frame_clause options, see WindowDef */
	Node	   *startOffset;	/* expression for starting bound, if any */
	Node	   *endOffset;		/* expression for ending bound, if any */
} WindowAgg;

/* ----------------
 *		unique node
 * ----------------
 */
typedef struct Unique
{
	Plan		plan;
	int			numCols;		/* number of columns to check for uniqueness */
	AttrNumber *uniqColIdx;		/* their indexes in the target list */
	Oid		   *uniqOperators;	/* equality operators to compare with */
} Unique;

/* ----------------
 *		hash build node
 *
 * If the executor is supposed to try to apply skew join optimization, then
 * skewTable/skewColumn/skewInherit identify the outer relation's join key
 * column, from which the relevant MCV statistics can be fetched.  Also, its
 * type information is provided to save a lookup.
 * ----------------
 */
typedef struct Hash
{
	Plan		plan;
	Oid			skewTable;		/* outer join key's table OID, or InvalidOid */
	AttrNumber	skewColumn;		/* outer join key's column #, or zero */
	bool		skewInherit;	/* is outer join rel an inheritance tree? */
	Oid			skewColType;	/* datatype of the outer key column */
	int32		skewColTypmod;	/* typmod of the outer key column */
	/* all other info is in the parent HashJoin node */
} Hash;

/* ----------------
 *		setop node
 * ----------------
 */
typedef enum SetOpCmd
{
	SETOPCMD_INTERSECT,
	SETOPCMD_INTERSECT_ALL,
	SETOPCMD_EXCEPT,
	SETOPCMD_EXCEPT_ALL
} SetOpCmd;

typedef enum SetOpStrategy
{
	SETOP_SORTED,				/* input must be sorted */
	SETOP_HASHED				/* use internal hashtable */
} SetOpStrategy;

typedef struct SetOp
{
	Plan		plan;
	SetOpCmd	cmd;			/* what to do */
	SetOpStrategy strategy;		/* how to do it */
	int			numCols;		/* number of columns to check for
								 * duplicate-ness */
	AttrNumber *dupColIdx;		/* their indexes in the target list */
	Oid		   *dupOperators;	/* equality operators to compare with */
	AttrNumber	flagColIdx;		/* where is the flag column, if any */
	int			firstFlag;		/* flag value for first input relation */
	long		numGroups;		/* estimated number of groups in input */
} SetOp;

/* ----------------
 *		lock-rows node
 *
 * rowMarks identifies the rels to be locked by this node; it should be
 * a subset of the rowMarks listed in the top-level PlannedStmt.
 * epqParam is a Param that all scan nodes below this one must depend on.
 * It is used to force re-evaluation of the plan during EvalPlanQual.
 * ----------------
 */
typedef struct LockRows
{
	Plan		plan;
	List	   *rowMarks;		/* a list of PlanRowMark's */
	int			epqParam;		/* ID of Param for EvalPlanQual re-eval */
} LockRows;

/* ----------------
 *		limit node
 *
 * Note: as of Postgres 8.2, the offset and count expressions are expected
 * to yield int8, rather than int4 as before.
 * ----------------
 */
typedef struct Limit
{
	Plan		plan;
	Node	   *limitOffset;	/* OFFSET parameter, or NULL if none */
	Node	   *limitCount;		/* COUNT parameter, or NULL if none */
} Limit;


/*
 * RowMarkType -
 *	  enums for types of row-marking operations
 *
 * The first four of these values represent different lock strengths that
 * we can take on tuples according to SELECT FOR [KEY] UPDATE/SHARE requests.
 * We support these on regular tables, as well as on foreign tables whose FDWs
 * report support for late locking.  For other foreign tables, any locking
 * that might be done for such requests must happen during the initial row
 * fetch; their FDWs provide no mechanism for going back to lock a row later.
 * This means that the semantics will be a bit different than for a local
 * table; in particular we are likely to lock more rows than would be locked
 * locally, since remote rows will be locked even if they then fail
 * locally-checked restriction or join quals.  However, the prospect of
 * doing a separate remote query to lock each selected row is usually pretty
 * unappealing, so early locking remains a credible design choice for FDWs.
 *
 * When doing UPDATE, DELETE, or SELECT FOR UPDATE/SHARE, we have to uniquely
 * identify all the source rows, not only those from the target relations, so
 * that we can perform EvalPlanQual rechecking at need.  For plain tables we
 * can just fetch the TID, much as for a target relation; this case is
 * represented by ROW_MARK_REFERENCE.  Otherwise (for example for VALUES or
 * FUNCTION scans) we have to copy the whole row value.  ROW_MARK_COPY is
 * pretty inefficient, since most of the time we'll never need the data; but
 * fortunately the overhead is usually not performance-critical in practice.
 * By default we use ROW_MARK_COPY for foreign tables, but if the FDW has
 * a concept of rowid it can request to use ROW_MARK_REFERENCE instead.
 * (Again, this probably doesn't make sense if a physical remote fetch is
 * needed, but for FDWs that map to local storage it might be credible.)
 */
typedef enum RowMarkType
{
	ROW_MARK_EXCLUSIVE,			/* obtain exclusive tuple lock */
	ROW_MARK_NOKEYEXCLUSIVE,	/* obtain no-key exclusive tuple lock */
	ROW_MARK_SHARE,				/* obtain shared tuple lock */
	ROW_MARK_KEYSHARE,			/* obtain keyshare tuple lock */
	ROW_MARK_REFERENCE,			/* just fetch the TID, don't lock it */
	ROW_MARK_COPY				/* physically copy the row value */
} RowMarkType;

#define RowMarkRequiresRowShareLock(marktype)  ((marktype) <= ROW_MARK_KEYSHARE)

/*
 * PlanRowMark -
 *	   plan-time representation of FOR [KEY] UPDATE/SHARE clauses
 *
 * When doing UPDATE, DELETE, or SELECT FOR UPDATE/SHARE, we create a separate
 * PlanRowMark node for each non-target relation in the query.  Relations that
 * are not specified as FOR UPDATE/SHARE are marked ROW_MARK_REFERENCE (if
 * regular tables or supported foreign tables) or ROW_MARK_COPY (if not).
 *
 * Initially all PlanRowMarks have rti == prti and isParent == false.
 * When the planner discovers that a relation is the root of an inheritance
 * tree, it sets isParent true, and adds an additional PlanRowMark to the
 * list for each child relation (including the target rel itself in its role
 * as a child).  The child entries have rti == child rel's RT index and
 * prti == parent's RT index, and can therefore be recognized as children by
 * the fact that prti != rti.  The parent's allMarkTypes field gets the OR
 * of (1<<markType) across all its children (this definition allows children
 * to use different markTypes).
 *
 * The planner also adds resjunk output columns to the plan that carry
 * information sufficient to identify the locked or fetched rows.  When
 * markType != ROW_MARK_COPY, these columns are named
 *		tableoid%u			OID of table
 *		ctid%u				TID of row
 * The tableoid column is only present for an inheritance hierarchy.
 * When markType == ROW_MARK_COPY, there is instead a single column named
 *		wholerow%u			whole-row value of relation
 * (An inheritance hierarchy could have all three resjunk output columns,
 * if some children use a different markType than others.)
 * In all three cases, %u represents the rowmark ID number (rowmarkId).
 * This number is unique within a plan tree, except that child relation
 * entries copy their parent's rowmarkId.  (Assigning unique numbers
 * means we needn't renumber rowmarkIds when flattening subqueries, which
 * would require finding and renaming the resjunk columns as well.)
 * Note this means that all tables in an inheritance hierarchy share the
 * same resjunk column names.  However, in an inherited UPDATE/DELETE the
 * columns could have different physical column numbers in each subplan.
 */
typedef struct PlanRowMark
{
	NodeTag		type;
	Index		rti;			/* range table index of markable relation */
	Index		prti;			/* range table index of parent relation */
	Index		rowmarkId;		/* unique identifier for resjunk columns */
	RowMarkType markType;		/* see enum above */
	int			allMarkTypes;	/* OR of (1<<markType) for all children */
	LockClauseStrength strength;	/* LockingClause's strength, or LCS_NONE */
	LockWaitPolicy waitPolicy;	/* NOWAIT and SKIP LOCKED options */
	bool		isParent;		/* true if this is a "dummy" parent entry */
} PlanRowMark;


/*
 * Plan invalidation info
 *
 * We track the objects on which a PlannedStmt depends in two ways:
 * relations are recorded as a simple list of OIDs, and everything else
 * is represented as a list of PlanInvalItems.  A PlanInvalItem is designed
 * to be used with the syscache invalidation mechanism, so it identifies a
 * system catalog entry by cache ID and hash value.
 */
typedef struct PlanInvalItem
{
	NodeTag		type;
	int			cacheId;		/* a syscache ID, see utils/syscache.h */
	uint32		hashValue;		/* hash value of object's cache lookup key */
} PlanInvalItem;

#endif   /* PLANNODES_H */
