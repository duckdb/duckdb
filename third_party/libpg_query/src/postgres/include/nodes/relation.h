/*-------------------------------------------------------------------------
 *
 * relation.h
 *	  Definitions for planner's internal data structures.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/relation.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELATION_H
#define RELATION_H

#include "access/sdir.h"
#include "lib/stringinfo.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "storage/block.h"


/*
 * Relids
 *		Set of relation identifiers (indexes into the rangetable).
 */
typedef Bitmapset *Relids;

/*
 * When looking for a "cheapest path", this enum specifies whether we want
 * cheapest startup cost or cheapest total cost.
 */
typedef enum CostSelector
{
	STARTUP_COST, TOTAL_COST
} CostSelector;

/*
 * The cost estimate produced by cost_qual_eval() includes both a one-time
 * (startup) cost, and a per-tuple cost.
 */
typedef struct QualCost
{
	Cost		startup;		/* one-time cost */
	Cost		per_tuple;		/* per-evaluation cost */
} QualCost;

/*
 * Costing aggregate function execution requires these statistics about
 * the aggregates to be executed by a given Agg node.  Note that the costs
 * include the execution costs of the aggregates' argument expressions as
 * well as the aggregate functions themselves.
 */
typedef struct AggClauseCosts
{
	int			numAggs;		/* total number of aggregate functions */
	int			numOrderedAggs; /* number w/ DISTINCT/ORDER BY/WITHIN GROUP */
	QualCost	transCost;		/* total per-input-row execution costs */
	Cost		finalCost;		/* total per-aggregated-row costs */
	Size		transitionSpace;	/* space for pass-by-ref transition data */
} AggClauseCosts;


/*----------
 * PlannerGlobal
 *		Global information for planning/optimization
 *
 * PlannerGlobal holds state for an entire planner invocation; this state
 * is shared across all levels of sub-Queries that exist in the command being
 * planned.
 *----------
 */
typedef struct PlannerGlobal
{
	NodeTag		type;

	ParamListInfo boundParams;	/* Param values provided to planner() */

	List	   *subplans;		/* Plans for SubPlan nodes */

	List	   *subroots;		/* PlannerInfos for SubPlan nodes */

	Bitmapset  *rewindPlanIDs;	/* indices of subplans that require REWIND */

	List	   *finalrtable;	/* "flat" rangetable for executor */

	List	   *finalrowmarks;	/* "flat" list of PlanRowMarks */

	List	   *resultRelations;	/* "flat" list of integer RT indexes */

	List	   *relationOids;	/* OIDs of relations the plan depends on */

	List	   *invalItems;		/* other dependencies, as PlanInvalItems */

	int			nParamExec;		/* number of PARAM_EXEC Params used */

	Index		lastPHId;		/* highest PlaceHolderVar ID assigned */

	Index		lastRowMarkId;	/* highest PlanRowMark ID assigned */

	bool		transientPlan;	/* redo plan when TransactionXmin changes? */

	bool		hasRowSecurity; /* row security applied? */
} PlannerGlobal;

/* macro for fetching the Plan associated with a SubPlan node */
#define planner_subplan_get_plan(root, subplan) \
	((Plan *) list_nth((root)->glob->subplans, (subplan)->plan_id - 1))


/*----------
 * PlannerInfo
 *		Per-query information for planning/optimization
 *
 * This struct is conventionally called "root" in all the planner routines.
 * It holds links to all of the planner's working state, in addition to the
 * original Query.  Note that at present the planner extensively modifies
 * the passed-in Query data structure; someday that should stop.
 *----------
 */
typedef struct PlannerInfo
{
	NodeTag		type;

	Query	   *parse;			/* the Query being planned */

	PlannerGlobal *glob;		/* global info for current planner run */

	Index		query_level;	/* 1 at the outermost Query */

	struct PlannerInfo *parent_root;	/* NULL at outermost Query */

	List	   *plan_params;	/* list of PlannerParamItems, see below */

	/*
	 * simple_rel_array holds pointers to "base rels" and "other rels" (see
	 * comments for RelOptInfo for more info).  It is indexed by rangetable
	 * index (so entry 0 is always wasted).  Entries can be NULL when an RTE
	 * does not correspond to a base relation, such as a join RTE or an
	 * unreferenced view RTE; or if the RelOptInfo hasn't been made yet.
	 */
	struct RelOptInfo **simple_rel_array;		/* All 1-rel RelOptInfos */
	int			simple_rel_array_size;	/* allocated size of array */

	/*
	 * simple_rte_array is the same length as simple_rel_array and holds
	 * pointers to the associated rangetable entries.  This lets us avoid
	 * rt_fetch(), which can be a bit slow once large inheritance sets have
	 * been expanded.
	 */
	RangeTblEntry **simple_rte_array;	/* rangetable as an array */

	/*
	 * all_baserels is a Relids set of all base relids (but not "other"
	 * relids) in the query; that is, the Relids identifier of the final join
	 * we need to form.  This is computed in make_one_rel, just before we
	 * start making Paths.
	 */
	Relids		all_baserels;

	/*
	 * nullable_baserels is a Relids set of base relids that are nullable by
	 * some outer join in the jointree; these are rels that are potentially
	 * nullable below the WHERE clause, SELECT targetlist, etc.  This is
	 * computed in deconstruct_jointree.
	 */
	Relids		nullable_baserels;

	/*
	 * join_rel_list is a list of all join-relation RelOptInfos we have
	 * considered in this planning run.  For small problems we just scan the
	 * list to do lookups, but when there are many join relations we build a
	 * hash table for faster lookups.  The hash table is present and valid
	 * when join_rel_hash is not NULL.  Note that we still maintain the list
	 * even when using the hash table for lookups; this simplifies life for
	 * GEQO.
	 */
	List	   *join_rel_list;	/* list of join-relation RelOptInfos */
	struct HTAB *join_rel_hash; /* optional hashtable for join relations */

	/*
	 * When doing a dynamic-programming-style join search, join_rel_level[k]
	 * is a list of all join-relation RelOptInfos of level k, and
	 * join_cur_level is the current level.  New join-relation RelOptInfos are
	 * automatically added to the join_rel_level[join_cur_level] list.
	 * join_rel_level is NULL if not in use.
	 */
	List	  **join_rel_level; /* lists of join-relation RelOptInfos */
	int			join_cur_level; /* index of list being extended */

	List	   *init_plans;		/* init SubPlans for query */

	List	   *cte_plan_ids;	/* per-CTE-item list of subplan IDs */

	List	   *multiexpr_params;		/* List of Lists of Params for
										 * MULTIEXPR subquery outputs */

	List	   *eq_classes;		/* list of active EquivalenceClasses */

	List	   *canon_pathkeys; /* list of "canonical" PathKeys */

	List	   *left_join_clauses;		/* list of RestrictInfos for
										 * mergejoinable outer join clauses
										 * w/nonnullable var on left */

	List	   *right_join_clauses;		/* list of RestrictInfos for
										 * mergejoinable outer join clauses
										 * w/nonnullable var on right */

	List	   *full_join_clauses;		/* list of RestrictInfos for
										 * mergejoinable full join clauses */

	List	   *join_info_list; /* list of SpecialJoinInfos */

	List	   *append_rel_list;	/* list of AppendRelInfos */

	List	   *rowMarks;		/* list of PlanRowMarks */

	List	   *placeholder_list;		/* list of PlaceHolderInfos */

	List	   *query_pathkeys; /* desired pathkeys for query_planner(), and
								 * actual pathkeys after planning */

	List	   *group_pathkeys; /* groupClause pathkeys, if any */
	List	   *window_pathkeys;	/* pathkeys of bottom window, if any */
	List	   *distinct_pathkeys;		/* distinctClause pathkeys, if any */
	List	   *sort_pathkeys;	/* sortClause pathkeys, if any */

	List	   *minmax_aggs;	/* List of MinMaxAggInfos */

	List	   *initial_rels;	/* RelOptInfos we are now trying to join */

	MemoryContext planner_cxt;	/* context holding PlannerInfo */

	double		total_table_pages;		/* # of pages in all tables of query */

	double		tuple_fraction; /* tuple_fraction passed to query_planner */
	double		limit_tuples;	/* limit_tuples passed to query_planner */

	bool		hasInheritedTarget;		/* true if parse->resultRelation is an
										 * inheritance child rel */
	bool		hasJoinRTEs;	/* true if any RTEs are RTE_JOIN kind */
	bool		hasLateralRTEs; /* true if any RTEs are marked LATERAL */
	bool		hasDeletedRTEs; /* true if any RTE was deleted from jointree */
	bool		hasHavingQual;	/* true if havingQual was non-null */
	bool		hasPseudoConstantQuals; /* true if any RestrictInfo has
										 * pseudoconstant = true */
	bool		hasRecursion;	/* true if planning a recursive WITH item */

	/* These fields are used only when hasRecursion is true: */
	int			wt_param_id;	/* PARAM_EXEC ID for the work table */
	struct Plan *non_recursive_plan;	/* plan for non-recursive term */

	/* These fields are workspace for createplan.c */
	Relids		curOuterRels;	/* outer rels above current node */
	List	   *curOuterParams; /* not-yet-assigned NestLoopParams */

	/* optional private data for join_search_hook, e.g., GEQO */
	void	   *join_search_private;

	/* for GroupingFunc fixup in setrefs */
	AttrNumber *grouping_map;
} PlannerInfo;


/*
 * In places where it's known that simple_rte_array[] must have been prepared
 * already, we just index into it to fetch RTEs.  In code that might be
 * executed before or after entering query_planner(), use this macro.
 */
#define planner_rt_fetch(rti, root) \
	((root)->simple_rte_array ? (root)->simple_rte_array[rti] : \
	 rt_fetch(rti, (root)->parse->rtable))


/*----------
 * RelOptInfo
 *		Per-relation information for planning/optimization
 *
 * For planning purposes, a "base rel" is either a plain relation (a table)
 * or the output of a sub-SELECT or function that appears in the range table.
 * In either case it is uniquely identified by an RT index.  A "joinrel"
 * is the joining of two or more base rels.  A joinrel is identified by
 * the set of RT indexes for its component baserels.  We create RelOptInfo
 * nodes for each baserel and joinrel, and store them in the PlannerInfo's
 * simple_rel_array and join_rel_list respectively.
 *
 * Note that there is only one joinrel for any given set of component
 * baserels, no matter what order we assemble them in; so an unordered
 * set is the right datatype to identify it with.
 *
 * We also have "other rels", which are like base rels in that they refer to
 * single RT indexes; but they are not part of the join tree, and are given
 * a different RelOptKind to identify them.  Lastly, there is a RelOptKind
 * for "dead" relations, which are base rels that we have proven we don't
 * need to join after all.
 *
 * Currently the only kind of otherrels are those made for member relations
 * of an "append relation", that is an inheritance set or UNION ALL subquery.
 * An append relation has a parent RTE that is a base rel, which represents
 * the entire append relation.  The member RTEs are otherrels.  The parent
 * is present in the query join tree but the members are not.  The member
 * RTEs and otherrels are used to plan the scans of the individual tables or
 * subqueries of the append set; then the parent baserel is given Append
 * and/or MergeAppend paths comprising the best paths for the individual
 * member rels.  (See comments for AppendRelInfo for more information.)
 *
 * At one time we also made otherrels to represent join RTEs, for use in
 * handling join alias Vars.  Currently this is not needed because all join
 * alias Vars are expanded to non-aliased form during preprocess_expression.
 *
 * Parts of this data structure are specific to various scan and join
 * mechanisms.  It didn't seem worth creating new node types for them.
 *
 *		relids - Set of base-relation identifiers; it is a base relation
 *				if there is just one, a join relation if more than one
 *		rows - estimated number of tuples in the relation after restriction
 *			   clauses have been applied (ie, output rows of a plan for it)
 *		width - avg. number of bytes per tuple in the relation after the
 *				appropriate projections have been done (ie, output width)
 *		consider_startup - true if there is any value in keeping plain paths for
 *						   this rel on the basis of having cheap startup cost
 *		consider_param_startup - the same for parameterized paths
 *		reltargetlist - List of Var and PlaceHolderVar nodes for the values
 *						we need to output from this relation.
 *						List is in no particular order, but all rels of an
 *						appendrel set must use corresponding orders.
 *						NOTE: in an appendrel child relation, may contain
 *						arbitrary expressions pulled up from a subquery!
 *		pathlist - List of Path nodes, one for each potentially useful
 *				   method of generating the relation
 *		ppilist - ParamPathInfo nodes for parameterized Paths, if any
 *		cheapest_startup_path - the pathlist member with lowest startup cost
 *			(regardless of ordering) among the unparameterized paths;
 *			or NULL if there is no unparameterized path
 *		cheapest_total_path - the pathlist member with lowest total cost
 *			(regardless of ordering) among the unparameterized paths;
 *			or if there is no unparameterized path, the path with lowest
 *			total cost among the paths with minimum parameterization
 *		cheapest_unique_path - for caching cheapest path to produce unique
 *			(no duplicates) output from relation; NULL if not yet requested
 *		cheapest_parameterized_paths - best paths for their parameterizations;
 *			always includes cheapest_total_path, even if that's unparameterized
 *		direct_lateral_relids - rels this rel has direct LATERAL references to
 *		lateral_relids - required outer rels for LATERAL, as a Relids set
 *			(includes both direct and indirect lateral references)
 *
 * If the relation is a base relation it will have these fields set:
 *
 *		relid - RTE index (this is redundant with the relids field, but
 *				is provided for convenience of access)
 *		rtekind - distinguishes plain relation, subquery, or function RTE
 *		min_attr, max_attr - range of valid AttrNumbers for rel
 *		attr_needed - array of bitmapsets indicating the highest joinrel
 *				in which each attribute is needed; if bit 0 is set then
 *				the attribute is needed as part of final targetlist
 *		attr_widths - cache space for per-attribute width estimates;
 *					  zero means not computed yet
 *		lateral_vars - lateral cross-references of rel, if any (list of
 *					   Vars and PlaceHolderVars)
 *		lateral_referencers - relids of rels that reference this one laterally
 *				(includes both direct and indirect lateral references)
 *		indexlist - list of IndexOptInfo nodes for relation's indexes
 *					(always NIL if it's not a table)
 *		pages - number of disk pages in relation (zero if not a table)
 *		tuples - number of tuples in relation (not considering restrictions)
 *		allvisfrac - fraction of disk pages that are marked all-visible
 *		subplan - plan for subquery (NULL if it's not a subquery)
 *		subroot - PlannerInfo for subquery (NULL if it's not a subquery)
 *		subplan_params - list of PlannerParamItems to be passed to subquery
 *
 *		Note: for a subquery, tuples, subplan, subroot are not set immediately
 *		upon creation of the RelOptInfo object; they are filled in when
 *		set_subquery_pathlist processes the object.
 *
 *		For otherrels that are appendrel members, these fields are filled
 *		in just as for a baserel, except we don't bother with lateral_vars.
 *
 * If the relation is either a foreign table or a join of foreign tables that
 * all belong to the same foreign server, these fields will be set:
 *
 *		serverid - OID of foreign server, if foreign table (else InvalidOid)
 *		fdwroutine - function hooks for FDW, if foreign table (else NULL)
 *		fdw_private - private state for FDW, if foreign table (else NULL)
 *
 * The presence of the remaining fields depends on the restrictions
 * and joins that the relation participates in:
 *
 *		baserestrictinfo - List of RestrictInfo nodes, containing info about
 *					each non-join qualification clause in which this relation
 *					participates (only used for base rels)
 *		baserestrictcost - Estimated cost of evaluating the baserestrictinfo
 *					clauses at a single tuple (only used for base rels)
 *		joininfo  - List of RestrictInfo nodes, containing info about each
 *					join clause in which this relation participates (but
 *					note this excludes clauses that might be derivable from
 *					EquivalenceClasses)
 *		has_eclass_joins - flag that EquivalenceClass joins are possible
 *
 * Note: Keeping a restrictinfo list in the RelOptInfo is useful only for
 * base rels, because for a join rel the set of clauses that are treated as
 * restrict clauses varies depending on which sub-relations we choose to join.
 * (For example, in a 3-base-rel join, a clause relating rels 1 and 2 must be
 * treated as a restrictclause if we join {1} and {2 3} to make {1 2 3}; but
 * if we join {1 2} and {3} then that clause will be a restrictclause in {1 2}
 * and should not be processed again at the level of {1 2 3}.)	Therefore,
 * the restrictinfo list in the join case appears in individual JoinPaths
 * (field joinrestrictinfo), not in the parent relation.  But it's OK for
 * the RelOptInfo to store the joininfo list, because that is the same
 * for a given rel no matter how we form it.
 *
 * We store baserestrictcost in the RelOptInfo (for base relations) because
 * we know we will need it at least once (to price the sequential scan)
 * and may need it multiple times to price index scans.
 *----------
 */
typedef enum RelOptKind
{
	RELOPT_BASEREL,
	RELOPT_JOINREL,
	RELOPT_OTHER_MEMBER_REL,
	RELOPT_DEADREL
} RelOptKind;

typedef struct RelOptInfo
{
	NodeTag		type;

	RelOptKind	reloptkind;

	/* all relations included in this RelOptInfo */
	Relids		relids;			/* set of base relids (rangetable indexes) */

	/* size estimates generated by planner */
	double		rows;			/* estimated number of result tuples */
	int			width;			/* estimated avg width of result tuples */

	/* per-relation planner control flags */
	bool		consider_startup;		/* keep cheap-startup-cost paths? */
	bool		consider_param_startup; /* ditto, for parameterized paths? */

	/* materialization information */
	List	   *reltargetlist;	/* Vars to be output by scan of relation */
	List	   *pathlist;		/* Path structures */
	List	   *ppilist;		/* ParamPathInfos used in pathlist */
	struct Path *cheapest_startup_path;
	struct Path *cheapest_total_path;
	struct Path *cheapest_unique_path;
	List	   *cheapest_parameterized_paths;

	/* parameterization information needed for both base rels and join rels */
	/* (see also lateral_vars and lateral_referencers) */
	Relids		direct_lateral_relids;	/* rels directly laterally referenced */
	Relids		lateral_relids; /* minimum parameterization of rel */

	/* information about a base rel (not set for join rels!) */
	Index		relid;
	Oid			reltablespace;	/* containing tablespace */
	RTEKind		rtekind;		/* RELATION, SUBQUERY, or FUNCTION */
	AttrNumber	min_attr;		/* smallest attrno of rel (often <0) */
	AttrNumber	max_attr;		/* largest attrno of rel */
	Relids	   *attr_needed;	/* array indexed [min_attr .. max_attr] */
	int32	   *attr_widths;	/* array indexed [min_attr .. max_attr] */
	List	   *lateral_vars;	/* LATERAL Vars and PHVs referenced by rel */
	Relids		lateral_referencers;	/* rels that reference me laterally */
	List	   *indexlist;		/* list of IndexOptInfo */
	BlockNumber pages;			/* size estimates derived from pg_class */
	double		tuples;
	double		allvisfrac;
	/* use "struct Plan" to avoid including plannodes.h here */
	struct Plan *subplan;		/* if subquery */
	PlannerInfo *subroot;		/* if subquery */
	List	   *subplan_params; /* if subquery */

	/* Information about foreign tables and foreign joins */
	Oid			serverid;		/* identifies server for the table or join */
	/* use "struct FdwRoutine" to avoid including fdwapi.h here */
	struct FdwRoutine *fdwroutine;
	void	   *fdw_private;

	/* used by various scans and joins: */
	List	   *baserestrictinfo;		/* RestrictInfo structures (if base
										 * rel) */
	QualCost	baserestrictcost;		/* cost of evaluating the above */
	List	   *joininfo;		/* RestrictInfo structures for join clauses
								 * involving this rel */
	bool		has_eclass_joins;		/* T means joininfo is incomplete */
} RelOptInfo;

/*
 * IndexOptInfo
 *		Per-index information for planning/optimization
 *
 *		indexkeys[], indexcollations[], opfamily[], and opcintype[]
 *		each have ncolumns entries.
 *
 *		sortopfamily[], reverse_sort[], and nulls_first[] likewise have
 *		ncolumns entries, if the index is ordered; but if it is unordered,
 *		those pointers are NULL.
 *
 *		Zeroes in the indexkeys[] array indicate index columns that are
 *		expressions; there is one element in indexprs for each such column.
 *
 *		For an ordered index, reverse_sort[] and nulls_first[] describe the
 *		sort ordering of a forward indexscan; we can also consider a backward
 *		indexscan, which will generate the reverse ordering.
 *
 *		The indexprs and indpred expressions have been run through
 *		prepqual.c and eval_const_expressions() for ease of matching to
 *		WHERE clauses. indpred is in implicit-AND form.
 *
 *		indextlist is a TargetEntry list representing the index columns.
 *		It provides an equivalent base-relation Var for each simple column,
 *		and links to the matching indexprs element for each expression column.
 */
typedef struct IndexOptInfo
{
	NodeTag		type;

	Oid			indexoid;		/* OID of the index relation */
	Oid			reltablespace;	/* tablespace of index (not table) */
	RelOptInfo *rel;			/* back-link to index's table */

	/* index-size statistics (from pg_class and elsewhere) */
	BlockNumber pages;			/* number of disk pages in index */
	double		tuples;			/* number of index tuples in index */
	int			tree_height;	/* index tree height, or -1 if unknown */

	/* index descriptor information */
	int			ncolumns;		/* number of columns in index */
	int		   *indexkeys;		/* column numbers of index's keys, or 0 */
	Oid		   *indexcollations;	/* OIDs of collations of index columns */
	Oid		   *opfamily;		/* OIDs of operator families for columns */
	Oid		   *opcintype;		/* OIDs of opclass declared input data types */
	Oid		   *sortopfamily;	/* OIDs of btree opfamilies, if orderable */
	bool	   *reverse_sort;	/* is sort order descending? */
	bool	   *nulls_first;	/* do NULLs come first in the sort order? */
	bool	   *canreturn;		/* which index cols can be returned in an
								 * index-only scan? */
	Oid			relam;			/* OID of the access method (in pg_am) */

	RegProcedure amcostestimate;	/* OID of the access method's cost fcn */

	List	   *indexprs;		/* expressions for non-simple index columns */
	List	   *indpred;		/* predicate if a partial index, else NIL */

	List	   *indextlist;		/* targetlist representing index columns */

	bool		predOK;			/* true if predicate matches query */
	bool		unique;			/* true if a unique index */
	bool		immediate;		/* is uniqueness enforced immediately? */
	bool		hypothetical;	/* true if index doesn't really exist */
	bool		amcanorderbyop; /* does AM support order by operator result? */
	bool		amoptionalkey;	/* can query omit key for the first column? */
	bool		amsearcharray;	/* can AM handle ScalarArrayOpExpr quals? */
	bool		amsearchnulls;	/* can AM search for NULL/NOT NULL entries? */
	bool		amhasgettuple;	/* does AM have amgettuple interface? */
	bool		amhasgetbitmap; /* does AM have amgetbitmap interface? */
} IndexOptInfo;


/*
 * EquivalenceClasses
 *
 * Whenever we can determine that a mergejoinable equality clause A = B is
 * not delayed by any outer join, we create an EquivalenceClass containing
 * the expressions A and B to record this knowledge.  If we later find another
 * equivalence B = C, we add C to the existing EquivalenceClass; this may
 * require merging two existing EquivalenceClasses.  At the end of the qual
 * distribution process, we have sets of values that are known all transitively
 * equal to each other, where "equal" is according to the rules of the btree
 * operator family(s) shown in ec_opfamilies, as well as the collation shown
 * by ec_collation.  (We restrict an EC to contain only equalities whose
 * operators belong to the same set of opfamilies.  This could probably be
 * relaxed, but for now it's not worth the trouble, since nearly all equality
 * operators belong to only one btree opclass anyway.  Similarly, we suppose
 * that all or none of the input datatypes are collatable, so that a single
 * collation value is sufficient.)
 *
 * We also use EquivalenceClasses as the base structure for PathKeys, letting
 * us represent knowledge about different sort orderings being equivalent.
 * Since every PathKey must reference an EquivalenceClass, we will end up
 * with single-member EquivalenceClasses whenever a sort key expression has
 * not been equivalenced to anything else.  It is also possible that such an
 * EquivalenceClass will contain a volatile expression ("ORDER BY random()"),
 * which is a case that can't arise otherwise since clauses containing
 * volatile functions are never considered mergejoinable.  We mark such
 * EquivalenceClasses specially to prevent them from being merged with
 * ordinary EquivalenceClasses.  Also, for volatile expressions we have
 * to be careful to match the EquivalenceClass to the correct targetlist
 * entry: consider SELECT random() AS a, random() AS b ... ORDER BY b,a.
 * So we record the SortGroupRef of the originating sort clause.
 *
 * We allow equality clauses appearing below the nullable side of an outer join
 * to form EquivalenceClasses, but these have a slightly different meaning:
 * the included values might be all NULL rather than all the same non-null
 * values.  See src/backend/optimizer/README for more on that point.
 *
 * NB: if ec_merged isn't NULL, this class has been merged into another, and
 * should be ignored in favor of using the pointed-to class.
 */
typedef struct EquivalenceClass
{
	NodeTag		type;

	List	   *ec_opfamilies;	/* btree operator family OIDs */
	Oid			ec_collation;	/* collation, if datatypes are collatable */
	List	   *ec_members;		/* list of EquivalenceMembers */
	List	   *ec_sources;		/* list of generating RestrictInfos */
	List	   *ec_derives;		/* list of derived RestrictInfos */
	Relids		ec_relids;		/* all relids appearing in ec_members, except
								 * for child members (see below) */
	bool		ec_has_const;	/* any pseudoconstants in ec_members? */
	bool		ec_has_volatile;	/* the (sole) member is a volatile expr */
	bool		ec_below_outer_join;	/* equivalence applies below an OJ */
	bool		ec_broken;		/* failed to generate needed clauses? */
	Index		ec_sortref;		/* originating sortclause label, or 0 */
	struct EquivalenceClass *ec_merged; /* set if merged into another EC */
} EquivalenceClass;

/*
 * If an EC contains a const and isn't below-outer-join, any PathKey depending
 * on it must be redundant, since there's only one possible value of the key.
 */
#define EC_MUST_BE_REDUNDANT(eclass)  \
	((eclass)->ec_has_const && !(eclass)->ec_below_outer_join)

/*
 * EquivalenceMember - one member expression of an EquivalenceClass
 *
 * em_is_child signifies that this element was built by transposing a member
 * for an appendrel parent relation to represent the corresponding expression
 * for an appendrel child.  These members are used for determining the
 * pathkeys of scans on the child relation and for explicitly sorting the
 * child when necessary to build a MergeAppend path for the whole appendrel
 * tree.  An em_is_child member has no impact on the properties of the EC as a
 * whole; in particular the EC's ec_relids field does NOT include the child
 * relation.  An em_is_child member should never be marked em_is_const nor
 * cause ec_has_const or ec_has_volatile to be set, either.  Thus, em_is_child
 * members are not really full-fledged members of the EC, but just reflections
 * or doppelgangers of real members.  Most operations on EquivalenceClasses
 * should ignore em_is_child members, and those that don't should test
 * em_relids to make sure they only consider relevant members.
 *
 * em_datatype is usually the same as exprType(em_expr), but can be
 * different when dealing with a binary-compatible opfamily; in particular
 * anyarray_ops would never work without this.  Use em_datatype when
 * looking up a specific btree operator to work with this expression.
 */
typedef struct EquivalenceMember
{
	NodeTag		type;

	Expr	   *em_expr;		/* the expression represented */
	Relids		em_relids;		/* all relids appearing in em_expr */
	Relids		em_nullable_relids;		/* nullable by lower outer joins */
	bool		em_is_const;	/* expression is pseudoconstant? */
	bool		em_is_child;	/* derived version for a child relation? */
	Oid			em_datatype;	/* the "nominal type" used by the opfamily */
} EquivalenceMember;

/*
 * PathKeys
 *
 * The sort ordering of a path is represented by a list of PathKey nodes.
 * An empty list implies no known ordering.  Otherwise the first item
 * represents the primary sort key, the second the first secondary sort key,
 * etc.  The value being sorted is represented by linking to an
 * EquivalenceClass containing that value and including pk_opfamily among its
 * ec_opfamilies.  The EquivalenceClass tells which collation to use, too.
 * This is a convenient method because it makes it trivial to detect
 * equivalent and closely-related orderings. (See optimizer/README for more
 * information.)
 *
 * Note: pk_strategy is either BTLessStrategyNumber (for ASC) or
 * BTGreaterStrategyNumber (for DESC).  We assume that all ordering-capable
 * index types will use btree-compatible strategy numbers.
 */
typedef struct PathKey
{
	NodeTag		type;

	EquivalenceClass *pk_eclass;	/* the value that is ordered */
	Oid			pk_opfamily;	/* btree opfamily defining the ordering */
	int			pk_strategy;	/* sort direction (ASC or DESC) */
	bool		pk_nulls_first; /* do NULLs come before normal values? */
} PathKey;


/*
 * ParamPathInfo
 *
 * All parameterized paths for a given relation with given required outer rels
 * link to a single ParamPathInfo, which stores common information such as
 * the estimated rowcount for this parameterization.  We do this partly to
 * avoid recalculations, but mostly to ensure that the estimated rowcount
 * is in fact the same for every such path.
 *
 * Note: ppi_clauses is only used in ParamPathInfos for base relation paths;
 * in join cases it's NIL because the set of relevant clauses varies depending
 * on how the join is formed.  The relevant clauses will appear in each
 * parameterized join path's joinrestrictinfo list, instead.
 */
typedef struct ParamPathInfo
{
	NodeTag		type;

	Relids		ppi_req_outer;	/* rels supplying parameters used by path */
	double		ppi_rows;		/* estimated number of result tuples */
	List	   *ppi_clauses;	/* join clauses available from outer rels */
} ParamPathInfo;


/*
 * Type "Path" is used as-is for sequential-scan paths, as well as some other
 * simple plan types that we don't need any extra information in the path for.
 * For other path types it is the first component of a larger struct.
 *
 * "pathtype" is the NodeTag of the Plan node we could build from this Path.
 * It is partially redundant with the Path's NodeTag, but allows us to use
 * the same Path type for multiple Plan types when there is no need to
 * distinguish the Plan type during path processing.
 *
 * "param_info", if not NULL, links to a ParamPathInfo that identifies outer
 * relation(s) that provide parameter values to each scan of this path.
 * That means this path can only be joined to those rels by means of nestloop
 * joins with this path on the inside.  Also note that a parameterized path
 * is responsible for testing all "movable" joinclauses involving this rel
 * and the specified outer rel(s).
 *
 * "rows" is the same as parent->rows in simple paths, but in parameterized
 * paths and UniquePaths it can be less than parent->rows, reflecting the
 * fact that we've filtered by extra join conditions or removed duplicates.
 *
 * "pathkeys" is a List of PathKey nodes (see above), describing the sort
 * ordering of the path's output rows.
 */
typedef struct Path
{
	NodeTag		type;

	NodeTag		pathtype;		/* tag identifying scan/join method */

	RelOptInfo *parent;			/* the relation this path can build */
	ParamPathInfo *param_info;	/* parameterization info, or NULL if none */

	/* estimated size/costs for path (see costsize.c for more info) */
	double		rows;			/* estimated number of result tuples */
	Cost		startup_cost;	/* cost expended before fetching any tuples */
	Cost		total_cost;		/* total cost (assuming all tuples fetched) */

	List	   *pathkeys;		/* sort ordering of path's output */
	/* pathkeys is a List of PathKey nodes; see above */
} Path;

/* Macro for extracting a path's parameterization relids; beware double eval */
#define PATH_REQ_OUTER(path)  \
	((path)->param_info ? (path)->param_info->ppi_req_outer : (Relids) NULL)

/*----------
 * IndexPath represents an index scan over a single index.
 *
 * This struct is used for both regular indexscans and index-only scans;
 * path.pathtype is T_IndexScan or T_IndexOnlyScan to show which is meant.
 *
 * 'indexinfo' is the index to be scanned.
 *
 * 'indexclauses' is a list of index qualification clauses, with implicit
 * AND semantics across the list.  Each clause is a RestrictInfo node from
 * the query's WHERE or JOIN conditions.  An empty list implies a full
 * index scan.
 *
 * 'indexquals' has the same structure as 'indexclauses', but it contains
 * the actual index qual conditions that can be used with the index.
 * In simple cases this is identical to 'indexclauses', but when special
 * indexable operators appear in 'indexclauses', they are replaced by the
 * derived indexscannable conditions in 'indexquals'.
 *
 * 'indexqualcols' is an integer list of index column numbers (zero-based)
 * of the same length as 'indexquals', showing which index column each qual
 * is meant to be used with.  'indexquals' is required to be ordered by
 * index column, so 'indexqualcols' must form a nondecreasing sequence.
 * (The order of multiple quals for the same index column is unspecified.)
 *
 * 'indexorderbys', if not NIL, is a list of ORDER BY expressions that have
 * been found to be usable as ordering operators for an amcanorderbyop index.
 * The list must match the path's pathkeys, ie, one expression per pathkey
 * in the same order.  These are not RestrictInfos, just bare expressions,
 * since they generally won't yield booleans.  Also, unlike the case for
 * quals, it's guaranteed that each expression has the index key on the left
 * side of the operator.
 *
 * 'indexorderbycols' is an integer list of index column numbers (zero-based)
 * of the same length as 'indexorderbys', showing which index column each
 * ORDER BY expression is meant to be used with.  (There is no restriction
 * on which index column each ORDER BY can be used with.)
 *
 * 'indexscandir' is one of:
 *		ForwardScanDirection: forward scan of an ordered index
 *		BackwardScanDirection: backward scan of an ordered index
 *		NoMovementScanDirection: scan of an unordered index, or don't care
 * (The executor doesn't care whether it gets ForwardScanDirection or
 * NoMovementScanDirection for an indexscan, but the planner wants to
 * distinguish ordered from unordered indexes for building pathkeys.)
 *
 * 'indextotalcost' and 'indexselectivity' are saved in the IndexPath so that
 * we need not recompute them when considering using the same index in a
 * bitmap index/heap scan (see BitmapHeapPath).  The costs of the IndexPath
 * itself represent the costs of an IndexScan or IndexOnlyScan plan type.
 *----------
 */
typedef struct IndexPath
{
	Path		path;
	IndexOptInfo *indexinfo;
	List	   *indexclauses;
	List	   *indexquals;
	List	   *indexqualcols;
	List	   *indexorderbys;
	List	   *indexorderbycols;
	ScanDirection indexscandir;
	Cost		indextotalcost;
	Selectivity indexselectivity;
} IndexPath;

/*
 * BitmapHeapPath represents one or more indexscans that generate TID bitmaps
 * instead of directly accessing the heap, followed by AND/OR combinations
 * to produce a single bitmap, followed by a heap scan that uses the bitmap.
 * Note that the output is always considered unordered, since it will come
 * out in physical heap order no matter what the underlying indexes did.
 *
 * The individual indexscans are represented by IndexPath nodes, and any
 * logic on top of them is represented by a tree of BitmapAndPath and
 * BitmapOrPath nodes.  Notice that we can use the same IndexPath node both
 * to represent a regular (or index-only) index scan plan, and as the child
 * of a BitmapHeapPath that represents scanning the same index using a
 * BitmapIndexScan.  The startup_cost and total_cost figures of an IndexPath
 * always represent the costs to use it as a regular (or index-only)
 * IndexScan.  The costs of a BitmapIndexScan can be computed using the
 * IndexPath's indextotalcost and indexselectivity.
 */
typedef struct BitmapHeapPath
{
	Path		path;
	Path	   *bitmapqual;		/* IndexPath, BitmapAndPath, BitmapOrPath */
} BitmapHeapPath;

/*
 * BitmapAndPath represents a BitmapAnd plan node; it can only appear as
 * part of the substructure of a BitmapHeapPath.  The Path structure is
 * a bit more heavyweight than we really need for this, but for simplicity
 * we make it a derivative of Path anyway.
 */
typedef struct BitmapAndPath
{
	Path		path;
	List	   *bitmapquals;	/* IndexPaths and BitmapOrPaths */
	Selectivity bitmapselectivity;
} BitmapAndPath;

/*
 * BitmapOrPath represents a BitmapOr plan node; it can only appear as
 * part of the substructure of a BitmapHeapPath.  The Path structure is
 * a bit more heavyweight than we really need for this, but for simplicity
 * we make it a derivative of Path anyway.
 */
typedef struct BitmapOrPath
{
	Path		path;
	List	   *bitmapquals;	/* IndexPaths and BitmapAndPaths */
	Selectivity bitmapselectivity;
} BitmapOrPath;

/*
 * TidPath represents a scan by TID
 *
 * tidquals is an implicitly OR'ed list of qual expressions of the form
 * "CTID = pseudoconstant" or "CTID = ANY(pseudoconstant_array)".
 * Note they are bare expressions, not RestrictInfos.
 */
typedef struct TidPath
{
	Path		path;
	List	   *tidquals;		/* qual(s) involving CTID = something */
} TidPath;

/*
 * ForeignPath represents a potential scan of a foreign table
 *
 * fdw_private stores FDW private data about the scan.  While fdw_private is
 * not actually touched by the core code during normal operations, it's
 * generally a good idea to use a representation that can be dumped by
 * nodeToString(), so that you can examine the structure during debugging
 * with tools like pprint().
 */
typedef struct ForeignPath
{
	Path		path;
	Path	   *fdw_outerpath;
	List	   *fdw_private;
} ForeignPath;

/*
 * CustomPath represents a table scan done by some out-of-core extension.
 *
 * We provide a set of hooks here - which the provider must take care to set
 * up correctly - to allow extensions to supply their own methods of scanning
 * a relation.  For example, a provider might provide GPU acceleration, a
 * cache-based scan, or some other kind of logic we haven't dreamed up yet.
 *
 * CustomPaths can be injected into the planning process for a relation by
 * set_rel_pathlist_hook functions.
 *
 * Core code must avoid assuming that the CustomPath is only as large as
 * the structure declared here; providers are allowed to make it the first
 * element in a larger structure.  (Since the planner never copies Paths,
 * this doesn't add any complication.)  However, for consistency with the
 * FDW case, we provide a "custom_private" field in CustomPath; providers
 * may prefer to use that rather than define another struct type.
 */
struct CustomPath;

#define CUSTOMPATH_SUPPORT_BACKWARD_SCAN	0x0001
#define CUSTOMPATH_SUPPORT_MARK_RESTORE		0x0002

typedef struct CustomPathMethods
{
	const char *CustomName;

	/* Convert Path to a Plan */
	struct Plan *(*PlanCustomPath) (PlannerInfo *root,
												RelOptInfo *rel,
												struct CustomPath *best_path,
												List *tlist,
												List *clauses,
												List *custom_plans);
	/* Optional: print additional fields besides "private" */
	void		(*TextOutCustomPath) (StringInfo str,
											  const struct CustomPath *node);
} CustomPathMethods;

typedef struct CustomPath
{
	Path		path;
	uint32		flags;			/* mask of CUSTOMPATH_* flags, see above */
	List	   *custom_paths;	/* list of child Path nodes, if any */
	List	   *custom_private;
	const CustomPathMethods *methods;
} CustomPath;

/*
 * AppendPath represents an Append plan, ie, successive execution of
 * several member plans.
 *
 * Note: it is possible for "subpaths" to contain only one, or even no,
 * elements.  These cases are optimized during create_append_plan.
 * In particular, an AppendPath with no subpaths is a "dummy" path that
 * is created to represent the case that a relation is provably empty.
 */
typedef struct AppendPath
{
	Path		path;
	List	   *subpaths;		/* list of component Paths */
} AppendPath;

#define IS_DUMMY_PATH(p) \
	(IsA((p), AppendPath) && ((AppendPath *) (p))->subpaths == NIL)

/* A relation that's been proven empty will have one path that is dummy */
#define IS_DUMMY_REL(r) \
	((r)->cheapest_total_path != NULL && \
	 IS_DUMMY_PATH((r)->cheapest_total_path))

/*
 * MergeAppendPath represents a MergeAppend plan, ie, the merging of sorted
 * results from several member plans to produce similarly-sorted output.
 */
typedef struct MergeAppendPath
{
	Path		path;
	List	   *subpaths;		/* list of component Paths */
	double		limit_tuples;	/* hard limit on output tuples, or -1 */
} MergeAppendPath;

/*
 * ResultPath represents use of a Result plan node to compute a variable-free
 * targetlist with no underlying tables (a "SELECT expressions" query).
 * The query could have a WHERE clause, too, represented by "quals".
 *
 * Note that quals is a list of bare clauses, not RestrictInfos.
 */
typedef struct ResultPath
{
	Path		path;
	List	   *quals;
} ResultPath;

/*
 * MaterialPath represents use of a Material plan node, i.e., caching of
 * the output of its subpath.  This is used when the subpath is expensive
 * and needs to be scanned repeatedly, or when we need mark/restore ability
 * and the subpath doesn't have it.
 */
typedef struct MaterialPath
{
	Path		path;
	Path	   *subpath;
} MaterialPath;

/*
 * UniquePath represents elimination of distinct rows from the output of
 * its subpath.
 *
 * This is unlike the other Path nodes in that it can actually generate
 * different plans: either hash-based or sort-based implementation, or a
 * no-op if the input path can be proven distinct already.  The decision
 * is sufficiently localized that it's not worth having separate Path node
 * types.  (Note: in the no-op case, we could eliminate the UniquePath node
 * entirely and just return the subpath; but it's convenient to have a
 * UniquePath in the path tree to signal upper-level routines that the input
 * is known distinct.)
 */
typedef enum
{
	UNIQUE_PATH_NOOP,			/* input is known unique already */
	UNIQUE_PATH_HASH,			/* use hashing */
	UNIQUE_PATH_SORT			/* use sorting */
} UniquePathMethod;

typedef struct UniquePath
{
	Path		path;
	Path	   *subpath;
	UniquePathMethod umethod;
	List	   *in_operators;	/* equality operators of the IN clause */
	List	   *uniq_exprs;		/* expressions to be made unique */
} UniquePath;

/*
 * All join-type paths share these fields.
 */

typedef struct JoinPath
{
	Path		path;

	JoinType	jointype;

	Path	   *outerjoinpath;	/* path for the outer side of the join */
	Path	   *innerjoinpath;	/* path for the inner side of the join */

	List	   *joinrestrictinfo;		/* RestrictInfos to apply to join */

	/*
	 * See the notes for RelOptInfo and ParamPathInfo to understand why
	 * joinrestrictinfo is needed in JoinPath, and can't be merged into the
	 * parent RelOptInfo.
	 */
} JoinPath;

/*
 * A nested-loop path needs no special fields.
 */

typedef JoinPath NestPath;

/*
 * A mergejoin path has these fields.
 *
 * Unlike other path types, a MergePath node doesn't represent just a single
 * run-time plan node: it can represent up to four.  Aside from the MergeJoin
 * node itself, there can be a Sort node for the outer input, a Sort node
 * for the inner input, and/or a Material node for the inner input.  We could
 * represent these nodes by separate path nodes, but considering how many
 * different merge paths are investigated during a complex join problem,
 * it seems better to avoid unnecessary palloc overhead.
 *
 * path_mergeclauses lists the clauses (in the form of RestrictInfos)
 * that will be used in the merge.
 *
 * Note that the mergeclauses are a subset of the parent relation's
 * restriction-clause list.  Any join clauses that are not mergejoinable
 * appear only in the parent's restrict list, and must be checked by a
 * qpqual at execution time.
 *
 * outersortkeys (resp. innersortkeys) is NIL if the outer path
 * (resp. inner path) is already ordered appropriately for the
 * mergejoin.  If it is not NIL then it is a PathKeys list describing
 * the ordering that must be created by an explicit Sort node.
 *
 * materialize_inner is TRUE if a Material node should be placed atop the
 * inner input.  This may appear with or without an inner Sort step.
 */

typedef struct MergePath
{
	JoinPath	jpath;
	List	   *path_mergeclauses;		/* join clauses to be used for merge */
	List	   *outersortkeys;	/* keys for explicit sort, if any */
	List	   *innersortkeys;	/* keys for explicit sort, if any */
	bool		materialize_inner;		/* add Materialize to inner? */
} MergePath;

/*
 * A hashjoin path has these fields.
 *
 * The remarks above for mergeclauses apply for hashclauses as well.
 *
 * Hashjoin does not care what order its inputs appear in, so we have
 * no need for sortkeys.
 */

typedef struct HashPath
{
	JoinPath	jpath;
	List	   *path_hashclauses;		/* join clauses used for hashing */
	int			num_batches;	/* number of batches expected */
} HashPath;

/*
 * Restriction clause info.
 *
 * We create one of these for each AND sub-clause of a restriction condition
 * (WHERE or JOIN/ON clause).  Since the restriction clauses are logically
 * ANDed, we can use any one of them or any subset of them to filter out
 * tuples, without having to evaluate the rest.  The RestrictInfo node itself
 * stores data used by the optimizer while choosing the best query plan.
 *
 * If a restriction clause references a single base relation, it will appear
 * in the baserestrictinfo list of the RelOptInfo for that base rel.
 *
 * If a restriction clause references more than one base rel, it will
 * appear in the joininfo list of every RelOptInfo that describes a strict
 * subset of the base rels mentioned in the clause.  The joininfo lists are
 * used to drive join tree building by selecting plausible join candidates.
 * The clause cannot actually be applied until we have built a join rel
 * containing all the base rels it references, however.
 *
 * When we construct a join rel that includes all the base rels referenced
 * in a multi-relation restriction clause, we place that clause into the
 * joinrestrictinfo lists of paths for the join rel, if neither left nor
 * right sub-path includes all base rels referenced in the clause.  The clause
 * will be applied at that join level, and will not propagate any further up
 * the join tree.  (Note: the "predicate migration" code was once intended to
 * push restriction clauses up and down the plan tree based on evaluation
 * costs, but it's dead code and is unlikely to be resurrected in the
 * foreseeable future.)
 *
 * Note that in the presence of more than two rels, a multi-rel restriction
 * might reach different heights in the join tree depending on the join
 * sequence we use.  So, these clauses cannot be associated directly with
 * the join RelOptInfo, but must be kept track of on a per-join-path basis.
 *
 * RestrictInfos that represent equivalence conditions (i.e., mergejoinable
 * equalities that are not outerjoin-delayed) are handled a bit differently.
 * Initially we attach them to the EquivalenceClasses that are derived from
 * them.  When we construct a scan or join path, we look through all the
 * EquivalenceClasses and generate derived RestrictInfos representing the
 * minimal set of conditions that need to be checked for this particular scan
 * or join to enforce that all members of each EquivalenceClass are in fact
 * equal in all rows emitted by the scan or join.
 *
 * When dealing with outer joins we have to be very careful about pushing qual
 * clauses up and down the tree.  An outer join's own JOIN/ON conditions must
 * be evaluated exactly at that join node, unless they are "degenerate"
 * conditions that reference only Vars from the nullable side of the join.
 * Quals appearing in WHERE or in a JOIN above the outer join cannot be pushed
 * down below the outer join, if they reference any nullable Vars.
 * RestrictInfo nodes contain a flag to indicate whether a qual has been
 * pushed down to a lower level than its original syntactic placement in the
 * join tree would suggest.  If an outer join prevents us from pushing a qual
 * down to its "natural" semantic level (the level associated with just the
 * base rels used in the qual) then we mark the qual with a "required_relids"
 * value including more than just the base rels it actually uses.  By
 * pretending that the qual references all the rels required to form the outer
 * join, we prevent it from being evaluated below the outer join's joinrel.
 * When we do form the outer join's joinrel, we still need to distinguish
 * those quals that are actually in that join's JOIN/ON condition from those
 * that appeared elsewhere in the tree and were pushed down to the join rel
 * because they used no other rels.  That's what the is_pushed_down flag is
 * for; it tells us that a qual is not an OUTER JOIN qual for the set of base
 * rels listed in required_relids.  A clause that originally came from WHERE
 * or an INNER JOIN condition will *always* have its is_pushed_down flag set.
 * It's possible for an OUTER JOIN clause to be marked is_pushed_down too,
 * if we decide that it can be pushed down into the nullable side of the join.
 * In that case it acts as a plain filter qual for wherever it gets evaluated.
 * (In short, is_pushed_down is only false for non-degenerate outer join
 * conditions.  Possibly we should rename it to reflect that meaning?)
 *
 * RestrictInfo nodes also contain an outerjoin_delayed flag, which is true
 * if the clause's applicability must be delayed due to any outer joins
 * appearing below it (ie, it has to be postponed to some join level higher
 * than the set of relations it actually references).
 *
 * There is also an outer_relids field, which is NULL except for outer join
 * clauses; for those, it is the set of relids on the outer side of the
 * clause's outer join.  (These are rels that the clause cannot be applied to
 * in parameterized scans, since pushing it into the join's outer side would
 * lead to wrong answers.)
 *
 * There is also a nullable_relids field, which is the set of rels the clause
 * references that can be forced null by some outer join below the clause.
 *
 * outerjoin_delayed = true is subtly different from nullable_relids != NULL:
 * a clause might reference some nullable rels and yet not be
 * outerjoin_delayed because it also references all the other rels of the
 * outer join(s). A clause that is not outerjoin_delayed can be enforced
 * anywhere it is computable.
 *
 * In general, the referenced clause might be arbitrarily complex.  The
 * kinds of clauses we can handle as indexscan quals, mergejoin clauses,
 * or hashjoin clauses are limited (e.g., no volatile functions).  The code
 * for each kind of path is responsible for identifying the restrict clauses
 * it can use and ignoring the rest.  Clauses not implemented by an indexscan,
 * mergejoin, or hashjoin will be placed in the plan qual or joinqual field
 * of the finished Plan node, where they will be enforced by general-purpose
 * qual-expression-evaluation code.  (But we are still entitled to count
 * their selectivity when estimating the result tuple count, if we
 * can guess what it is...)
 *
 * When the referenced clause is an OR clause, we generate a modified copy
 * in which additional RestrictInfo nodes are inserted below the top-level
 * OR/AND structure.  This is a convenience for OR indexscan processing:
 * indexquals taken from either the top level or an OR subclause will have
 * associated RestrictInfo nodes.
 *
 * The can_join flag is set true if the clause looks potentially useful as
 * a merge or hash join clause, that is if it is a binary opclause with
 * nonoverlapping sets of relids referenced in the left and right sides.
 * (Whether the operator is actually merge or hash joinable isn't checked,
 * however.)
 *
 * The pseudoconstant flag is set true if the clause contains no Vars of
 * the current query level and no volatile functions.  Such a clause can be
 * pulled out and used as a one-time qual in a gating Result node.  We keep
 * pseudoconstant clauses in the same lists as other RestrictInfos so that
 * the regular clause-pushing machinery can assign them to the correct join
 * level, but they need to be treated specially for cost and selectivity
 * estimates.  Note that a pseudoconstant clause can never be an indexqual
 * or merge or hash join clause, so it's of no interest to large parts of
 * the planner.
 *
 * When join clauses are generated from EquivalenceClasses, there may be
 * several equally valid ways to enforce join equivalence, of which we need
 * apply only one.  We mark clauses of this kind by setting parent_ec to
 * point to the generating EquivalenceClass.  Multiple clauses with the same
 * parent_ec in the same join are redundant.
 */

typedef struct RestrictInfo
{
	NodeTag		type;

	Expr	   *clause;			/* the represented clause of WHERE or JOIN */

	bool		is_pushed_down; /* TRUE if clause was pushed down in level */

	bool		outerjoin_delayed;		/* TRUE if delayed by lower outer join */

	bool		can_join;		/* see comment above */

	bool		pseudoconstant; /* see comment above */

	/* The set of relids (varnos) actually referenced in the clause: */
	Relids		clause_relids;

	/* The set of relids required to evaluate the clause: */
	Relids		required_relids;

	/* If an outer-join clause, the outer-side relations, else NULL: */
	Relids		outer_relids;

	/* The relids used in the clause that are nullable by lower outer joins: */
	Relids		nullable_relids;

	/* These fields are set for any binary opclause: */
	Relids		left_relids;	/* relids in left side of clause */
	Relids		right_relids;	/* relids in right side of clause */

	/* This field is NULL unless clause is an OR clause: */
	Expr	   *orclause;		/* modified clause with RestrictInfos */

	/* This field is NULL unless clause is potentially redundant: */
	EquivalenceClass *parent_ec;	/* generating EquivalenceClass */

	/* cache space for cost and selectivity */
	QualCost	eval_cost;		/* eval cost of clause; -1 if not yet set */
	Selectivity norm_selec;		/* selectivity for "normal" (JOIN_INNER)
								 * semantics; -1 if not yet set; >1 means a
								 * redundant clause */
	Selectivity outer_selec;	/* selectivity for outer join semantics; -1 if
								 * not yet set */

	/* valid if clause is mergejoinable, else NIL */
	List	   *mergeopfamilies;	/* opfamilies containing clause operator */

	/* cache space for mergeclause processing; NULL if not yet set */
	EquivalenceClass *left_ec;	/* EquivalenceClass containing lefthand */
	EquivalenceClass *right_ec; /* EquivalenceClass containing righthand */
	EquivalenceMember *left_em; /* EquivalenceMember for lefthand */
	EquivalenceMember *right_em;	/* EquivalenceMember for righthand */
	List	   *scansel_cache;	/* list of MergeScanSelCache structs */

	/* transient workspace for use while considering a specific join path */
	bool		outer_is_left;	/* T = outer var on left, F = on right */

	/* valid if clause is hashjoinable, else InvalidOid: */
	Oid			hashjoinoperator;		/* copy of clause operator */

	/* cache space for hashclause processing; -1 if not yet set */
	Selectivity left_bucketsize;	/* avg bucketsize of left side */
	Selectivity right_bucketsize;		/* avg bucketsize of right side */
} RestrictInfo;

/*
 * Since mergejoinscansel() is a relatively expensive function, and would
 * otherwise be invoked many times while planning a large join tree,
 * we go out of our way to cache its results.  Each mergejoinable
 * RestrictInfo carries a list of the specific sort orderings that have
 * been considered for use with it, and the resulting selectivities.
 */
typedef struct MergeScanSelCache
{
	/* Ordering details (cache lookup key) */
	Oid			opfamily;		/* btree opfamily defining the ordering */
	Oid			collation;		/* collation for the ordering */
	int			strategy;		/* sort direction (ASC or DESC) */
	bool		nulls_first;	/* do NULLs come before normal values? */
	/* Results */
	Selectivity leftstartsel;	/* first-join fraction for clause left side */
	Selectivity leftendsel;		/* last-join fraction for clause left side */
	Selectivity rightstartsel;	/* first-join fraction for clause right side */
	Selectivity rightendsel;	/* last-join fraction for clause right side */
} MergeScanSelCache;

/*
 * Placeholder node for an expression to be evaluated below the top level
 * of a plan tree.  This is used during planning to represent the contained
 * expression.  At the end of the planning process it is replaced by either
 * the contained expression or a Var referring to a lower-level evaluation of
 * the contained expression.  Typically the evaluation occurs below an outer
 * join, and Var references above the outer join might thereby yield NULL
 * instead of the expression value.
 *
 * Although the planner treats this as an expression node type, it is not
 * recognized by the parser or executor, so we declare it here rather than
 * in primnodes.h.
 */

typedef struct PlaceHolderVar
{
	Expr		xpr;
	Expr	   *phexpr;			/* the represented expression */
	Relids		phrels;			/* base relids syntactically within expr src */
	Index		phid;			/* ID for PHV (unique within planner run) */
	Index		phlevelsup;		/* > 0 if PHV belongs to outer query */
} PlaceHolderVar;

/*
 * "Special join" info.
 *
 * One-sided outer joins constrain the order of joining partially but not
 * completely.  We flatten such joins into the planner's top-level list of
 * relations to join, but record information about each outer join in a
 * SpecialJoinInfo struct.  These structs are kept in the PlannerInfo node's
 * join_info_list.
 *
 * Similarly, semijoins and antijoins created by flattening IN (subselect)
 * and EXISTS(subselect) clauses create partial constraints on join order.
 * These are likewise recorded in SpecialJoinInfo structs.
 *
 * We make SpecialJoinInfos for FULL JOINs even though there is no flexibility
 * of planning for them, because this simplifies make_join_rel()'s API.
 *
 * min_lefthand and min_righthand are the sets of base relids that must be
 * available on each side when performing the special join.  lhs_strict is
 * true if the special join's condition cannot succeed when the LHS variables
 * are all NULL (this means that an outer join can commute with upper-level
 * outer joins even if it appears in their RHS).  We don't bother to set
 * lhs_strict for FULL JOINs, however.
 *
 * It is not valid for either min_lefthand or min_righthand to be empty sets;
 * if they were, this would break the logic that enforces join order.
 *
 * syn_lefthand and syn_righthand are the sets of base relids that are
 * syntactically below this special join.  (These are needed to help compute
 * min_lefthand and min_righthand for higher joins.)
 *
 * delay_upper_joins is set TRUE if we detect a pushed-down clause that has
 * to be evaluated after this join is formed (because it references the RHS).
 * Any outer joins that have such a clause and this join in their RHS cannot
 * commute with this join, because that would leave noplace to check the
 * pushed-down clause.  (We don't track this for FULL JOINs, either.)
 *
 * For a semijoin, we also extract the join operators and their RHS arguments
 * and set semi_operators, semi_rhs_exprs, semi_can_btree, and semi_can_hash.
 * This is done in support of possibly unique-ifying the RHS, so we don't
 * bother unless at least one of semi_can_btree and semi_can_hash can be set
 * true.  (You might expect that this information would be computed during
 * join planning; but it's helpful to have it available during planning of
 * parameterized table scans, so we store it in the SpecialJoinInfo structs.)
 *
 * jointype is never JOIN_RIGHT; a RIGHT JOIN is handled by switching
 * the inputs to make it a LEFT JOIN.  So the allowed values of jointype
 * in a join_info_list member are only LEFT, FULL, SEMI, or ANTI.
 *
 * For purposes of join selectivity estimation, we create transient
 * SpecialJoinInfo structures for regular inner joins; so it is possible
 * to have jointype == JOIN_INNER in such a structure, even though this is
 * not allowed within join_info_list.  We also create transient
 * SpecialJoinInfos with jointype == JOIN_INNER for outer joins, since for
 * cost estimation purposes it is sometimes useful to know the join size under
 * plain innerjoin semantics.  Note that lhs_strict, delay_upper_joins, and
 * of course the semi_xxx fields are not set meaningfully within such structs.
 */

typedef struct SpecialJoinInfo
{
	NodeTag		type;
	Relids		min_lefthand;	/* base relids in minimum LHS for join */
	Relids		min_righthand;	/* base relids in minimum RHS for join */
	Relids		syn_lefthand;	/* base relids syntactically within LHS */
	Relids		syn_righthand;	/* base relids syntactically within RHS */
	JoinType	jointype;		/* always INNER, LEFT, FULL, SEMI, or ANTI */
	bool		lhs_strict;		/* joinclause is strict for some LHS rel */
	bool		delay_upper_joins;		/* can't commute with upper RHS */
	/* Remaining fields are set only for JOIN_SEMI jointype: */
	bool		semi_can_btree; /* true if semi_operators are all btree */
	bool		semi_can_hash;	/* true if semi_operators are all hash */
	List	   *semi_operators; /* OIDs of equality join operators */
	List	   *semi_rhs_exprs; /* righthand-side expressions of these ops */
} SpecialJoinInfo;

/*
 * Append-relation info.
 *
 * When we expand an inheritable table or a UNION-ALL subselect into an
 * "append relation" (essentially, a list of child RTEs), we build an
 * AppendRelInfo for each child RTE.  The list of AppendRelInfos indicates
 * which child RTEs must be included when expanding the parent, and each
 * node carries information needed to translate Vars referencing the parent
 * into Vars referencing that child.
 *
 * These structs are kept in the PlannerInfo node's append_rel_list.
 * Note that we just throw all the structs into one list, and scan the
 * whole list when desiring to expand any one parent.  We could have used
 * a more complex data structure (eg, one list per parent), but this would
 * be harder to update during operations such as pulling up subqueries,
 * and not really any easier to scan.  Considering that typical queries
 * will not have many different append parents, it doesn't seem worthwhile
 * to complicate things.
 *
 * Note: after completion of the planner prep phase, any given RTE is an
 * append parent having entries in append_rel_list if and only if its
 * "inh" flag is set.  We clear "inh" for plain tables that turn out not
 * to have inheritance children, and (in an abuse of the original meaning
 * of the flag) we set "inh" for subquery RTEs that turn out to be
 * flattenable UNION ALL queries.  This lets us avoid useless searches
 * of append_rel_list.
 *
 * Note: the data structure assumes that append-rel members are single
 * baserels.  This is OK for inheritance, but it prevents us from pulling
 * up a UNION ALL member subquery if it contains a join.  While that could
 * be fixed with a more complex data structure, at present there's not much
 * point because no improvement in the plan could result.
 */

typedef struct AppendRelInfo
{
	NodeTag		type;

	/*
	 * These fields uniquely identify this append relationship.  There can be
	 * (in fact, always should be) multiple AppendRelInfos for the same
	 * parent_relid, but never more than one per child_relid, since a given
	 * RTE cannot be a child of more than one append parent.
	 */
	Index		parent_relid;	/* RT index of append parent rel */
	Index		child_relid;	/* RT index of append child rel */

	/*
	 * For an inheritance appendrel, the parent and child are both regular
	 * relations, and we store their rowtype OIDs here for use in translating
	 * whole-row Vars.  For a UNION-ALL appendrel, the parent and child are
	 * both subqueries with no named rowtype, and we store InvalidOid here.
	 */
	Oid			parent_reltype; /* OID of parent's composite type */
	Oid			child_reltype;	/* OID of child's composite type */

	/*
	 * The N'th element of this list is a Var or expression representing the
	 * child column corresponding to the N'th column of the parent. This is
	 * used to translate Vars referencing the parent rel into references to
	 * the child.  A list element is NULL if it corresponds to a dropped
	 * column of the parent (this is only possible for inheritance cases, not
	 * UNION ALL).  The list elements are always simple Vars for inheritance
	 * cases, but can be arbitrary expressions in UNION ALL cases.
	 *
	 * Notice we only store entries for user columns (attno > 0).  Whole-row
	 * Vars are special-cased, and system columns (attno < 0) need no special
	 * translation since their attnos are the same for all tables.
	 *
	 * Caution: the Vars have varlevelsup = 0.  Be careful to adjust as needed
	 * when copying into a subquery.
	 */
	List	   *translated_vars;	/* Expressions in the child's Vars */

	/*
	 * We store the parent table's OID here for inheritance, or InvalidOid for
	 * UNION ALL.  This is only needed to help in generating error messages if
	 * an attempt is made to reference a dropped parent column.
	 */
	Oid			parent_reloid;	/* OID of parent relation */
} AppendRelInfo;

/*
 * For each distinct placeholder expression generated during planning, we
 * store a PlaceHolderInfo node in the PlannerInfo node's placeholder_list.
 * This stores info that is needed centrally rather than in each copy of the
 * PlaceHolderVar.  The phid fields identify which PlaceHolderInfo goes with
 * each PlaceHolderVar.  Note that phid is unique throughout a planner run,
 * not just within a query level --- this is so that we need not reassign ID's
 * when pulling a subquery into its parent.
 *
 * The idea is to evaluate the expression at (only) the ph_eval_at join level,
 * then allow it to bubble up like a Var until the ph_needed join level.
 * ph_needed has the same definition as attr_needed for a regular Var.
 *
 * The PlaceHolderVar's expression might contain LATERAL references to vars
 * coming from outside its syntactic scope.  If so, those rels are *not*
 * included in ph_eval_at, but they are recorded in ph_lateral.
 *
 * Notice that when ph_eval_at is a join rather than a single baserel, the
 * PlaceHolderInfo may create constraints on join order: the ph_eval_at join
 * has to be formed below any outer joins that should null the PlaceHolderVar.
 *
 * We create a PlaceHolderInfo only after determining that the PlaceHolderVar
 * is actually referenced in the plan tree, so that unreferenced placeholders
 * don't result in unnecessary constraints on join order.
 */

typedef struct PlaceHolderInfo
{
	NodeTag		type;

	Index		phid;			/* ID for PH (unique within planner run) */
	PlaceHolderVar *ph_var;		/* copy of PlaceHolderVar tree */
	Relids		ph_eval_at;		/* lowest level we can evaluate value at */
	Relids		ph_lateral;		/* relids of contained lateral refs, if any */
	Relids		ph_needed;		/* highest level the value is needed at */
	int32		ph_width;		/* estimated attribute width */
} PlaceHolderInfo;

/*
 * For each potentially index-optimizable MIN/MAX aggregate function,
 * root->minmax_aggs stores a MinMaxAggInfo describing it.
 */
typedef struct MinMaxAggInfo
{
	NodeTag		type;

	Oid			aggfnoid;		/* pg_proc Oid of the aggregate */
	Oid			aggsortop;		/* Oid of its sort operator */
	Expr	   *target;			/* expression we are aggregating on */
	PlannerInfo *subroot;		/* modified "root" for planning the subquery */
	Path	   *path;			/* access path for subquery */
	Cost		pathcost;		/* estimated cost to fetch first row */
	Param	   *param;			/* param for subplan's output */
} MinMaxAggInfo;

/*
 * At runtime, PARAM_EXEC slots are used to pass values around from one plan
 * node to another.  They can be used to pass values down into subqueries (for
 * outer references in subqueries), or up out of subqueries (for the results
 * of a subplan), or from a NestLoop plan node into its inner relation (when
 * the inner scan is parameterized with values from the outer relation).
 * The planner is responsible for assigning nonconflicting PARAM_EXEC IDs to
 * the PARAM_EXEC Params it generates.
 *
 * Outer references are managed via root->plan_params, which is a list of
 * PlannerParamItems.  While planning a subquery, each parent query level's
 * plan_params contains the values required from it by the current subquery.
 * During create_plan(), we use plan_params to track values that must be
 * passed from outer to inner sides of NestLoop plan nodes.
 *
 * The item a PlannerParamItem represents can be one of three kinds:
 *
 * A Var: the slot represents a variable of this level that must be passed
 * down because subqueries have outer references to it, or must be passed
 * from a NestLoop node to its inner scan.  The varlevelsup value in the Var
 * will always be zero.
 *
 * A PlaceHolderVar: this works much like the Var case, except that the
 * entry is a PlaceHolderVar node with a contained expression.  The PHV
 * will have phlevelsup = 0, and the contained expression is adjusted
 * to match in level.
 *
 * An Aggref (with an expression tree representing its argument): the slot
 * represents an aggregate expression that is an outer reference for some
 * subquery.  The Aggref itself has agglevelsup = 0, and its argument tree
 * is adjusted to match in level.
 *
 * Note: we detect duplicate Var and PlaceHolderVar parameters and coalesce
 * them into one slot, but we do not bother to do that for Aggrefs.
 * The scope of duplicate-elimination only extends across the set of
 * parameters passed from one query level into a single subquery, or for
 * nestloop parameters across the set of nestloop parameters used in a single
 * query level.  So there is no possibility of a PARAM_EXEC slot being used
 * for conflicting purposes.
 *
 * In addition, PARAM_EXEC slots are assigned for Params representing outputs
 * from subplans (values that are setParam items for those subplans).  These
 * IDs need not be tracked via PlannerParamItems, since we do not need any
 * duplicate-elimination nor later processing of the represented expressions.
 * Instead, we just record the assignment of the slot number by incrementing
 * root->glob->nParamExec.
 */
typedef struct PlannerParamItem
{
	NodeTag		type;

	Node	   *item;			/* the Var, PlaceHolderVar, or Aggref */
	int			paramId;		/* its assigned PARAM_EXEC slot number */
} PlannerParamItem;

/*
 * When making cost estimates for a SEMI or ANTI join, there are some
 * correction factors that are needed in both nestloop and hash joins
 * to account for the fact that the executor can stop scanning inner rows
 * as soon as it finds a match to the current outer row.  These numbers
 * depend only on the selected outer and inner join relations, not on the
 * particular paths used for them, so it's worthwhile to calculate them
 * just once per relation pair not once per considered path.  This struct
 * is filled by compute_semi_anti_join_factors and must be passed along
 * to the join cost estimation functions.
 *
 * outer_match_frac is the fraction of the outer tuples that are
 *		expected to have at least one match.
 * match_count is the average number of matches expected for
 *		outer tuples that have at least one match.
 */
typedef struct SemiAntiJoinFactors
{
	Selectivity outer_match_frac;
	Selectivity match_count;
} SemiAntiJoinFactors;

/*
 * Struct for extra information passed to subroutines of add_paths_to_joinrel
 *
 * restrictlist contains all of the RestrictInfo nodes for restriction
 *		clauses that apply to this join
 * mergeclause_list is a list of RestrictInfo nodes for available
 *		mergejoin clauses in this join
 * sjinfo is extra info about special joins for selectivity estimation
 * semifactors is as shown above (only valid for SEMI or ANTI joins)
 * param_source_rels are OK targets for parameterization of result paths
 */
typedef struct JoinPathExtraData
{
	List	   *restrictlist;
	List	   *mergeclause_list;
	SpecialJoinInfo *sjinfo;
	SemiAntiJoinFactors semifactors;
	Relids		param_source_rels;
} JoinPathExtraData;

/*
 * For speed reasons, cost estimation for join paths is performed in two
 * phases: the first phase tries to quickly derive a lower bound for the
 * join cost, and then we check if that's sufficient to reject the path.
 * If not, we come back for a more refined cost estimate.  The first phase
 * fills a JoinCostWorkspace struct with its preliminary cost estimates
 * and possibly additional intermediate values.  The second phase takes
 * these values as inputs to avoid repeating work.
 *
 * (Ideally we'd declare this in cost.h, but it's also needed in pathnode.h,
 * so seems best to put it here.)
 */
typedef struct JoinCostWorkspace
{
	/* Preliminary cost estimates --- must not be larger than final ones! */
	Cost		startup_cost;	/* cost expended before fetching any tuples */
	Cost		total_cost;		/* total cost (assuming all tuples fetched) */

	/* Fields below here should be treated as private to costsize.c */
	Cost		run_cost;		/* non-startup cost components */

	/* private for cost_nestloop code */
	Cost		inner_run_cost; /* also used by cost_mergejoin code */
	Cost		inner_rescan_run_cost;

	/* private for cost_mergejoin code */
	double		outer_rows;
	double		inner_rows;
	double		outer_skip_rows;
	double		inner_skip_rows;

	/* private for cost_hashjoin code */
	int			numbuckets;
	int			numbatches;
} JoinCostWorkspace;

#endif   /* RELATION_H */
