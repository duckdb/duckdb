/*-------------------------------------------------------------------------
 *
 * plancache.h
 *	  Plan cache definitions.
 *
 * See plancache.c for comments.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/plancache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLANCACHE_H
#define PLANCACHE_H

#include "access/tupdesc.h"
#include "nodes/params.h"

#define CACHEDPLANSOURCE_MAGIC		195726186
#define CACHEDPLAN_MAGIC			953717834

/*
 * CachedPlanSource (which might better have been called CachedQuery)
 * represents a SQL query that we expect to use multiple times.  It stores
 * the query source text, the raw parse tree, and the analyzed-and-rewritten
 * query tree, as well as adjunct data.  Cache invalidation can happen as a
 * result of DDL affecting objects used by the query.  In that case we discard
 * the analyzed-and-rewritten query tree, and rebuild it when next needed.
 *
 * An actual execution plan, represented by CachedPlan, is derived from the
 * CachedPlanSource when we need to execute the query.  The plan could be
 * either generic (usable with any set of plan parameters) or custom (for a
 * specific set of parameters).  plancache.c contains the logic that decides
 * which way to do it for any particular execution.  If we are using a generic
 * cached plan then it is meant to be re-used across multiple executions, so
 * callers must always treat CachedPlans as read-only.
 *
 * Once successfully built and "saved", CachedPlanSources typically live
 * for the life of the backend, although they can be dropped explicitly.
 * CachedPlans are reference-counted and go away automatically when the last
 * reference is dropped.  A CachedPlan can outlive the CachedPlanSource it
 * was created from.
 *
 * An "unsaved" CachedPlanSource can be used for generating plans, but it
 * lives in transient storage and will not be updated in response to sinval
 * events.
 *
 * CachedPlans made from saved CachedPlanSources are likewise in permanent
 * storage, so to avoid memory leaks, the reference-counted references to them
 * must be held in permanent data structures or ResourceOwners.  CachedPlans
 * made from unsaved CachedPlanSources are in children of the caller's
 * memory context, so references to them should not be longer-lived than
 * that context.  (Reference counting is somewhat pro forma in that case,
 * though it may be useful if the CachedPlan can be discarded early.)
 *
 * A CachedPlanSource has two associated memory contexts: one that holds the
 * struct itself, the query source text and the raw parse tree, and another
 * context that holds the rewritten query tree and associated data.  This
 * allows the query tree to be discarded easily when it is invalidated.
 *
 * Some callers wish to use the CachedPlan API even with one-shot queries
 * that have no reason to be saved at all.  We therefore support a "oneshot"
 * variant that does no data copying or invalidation checking.  In this case
 * there are no separate memory contexts: the CachedPlanSource struct and
 * all subsidiary data live in the caller's CurrentMemoryContext, and there
 * is no way to free memory short of clearing that entire context.  A oneshot
 * plan is always treated as unsaved.
 *
 * Note: the string referenced by commandTag is not subsidiary storage;
 * it is assumed to be a compile-time-constant string.  As with portals,
 * commandTag shall be NULL if and only if the original query string (before
 * rewriting) was an empty string.
 */
typedef struct CachedPlanSource
{
	int			magic;			/* should equal CACHEDPLANSOURCE_MAGIC */
	Node	   *raw_parse_tree; /* output of raw_parser(), or NULL */
	const char *query_string;	/* source text of query */
	const char *commandTag;		/* command tag (a constant!), or NULL */
	Oid		   *param_types;	/* array of parameter type OIDs, or NULL */
	int			num_params;		/* length of param_types array */
	ParserSetupHook parserSetup;	/* alternative parameter spec method */
	void	   *parserSetupArg;
	int			cursor_options; /* cursor options used for planning */
	bool		fixed_result;	/* disallow change in result tupdesc? */
	TupleDesc	resultDesc;		/* result type; NULL = doesn't return tuples */
	MemoryContext context;		/* memory context holding all above */
	/* These fields describe the current analyzed-and-rewritten query tree: */
	List	   *query_list;		/* list of Query nodes, or NIL if not valid */
	List	   *relationOids;	/* OIDs of relations the queries depend on */
	List	   *invalItems;		/* other dependencies, as PlanInvalItems */
	struct OverrideSearchPath *search_path;		/* search_path used for
												 * parsing and planning */
	Oid			planUserId;		/* User-id that the plan depends on */
	MemoryContext query_context;	/* context holding the above, or NULL */
	/* If we have a generic plan, this is a reference-counted link to it: */
	struct CachedPlan *gplan;	/* generic plan, or NULL if not valid */
	/* Some state flags: */
	bool		is_oneshot;		/* is it a "oneshot" plan? */
	bool		is_complete;	/* has CompleteCachedPlan been done? */
	bool		is_saved;		/* has CachedPlanSource been "saved"? */
	bool		is_valid;		/* is the query_list currently valid? */
	int			generation;		/* increments each time we create a plan */
	/* If CachedPlanSource has been saved, it is a member of a global list */
	struct CachedPlanSource *next_saved;		/* list link, if so */
	/* State kept to help decide whether to use custom or generic plans: */
	double		generic_cost;	/* cost of generic plan, or -1 if not known */
	double		total_custom_cost;		/* total cost of custom plans so far */
	int			num_custom_plans;		/* number of plans included in total */
	bool		hasRowSecurity; /* planned with row security? */
	bool		row_security_env;		/* row security setting when planned */
} CachedPlanSource;

/*
 * CachedPlan represents an execution plan derived from a CachedPlanSource.
 * The reference count includes both the link from the parent CachedPlanSource
 * (if any), and any active plan executions, so the plan can be discarded
 * exactly when refcount goes to zero.  Both the struct itself and the
 * subsidiary data live in the context denoted by the context field.
 * This makes it easy to free a no-longer-needed cached plan.  (However,
 * if is_oneshot is true, the context does not belong solely to the CachedPlan
 * so no freeing is possible.)
 */
typedef struct CachedPlan
{
	int			magic;			/* should equal CACHEDPLAN_MAGIC */
	List	   *stmt_list;		/* list of statement nodes (PlannedStmts and
								 * bare utility statements) */
	bool		is_oneshot;		/* is it a "oneshot" plan? */
	bool		is_saved;		/* is CachedPlan in a long-lived context? */
	bool		is_valid;		/* is the stmt_list currently valid? */
	TransactionId saved_xmin;	/* if valid, replan when TransactionXmin
								 * changes from this value */
	int			generation;		/* parent's generation number for this plan */
	int			refcount;		/* count of live references to this struct */
	MemoryContext context;		/* context containing this CachedPlan */
} CachedPlan;


extern void InitPlanCache(void);
extern void ResetPlanCache(void);

extern CachedPlanSource *CreateCachedPlan(Node *raw_parse_tree,
				 const char *query_string,
				 const char *commandTag);
extern CachedPlanSource *CreateOneShotCachedPlan(Node *raw_parse_tree,
						const char *query_string,
						const char *commandTag);
extern void CompleteCachedPlan(CachedPlanSource *plansource,
				   List *querytree_list,
				   MemoryContext querytree_context,
				   Oid *param_types,
				   int num_params,
				   ParserSetupHook parserSetup,
				   void *parserSetupArg,
				   int cursor_options,
				   bool fixed_result);

extern void SaveCachedPlan(CachedPlanSource *plansource);
extern void DropCachedPlan(CachedPlanSource *plansource);

extern void CachedPlanSetParentContext(CachedPlanSource *plansource,
						   MemoryContext newcontext);

extern CachedPlanSource *CopyCachedPlan(CachedPlanSource *plansource);

extern bool CachedPlanIsValid(CachedPlanSource *plansource);

extern List *CachedPlanGetTargetList(CachedPlanSource *plansource);

extern CachedPlan *GetCachedPlan(CachedPlanSource *plansource,
			  ParamListInfo boundParams,
			  bool useResOwner);
extern void ReleaseCachedPlan(CachedPlan *plan, bool useResOwner);

#endif   /* PLANCACHE_H */
