/*-------------------------------------------------------------------------
 *
 * executor.h
 *	  support for the POSTGRES executor module
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/executor.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "executor/execdesc.h"
#include "nodes/parsenodes.h"


/*
 * The "eflags" argument to ExecutorStart and the various ExecInitNode
 * routines is a bitwise OR of the following flag bits, which tell the
 * called plan node what to expect.  Note that the flags will get modified
 * as they are passed down the plan tree, since an upper node may require
 * functionality in its subnode not demanded of the plan as a whole
 * (example: MergeJoin requires mark/restore capability in its inner input),
 * or an upper node may shield its input from some functionality requirement
 * (example: Materialize shields its input from needing to do backward scan).
 *
 * EXPLAIN_ONLY indicates that the plan tree is being initialized just so
 * EXPLAIN can print it out; it will not be run.  Hence, no side-effects
 * of startup should occur.  However, error checks (such as permission checks)
 * should be performed.
 *
 * REWIND indicates that the plan node should try to efficiently support
 * rescans without parameter changes.  (Nodes must support ExecReScan calls
 * in any case, but if this flag was not given, they are at liberty to do it
 * through complete recalculation.  Note that a parameter change forces a
 * full recalculation in any case.)
 *
 * BACKWARD indicates that the plan node must respect the es_direction flag.
 * When this is not passed, the plan node will only be run forwards.
 *
 * MARK indicates that the plan node must support Mark/Restore calls.
 * When this is not passed, no Mark/Restore will occur.
 *
 * SKIP_TRIGGERS tells ExecutorStart/ExecutorFinish to skip calling
 * AfterTriggerBeginQuery/AfterTriggerEndQuery.  This does not necessarily
 * mean that the plan can't queue any AFTER triggers; just that the caller
 * is responsible for there being a trigger context for them to be queued in.
 *
 * WITH/WITHOUT_OIDS tell the executor to emit tuples with or without space
 * for OIDs, respectively.  These are currently used only for CREATE TABLE AS.
 * If neither is set, the plan may or may not produce tuples including OIDs.
 */
#define EXEC_FLAG_EXPLAIN_ONLY	0x0001	/* EXPLAIN, no ANALYZE */
#define EXEC_FLAG_REWIND		0x0002	/* need efficient rescan */
#define EXEC_FLAG_BACKWARD		0x0004	/* need backward scan */
#define EXEC_FLAG_MARK			0x0008	/* need mark/restore */
#define EXEC_FLAG_SKIP_TRIGGERS 0x0010	/* skip AfterTrigger calls */
#define EXEC_FLAG_WITH_OIDS		0x0020	/* force OIDs in returned tuples */
#define EXEC_FLAG_WITHOUT_OIDS	0x0040	/* force no OIDs in returned tuples */
#define EXEC_FLAG_WITH_NO_DATA	0x0080	/* rel scannability doesn't matter */


/*
 * ExecEvalExpr was formerly a function containing a switch statement;
 * now it's just a macro invoking the function pointed to by an ExprState
 * node.  Beware of double evaluation of the ExprState argument!
 */
#define ExecEvalExpr(expr, econtext, isNull, isDone) \
	((*(expr)->evalfunc) (expr, econtext, isNull, isDone))


/* Hook for plugins to get control in ExecutorStart() */
typedef void (*ExecutorStart_hook_type) (QueryDesc *queryDesc, int eflags);
extern PGDLLIMPORT ExecutorStart_hook_type ExecutorStart_hook;

/* Hook for plugins to get control in ExecutorRun() */
typedef void (*ExecutorRun_hook_type) (QueryDesc *queryDesc,
												   ScanDirection direction,
												   long count);
extern PGDLLIMPORT ExecutorRun_hook_type ExecutorRun_hook;

/* Hook for plugins to get control in ExecutorFinish() */
typedef void (*ExecutorFinish_hook_type) (QueryDesc *queryDesc);
extern PGDLLIMPORT ExecutorFinish_hook_type ExecutorFinish_hook;

/* Hook for plugins to get control in ExecutorEnd() */
typedef void (*ExecutorEnd_hook_type) (QueryDesc *queryDesc);
extern PGDLLIMPORT ExecutorEnd_hook_type ExecutorEnd_hook;

/* Hook for plugins to get control in ExecCheckRTPerms() */
typedef bool (*ExecutorCheckPerms_hook_type) (List *, bool);
extern PGDLLIMPORT ExecutorCheckPerms_hook_type ExecutorCheckPerms_hook;


/*
 * prototypes from functions in execAmi.c
 */
struct Path;					/* avoid including relation.h here */

extern void ExecReScan(PlanState *node);
extern void ExecMarkPos(PlanState *node);
extern void ExecRestrPos(PlanState *node);
extern bool ExecSupportsMarkRestore(struct Path *pathnode);
extern bool ExecSupportsBackwardScan(Plan *node);
extern bool ExecMaterializesOutput(NodeTag plantype);

/*
 * prototypes from functions in execCurrent.c
 */
extern bool execCurrentOf(CurrentOfExpr *cexpr,
			  ExprContext *econtext,
			  Oid table_oid,
			  ItemPointer current_tid);

/*
 * prototypes from functions in execGrouping.c
 */
extern bool execTuplesMatch(TupleTableSlot *slot1,
				TupleTableSlot *slot2,
				int numCols,
				AttrNumber *matchColIdx,
				FmgrInfo *eqfunctions,
				MemoryContext evalContext);
extern bool execTuplesUnequal(TupleTableSlot *slot1,
				  TupleTableSlot *slot2,
				  int numCols,
				  AttrNumber *matchColIdx,
				  FmgrInfo *eqfunctions,
				  MemoryContext evalContext);
extern FmgrInfo *execTuplesMatchPrepare(int numCols,
					   Oid *eqOperators);
extern void execTuplesHashPrepare(int numCols,
					  Oid *eqOperators,
					  FmgrInfo **eqFunctions,
					  FmgrInfo **hashFunctions);
extern TupleHashTable BuildTupleHashTable(int numCols, AttrNumber *keyColIdx,
					FmgrInfo *eqfunctions,
					FmgrInfo *hashfunctions,
					long nbuckets, Size entrysize,
					MemoryContext tablecxt,
					MemoryContext tempcxt);
extern TupleHashEntry LookupTupleHashEntry(TupleHashTable hashtable,
					 TupleTableSlot *slot,
					 bool *isnew);
extern TupleHashEntry FindTupleHashEntry(TupleHashTable hashtable,
				   TupleTableSlot *slot,
				   FmgrInfo *eqfunctions,
				   FmgrInfo *hashfunctions);

/*
 * prototypes from functions in execJunk.c
 */
extern JunkFilter *ExecInitJunkFilter(List *targetList, bool hasoid,
				   TupleTableSlot *slot);
extern JunkFilter *ExecInitJunkFilterConversion(List *targetList,
							 TupleDesc cleanTupType,
							 TupleTableSlot *slot);
extern AttrNumber ExecFindJunkAttribute(JunkFilter *junkfilter,
					  const char *attrName);
extern AttrNumber ExecFindJunkAttributeInTlist(List *targetlist,
							 const char *attrName);
extern Datum ExecGetJunkAttribute(TupleTableSlot *slot, AttrNumber attno,
					 bool *isNull);
extern TupleTableSlot *ExecFilterJunk(JunkFilter *junkfilter,
			   TupleTableSlot *slot);


/*
 * prototypes from functions in execMain.c
 */
extern void ExecutorStart(QueryDesc *queryDesc, int eflags);
extern void standard_ExecutorStart(QueryDesc *queryDesc, int eflags);
extern void ExecutorRun(QueryDesc *queryDesc,
			ScanDirection direction, long count);
extern void standard_ExecutorRun(QueryDesc *queryDesc,
					 ScanDirection direction, long count);
extern void ExecutorFinish(QueryDesc *queryDesc);
extern void standard_ExecutorFinish(QueryDesc *queryDesc);
extern void ExecutorEnd(QueryDesc *queryDesc);
extern void standard_ExecutorEnd(QueryDesc *queryDesc);
extern void ExecutorRewind(QueryDesc *queryDesc);
extern bool ExecCheckRTPerms(List *rangeTable, bool ereport_on_violation);
extern void CheckValidResultRel(Relation resultRel, CmdType operation);
extern void InitResultRelInfo(ResultRelInfo *resultRelInfo,
				  Relation resultRelationDesc,
				  Index resultRelationIndex,
				  int instrument_options);
extern ResultRelInfo *ExecGetTriggerResultRel(EState *estate, Oid relid);
extern bool ExecContextForcesOids(PlanState *planstate, bool *hasoids);
extern void ExecConstraints(ResultRelInfo *resultRelInfo,
				TupleTableSlot *slot, EState *estate);
extern void ExecWithCheckOptions(WCOKind kind, ResultRelInfo *resultRelInfo,
					 TupleTableSlot *slot, EState *estate);
extern LockTupleMode ExecUpdateLockMode(EState *estate, ResultRelInfo *relinfo);
extern ExecRowMark *ExecFindRowMark(EState *estate, Index rti, bool missing_ok);
extern ExecAuxRowMark *ExecBuildAuxRowMark(ExecRowMark *erm, List *targetlist);
extern TupleTableSlot *EvalPlanQual(EState *estate, EPQState *epqstate,
			 Relation relation, Index rti, int lockmode,
			 ItemPointer tid, TransactionId priorXmax);
extern HeapTuple EvalPlanQualFetch(EState *estate, Relation relation,
				  int lockmode, LockWaitPolicy wait_policy, ItemPointer tid,
				  TransactionId priorXmax);
extern void EvalPlanQualInit(EPQState *epqstate, EState *estate,
				 Plan *subplan, List *auxrowmarks, int epqParam);
extern void EvalPlanQualSetPlan(EPQState *epqstate,
					Plan *subplan, List *auxrowmarks);
extern void EvalPlanQualSetTuple(EPQState *epqstate, Index rti,
					 HeapTuple tuple);
extern HeapTuple EvalPlanQualGetTuple(EPQState *epqstate, Index rti);

#define EvalPlanQualSetSlot(epqstate, slot)  ((epqstate)->origslot = (slot))
extern void EvalPlanQualFetchRowMarks(EPQState *epqstate);
extern TupleTableSlot *EvalPlanQualNext(EPQState *epqstate);
extern void EvalPlanQualBegin(EPQState *epqstate, EState *parentestate);
extern void EvalPlanQualEnd(EPQState *epqstate);

/*
 * prototypes from functions in execProcnode.c
 */
extern PlanState *ExecInitNode(Plan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecProcNode(PlanState *node);
extern Node *MultiExecProcNode(PlanState *node);
extern void ExecEndNode(PlanState *node);

/*
 * prototypes from functions in execQual.c
 */
extern Datum GetAttributeByNum(HeapTupleHeader tuple, AttrNumber attrno,
				  bool *isNull);
extern Datum GetAttributeByName(HeapTupleHeader tuple, const char *attname,
				   bool *isNull);
extern Tuplestorestate *ExecMakeTableFunctionResult(ExprState *funcexpr,
							ExprContext *econtext,
							MemoryContext argContext,
							TupleDesc expectedDesc,
							bool randomAccess);
extern Datum ExecEvalExprSwitchContext(ExprState *expression, ExprContext *econtext,
						  bool *isNull, ExprDoneCond *isDone);
extern ExprState *ExecInitExpr(Expr *node, PlanState *parent);
extern ExprState *ExecPrepareExpr(Expr *node, EState *estate);
extern bool ExecQual(List *qual, ExprContext *econtext, bool resultForNull);
extern int	ExecTargetListLength(List *targetlist);
extern int	ExecCleanTargetListLength(List *targetlist);
extern TupleTableSlot *ExecProject(ProjectionInfo *projInfo,
			ExprDoneCond *isDone);

/*
 * prototypes from functions in execScan.c
 */
typedef TupleTableSlot *(*ExecScanAccessMtd) (ScanState *node);
typedef bool (*ExecScanRecheckMtd) (ScanState *node, TupleTableSlot *slot);

extern TupleTableSlot *ExecScan(ScanState *node, ExecScanAccessMtd accessMtd,
		 ExecScanRecheckMtd recheckMtd);
extern void ExecAssignScanProjectionInfo(ScanState *node);
extern void ExecAssignScanProjectionInfoWithVarno(ScanState *node, Index varno);
extern void ExecScanReScan(ScanState *node);

/*
 * prototypes from functions in execTuples.c
 */
extern void ExecInitResultTupleSlot(EState *estate, PlanState *planstate);
extern void ExecInitScanTupleSlot(EState *estate, ScanState *scanstate);
extern TupleTableSlot *ExecInitExtraTupleSlot(EState *estate);
extern TupleTableSlot *ExecInitNullTupleSlot(EState *estate,
					  TupleDesc tupType);
extern TupleDesc ExecTypeFromTL(List *targetList, bool hasoid);
extern TupleDesc ExecCleanTypeFromTL(List *targetList, bool hasoid);
extern TupleDesc ExecTypeFromExprList(List *exprList);
extern void ExecTypeSetColNames(TupleDesc typeInfo, List *namesList);
extern void UpdateChangedParamSet(PlanState *node, Bitmapset *newchg);

typedef struct TupOutputState
{
	TupleTableSlot *slot;
	DestReceiver *dest;
} TupOutputState;

extern TupOutputState *begin_tup_output_tupdesc(DestReceiver *dest,
						 TupleDesc tupdesc);
extern void do_tup_output(TupOutputState *tstate, Datum *values, bool *isnull);
extern void do_text_output_multiline(TupOutputState *tstate, char *text);
extern void end_tup_output(TupOutputState *tstate);

/*
 * Write a single line of text given as a C string.
 *
 * Should only be used with a single-TEXT-attribute tupdesc.
 */
#define do_text_output_oneline(tstate, str_to_emit) \
	do { \
		Datum	values_[1]; \
		bool	isnull_[1]; \
		values_[0] = PointerGetDatum(cstring_to_text(str_to_emit)); \
		isnull_[0] = false; \
		do_tup_output(tstate, values_, isnull_); \
		pfree(DatumGetPointer(values_[0])); \
	} while (0)


/*
 * prototypes from functions in execUtils.c
 */
extern EState *CreateExecutorState(void);
extern void FreeExecutorState(EState *estate);
extern ExprContext *CreateExprContext(EState *estate);
extern ExprContext *CreateStandaloneExprContext(void);
extern void FreeExprContext(ExprContext *econtext, bool isCommit);
extern void ReScanExprContext(ExprContext *econtext);

#define ResetExprContext(econtext) \
	MemoryContextReset((econtext)->ecxt_per_tuple_memory)

extern ExprContext *MakePerTupleExprContext(EState *estate);

/* Get an EState's per-output-tuple exprcontext, making it if first use */
#define GetPerTupleExprContext(estate) \
	((estate)->es_per_tuple_exprcontext ? \
	 (estate)->es_per_tuple_exprcontext : \
	 MakePerTupleExprContext(estate))

#define GetPerTupleMemoryContext(estate) \
	(GetPerTupleExprContext(estate)->ecxt_per_tuple_memory)

/* Reset an EState's per-output-tuple exprcontext, if one's been created */
#define ResetPerTupleExprContext(estate) \
	do { \
		if ((estate)->es_per_tuple_exprcontext) \
			ResetExprContext((estate)->es_per_tuple_exprcontext); \
	} while (0)

extern void ExecAssignExprContext(EState *estate, PlanState *planstate);
extern void ExecAssignResultType(PlanState *planstate, TupleDesc tupDesc);
extern void ExecAssignResultTypeFromTL(PlanState *planstate);
extern TupleDesc ExecGetResultType(PlanState *planstate);
extern ProjectionInfo *ExecBuildProjectionInfo(List *targetList,
						ExprContext *econtext,
						TupleTableSlot *slot,
						TupleDesc inputDesc);
extern void ExecAssignProjectionInfo(PlanState *planstate,
						 TupleDesc inputDesc);
extern void ExecFreeExprContext(PlanState *planstate);
extern TupleDesc ExecGetScanType(ScanState *scanstate);
extern void ExecAssignScanType(ScanState *scanstate, TupleDesc tupDesc);
extern void ExecAssignScanTypeFromOuterPlan(ScanState *scanstate);

extern bool ExecRelationIsTargetRelation(EState *estate, Index scanrelid);

extern Relation ExecOpenScanRelation(EState *estate, Index scanrelid, int eflags);
extern void ExecCloseScanRelation(Relation scanrel);

extern void RegisterExprContextCallback(ExprContext *econtext,
							ExprContextCallbackFunction function,
							Datum arg);
extern void UnregisterExprContextCallback(ExprContext *econtext,
							  ExprContextCallbackFunction function,
							  Datum arg);

/*
 * prototypes from functions in execIndexing.c
 */
extern void ExecOpenIndices(ResultRelInfo *resultRelInfo, bool speculative);
extern void ExecCloseIndices(ResultRelInfo *resultRelInfo);
extern List *ExecInsertIndexTuples(TupleTableSlot *slot, ItemPointer tupleid,
					  EState *estate, bool noDupErr, bool *specConflict,
					  List *arbiterIndexes);
extern bool ExecCheckIndexConstraints(TupleTableSlot *slot, EState *estate,
						  ItemPointer conflictTid, List *arbiterIndexes);
extern void check_exclusion_constraint(Relation heap, Relation index,
						   IndexInfo *indexInfo,
						   ItemPointer tupleid,
						   Datum *values, bool *isnull,
						   EState *estate, bool newIndex);


#endif   /* EXECUTOR_H  */
