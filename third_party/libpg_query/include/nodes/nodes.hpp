/*-------------------------------------------------------------------------
 *
 * nodes.h
 *	  Definitions for tagged nodes.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/nodes.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include "pg_definitions.hpp"

namespace duckdb_libpgquery {

/*
 * The first field of every node is NodeTag. Each node created (with makeNode)
 * will have one of the following tags as the value of its first field.
 *
 * Note that inserting or deleting node types changes the numbers of other
 * node types later in the list.  This is no problem during development, since
 * the node numbers are never stored on disk.  But don't do it in a released
 * branch, because that would represent an ABI break for extensions.
 */
typedef enum PGNodeTag {
	T_PGInvalid = 0,

	/*
	 * TAGS FOR EXECUTOR NODES (execnodes.h)
	 */
	T_PGIndexInfo,
	T_PGExprContext,
	T_PGProjectionInfo,
	T_PGJunkFilter,
	T_PGResultRelInfo,
	T_PGEState,
	T_PGTupleTableSlot,

	/*
	 * TAGS FOR PLAN NODES (plannodes.h)
	 */
	T_PGPlan,
	T_PGResult,
	T_PGProjectSet,
	T_PGModifyTable,
	T_PGAppend,
	T_PGMergeAppend,
	T_PGRecursiveUnion,
	T_PGBitmapAnd,
	T_PGBitmapOr,
	T_PGScan,
	T_PGSeqScan,
	T_PGSampleScan,
	T_PGIndexScan,
	T_PGIndexOnlyScan,
	T_PGBitmapIndexScan,
	T_PGBitmapHeapScan,
	T_PGTidScan,
	T_PGSubqueryScan,
	T_PGFunctionScan,
	T_PGValuesScan,
	T_PGTableFuncScan,
	T_PGCteScan,
	T_PGNamedTuplestoreScan,
	T_PGWorkTableScan,
	T_PGForeignScan,
	T_PGCustomScan,
	T_PGJoin,
	T_PGNestLoop,
	T_PGMergeJoin,
	T_PGHashJoin,
	T_PGMaterial,
	T_PGSort,
	T_PGGroup,
	T_PGAgg,
	T_PGWindowAgg,
	T_PGUnique,
	T_PGGather,
	T_PGGatherMerge,
	T_PGHash,
	T_PGSetOp,
	T_PGLockRows,
	T_PGLimit,
	/* these aren't subclasses of PGPlan: */
	T_PGNestLoopParam,
	T_PGPlanRowMark,
	T_PGPlanInvalItem,

	/*
	 * TAGS FOR PLAN STATE NODES (execnodes.h)
	 *
	 * These should correspond one-to-one with PGPlan node types.
	 */
	T_PGPlanState,
	T_PGResultState,
	T_PGProjectSetState,
	T_PGModifyTableState,
	T_PGAppendState,
	T_PGMergeAppendState,
	T_PGRecursiveUnionState,
	T_PGBitmapAndState,
	T_PGBitmapOrState,
	T_PGScanState,
	T_PGSeqScanState,
	T_PGSampleScanState,
	T_PGIndexScanState,
	T_PGIndexOnlyScanState,
	T_PGBitmapIndexScanState,
	T_PGBitmapHeapScanState,
	T_PGTidScanState,
	T_PGSubqueryScanState,
	T_PGFunctionScanState,
	T_PGTableFuncScanState,
	T_PGValuesScanState,
	T_PGCteScanState,
	T_PGNamedTuplestoreScanState,
	T_PGWorkTableScanState,
	T_PGForeignScanState,
	T_PGCustomScanState,
	T_PGJoinState,
	T_PGNestLoopState,
	T_PGMergeJoinState,
	T_PGHashJoinState,
	T_PGMaterialState,
	T_PGSortState,
	T_PGGroupState,
	T_PGAggState,
	T_PGWindowAggState,
	T_PGUniqueState,
	T_PGGatherState,
	T_PGGatherMergeState,
	T_PGHashState,
	T_PGSetOpState,
	T_PGLockRowsState,
	T_PGLimitState,

	/*
	 * TAGS FOR PRIMITIVE NODES (primnodes.h)
	 */
	T_PGAlias,
	T_PGRangeVar,
	T_PGTableFunc,
	T_PGExpr,
	T_PGVar,
	T_PGConst,
	T_PGParam,
	T_PGAggref,
	T_PGGroupingFunc,
	T_PGWindowFunc,
	T_PGArrayRef,
	T_PGFuncExpr,
	T_PGNamedArgExpr,
	T_PGOpExpr,
	T_PGDistinctExpr,
	T_PGNullIfExpr,
	T_PGScalarArrayOpExpr,
	T_PGBoolExpr,
	T_PGSubLink,
	T_PGSubPlan,
	T_PGAlternativeSubPlan,
	T_PGFieldSelect,
	T_PGFieldStore,
	T_PGRelabelType,
	T_PGCoerceViaIO,
	T_PGArrayCoerceExpr,
	T_PGConvertRowtypeExpr,
	T_PGCollateExpr,
	T_PGCaseExpr,
	T_PGCaseWhen,
	T_PGCaseTestExpr,
	T_PGArrayExpr,
	T_PGRowExpr,
	T_PGRowCompareExpr,
	T_PGCoalesceExpr,
	T_PGMinMaxExpr,
	T_PGSQLValueFunction,
	T_PGXmlExpr,
	T_PGNullTest,
	T_PGBooleanTest,
	T_PGCoerceToDomain,
	T_PGCoerceToDomainValue,
	T_PGSetToDefault,
	T_PGCurrentOfExpr,
	T_PGNextValueExpr,
	T_PGInferenceElem,
	T_PGTargetEntry,
	T_PGRangeTblRef,
	T_PGJoinExpr,
	T_PGFromExpr,
	T_PGOnConflictExpr,
	T_PGIntoClause,
	T_PGLambdaFunction,

	/*
	 * TAGS FOR EXPRESSION STATE NODES (execnodes.h)
	 *
	 * ExprState represents the evaluation state for a whole expression tree.
	 * Most Expr-based plan nodes do not have a corresponding expression state
	 * node, they're fully handled within execExpr* - but sometimes the state
	 * needs to be shared with other parts of the executor, as for example
	 * with AggrefExprState, which nodeAgg.c has to modify.
	 */
	T_PGExprState,
	T_PGAggrefExprState,
	T_PGWindowFuncExprState,
	T_PGSetExprState,
	T_PGSubPlanState,
	T_PGAlternativeSubPlanState,
	T_PGDomainConstraintState,

	/*
	 * TAGS FOR PLANNER NODES (relation.h)
	 */
	T_PGPlannerInfo,
	T_PGPlannerGlobal,
	T_PGRelOptInfo,
	T_PGIndexOptInfo,
	T_PGForeignKeyOptInfo,
	T_PGParamPathInfo,
	T_PGPath,
	T_PGIndexPath,
	T_PGBitmapHeapPath,
	T_PGBitmapAndPath,
	T_PGBitmapOrPath,
	T_PGTidPath,
	T_PGSubqueryScanPath,
	T_PGForeignPath,
	T_PGCustomPath,
	T_PGNestPath,
	T_PGMergePath,
	T_PGHashPath,
	T_PGAppendPath,
	T_PGMergeAppendPath,
	T_PGResultPath,
	T_PGMaterialPath,
	T_PGUniquePath,
	T_PGGatherPath,
	T_PGGatherMergePath,
	T_PGProjectionPath,
	T_PGProjectSetPath,
	T_PGSortPath,
	T_PGGroupPath,
	T_PGUpperUniquePath,
	T_PGAggPath,
	T_PGGroupingSetsPath,
	T_PGMinMaxAggPath,
	T_PGWindowAggPath,
	T_PGSetOpPath,
	T_PGRecursiveUnionPath,
	T_PGLockRowsPath,
	T_PGModifyTablePath,
	T_PGLimitPath,
	/* these aren't subclasses of Path: */
	T_PGEquivalenceClass,
	T_PGEquivalenceMember,
	T_PGPathKey,
	T_PGPathTarget,
	T_PGRestrictInfo,
	T_PGPlaceHolderVar,
	T_PGSpecialJoinInfo,
	T_PGAppendRelInfo,
	T_PGPartitionedChildRelInfo,
	T_PGPlaceHolderInfo,
	T_PGMinMaxAggInfo,
	T_PGPlannerParamItem,
	T_PGRollupData,
	T_PGGroupingSetData,
	T_PGStatisticExtInfo,

	/*
	 * TAGS FOR MEMORY NODES (memnodes.h)
	 */
	T_PGMemoryContext,
	T_PGAllocSetContext,
	T_PGSlabContext,

	/*
	 * TAGS FOR VALUE NODES (value.h)
	 */
	T_PGValue,
	T_PGInteger,
	T_PGFloat,
	T_PGString,
	T_PGBitString,
	T_PGNull,

	/*
	 * TAGS FOR LIST NODES (pg_list.h)
	 */
	T_PGList,
	T_PGIntList,
	T_PGOidList,

	/*
	 * TAGS FOR EXTENSIBLE NODES (extensible.h)
	 */
	T_PGExtensibleNode,

	/*
	 * TAGS FOR STATEMENT NODES (mostly in parsenodes.h)
	 */
	T_PGRawStmt,
	T_PGQuery,
	T_PGPlannedStmt,
	T_PGInsertStmt,
	T_PGDeleteStmt,
	T_PGUpdateStmt,
	T_PGSelectStmt,
	T_PGAlterTableStmt,
	T_PGAlterTableCmd,
	T_PGAlterDomainStmt,
	T_PGSetOperationStmt,
	T_PGGrantStmt,
	T_PGGrantRoleStmt,
	T_PGAlterDefaultPrivilegesStmt,
	T_PGClosePortalStmt,
	T_PGClusterStmt,
	T_PGCopyStmt,
	T_PGCreateStmt,
	T_PGDefineStmt,
	T_PGDropStmt,
	T_PGTruncateStmt,
	T_PGCommentStmt,
	T_PGFetchStmt,
	T_PGIndexStmt,
	T_PGCreateFunctionStmt,
    T_PGCreateMatViewStmt,
	T_PGAlterFunctionStmt,
	T_PGDoStmt,
	T_PGRenameStmt,
	T_PGRuleStmt,
	T_PGNotifyStmt,
	T_PGListenStmt,
	T_PGUnlistenStmt,
	T_PGTransactionStmt,
	T_PGViewStmt,
	T_PGLoadStmt,
	T_PGCreateDomainStmt,
	T_PGCreatedbStmt,
	T_PGDropdbStmt,
	T_PGVacuumStmt,
	T_PGExplainStmt,
	T_PGCreateTableAsStmt,
	T_PGCreateSeqStmt,
	T_PGAlterSeqStmt,
	T_PGVariableSetStmt,
	T_PGVariableShowStmt,
	T_PGVariableShowSelectStmt,
	T_PGDiscardStmt,
	T_PGCreateTrigStmt,
	T_PGCreatePLangStmt,
	T_PGCreateRoleStmt,
	T_PGAlterRoleStmt,
	T_PGDropRoleStmt,
	T_PGLockStmt,
	T_PGConstraintsSetStmt,
	T_PGReindexStmt,
	T_PGCheckPointStmt,
	T_PGCreateSchemaStmt,
	T_PGAlterDatabaseStmt,
	T_PGAlterDatabaseSetStmt,
	T_PGAlterRoleSetStmt,
	T_PGCreateConversionStmt,
	T_PGCreateCastStmt,
	T_PGCreateOpClassStmt,
	T_PGCreateOpFamilyStmt,
	T_PGAlterOpFamilyStmt,
	T_PGPrepareStmt,
	T_PGExecuteStmt,
	T_PGCallStmt,
	T_PGDeallocateStmt,
	T_PGDeclareCursorStmt,
	T_PGCreateTableSpaceStmt,
	T_PGDropTableSpaceStmt,
	T_PGAlterObjectDependsStmt,
	T_PGAlterObjectSchemaStmt,
	T_PGAlterOwnerStmt,
	T_PGAlterOperatorStmt,
	T_PGDropOwnedStmt,
	T_PGReassignOwnedStmt,
	T_PGCompositeTypeStmt,
	T_PGCreateEnumStmt,
	T_PGCreateRangeStmt,
	T_PGAlterEnumStmt,
	T_PGAlterTSDictionaryStmt,
	T_PGAlterTSConfigurationStmt,
	T_PGCreateFdwStmt,
	T_PGAlterFdwStmt,
	T_PGCreateForeignServerStmt,
	T_PGAlterForeignServerStmt,
	T_PGCreateUserMappingStmt,
	T_PGAlterUserMappingStmt,
	T_PGDropUserMappingStmt,
	T_PGAlterTableSpaceOptionsStmt,
	T_PGAlterTableMoveAllStmt,
	T_PGSecLabelStmt,
	T_PGCreateForeignTableStmt,
	T_PGImportForeignSchemaStmt,
	T_PGCreateExtensionStmt,
	T_PGAlterExtensionStmt,
	T_PGAlterExtensionContentsStmt,
	T_PGCreateEventTrigStmt,
	T_PGAlterEventTrigStmt,
	T_PGRefreshMatViewStmt,
	T_PGReplicaIdentityStmt,
	T_PGAlterSystemStmt,
	T_PGCreatePolicyStmt,
	T_PGAlterPolicyStmt,
	T_PGCreateTransformStmt,
	T_PGCreateAmStmt,
	T_PGCreatePublicationStmt,
	T_PGAlterPublicationStmt,
	T_PGCreateSubscriptionStmt,
	T_PGAlterSubscriptionStmt,
	T_PGDropSubscriptionStmt,
	T_PGCreateStatsStmt,
	T_PGAlterCollationStmt,
	T_PGPragmaStmt,
	T_PGExportStmt,
	T_PGImportStmt,

	/*
	 * TAGS FOR PARSE TREE NODES (parsenodes.h)
	 */
	T_PGAExpr,
	T_PGColumnRef,
	T_PGParamRef,
	T_PGAConst,
	T_PGFuncCall,
	T_PGAStar,
	T_PGAIndices,
	T_PGAIndirection,
	T_PGAArrayExpr,
	T_PGResTarget,
	T_PGMultiAssignRef,
	T_PGTypeCast,
	T_PGCollateClause,
	T_PGSortBy,
	T_PGWindowDef,
	T_PGRangeSubselect,
	T_PGRangeFunction,
	T_PGRangeTableSample,
	T_PGRangeTableFunc,
	T_PGRangeTableFuncCol,
	T_PGTypeName,
	T_PGColumnDef,
	T_PGIndexElem,
	T_PGConstraint,
	T_PGDefElem,
	T_PGRangeTblEntry,
	T_PGRangeTblFunction,
	T_PGTableSampleClause,
	T_PGWithCheckOption,
	T_PGSortGroupClause,
	T_PGGroupingSet,
	T_PGWindowClause,
	T_PGObjectWithArgs,
	T_PGAccessPriv,
	T_PGCreateOpClassItem,
	T_PGTableLikeClause,
	T_PGFunctionParameter,
	T_PGLockingClause,
	T_PGRowMarkClause,
	T_PGXmlSerialize,
	T_PGWithClause,
	T_PGInferClause,
	T_PGOnConflictClause,
	T_PGCommonTableExpr,
	T_PGRoleSpec,
	T_PGTriggerTransition,
	T_PGPartitionElem,
	T_PGPartitionSpec,
	T_PGPartitionBoundSpec,
	T_PGPartitionRangeDatum,
	T_PGPartitionCmd,
	T_PGIntervalConstant,
	T_PGSampleSize,
	T_PGSampleOptions,
	T_PGLimitPercent,
	T_PGPositionalReference,

	/*
	 * TAGS FOR REPLICATION GRAMMAR PARSE NODES (replnodes.h)
	 */
	T_PGIdentifySystemCmd,
	T_PGBaseBackupCmd,
	T_PGCreateReplicationSlotCmd,
	T_PGDropReplicationSlotCmd,
	T_PGStartReplicationCmd,
	T_PGTimeLineHistoryCmd,
	T_PGSQLCmd,

	/*
	 * TAGS FOR RANDOM OTHER STUFF
	 *
	 * These are objects that aren't part of parse/plan/execute node tree
	 * structures, but we give them NodeTags anyway for identification
	 * purposes (usually because they are involved in APIs where we want to
	 * pass multiple object types through the same pointer).
	 */
	T_PGTriggerData,        /* in commands/trigger.h */
	T_PGEventTriggerData,   /* in commands/event_trigger.h */
	T_PGReturnSetInfo,      /* in nodes/execnodes.h */
	T_PGWindowObjectData,   /* private in nodeWindowAgg.c */
	T_PGTIDBitmap,          /* in nodes/tidbitmap.h */
	T_PGInlineCodeBlock,    /* in nodes/parsenodes.h */
	T_PGFdwRoutine,         /* in foreign/fdwapi.h */
	T_PGIndexAmRoutine,     /* in access/amapi.h */
	T_PGTsmRoutine,         /* in access/tsmapi.h */
	T_PGForeignKeyCacheInfo /* in utils/rel.h */
} PGNodeTag;

/*
 * The first field of a node of any type is guaranteed to be the NodeTag.
 * Hence the type of any node can be gotten by casting it to Node. Declaring
 * a variable to be of PGNode * (instead of void *) can also facilitate
 * debugging.
 */
typedef struct PGNode {
	PGNodeTag type;
} PGNode;

#define nodeTag(nodeptr) (((const PGNode *)(nodeptr))->type)

#define makeNode(_type_) ((_type_ *)newNode(sizeof(_type_), T_##_type_))

#define NodeSetTag(nodeptr, t) (((PGNode *)(nodeptr))->type = (t))

#define IsA(nodeptr, _type_) (nodeTag(nodeptr) == T_##_type_)

/*
 * castNode(type, ptr) casts ptr to "type *", and if assertions are enabled,
 * verifies that the node has the appropriate type (using its nodeTag()).
 *
 * Use an inline function when assertions are enabled, to avoid multiple
 * evaluations of the ptr argument (which could e.g. be a function call).
 */
#ifdef USE_ASSERT_CHECKING
static inline PGNode *castNodeImpl(PGNodeTag type, void *ptr) {
	Assert(ptr == NULL || nodeTag(ptr) == type);
	return (PGNode *)ptr;
}
#define castNode(_type_, nodeptr) ((_type_ *)castNodeImpl(T_##_type_, nodeptr))
#else
#define castNode(_type_, nodeptr) ((_type_ *)(nodeptr))
#endif /* USE_ASSERT_CHECKING */

/* ----------------------------------------------------------------
 *					  extern declarations follow
 * ----------------------------------------------------------------
 */

/*
 * nodes/{outfuncs.c,print.c}
 */
struct PGBitmapset;      /* not to include bitmapset.h here */
struct PGStringInfoData; /* not to include stringinfo.h here */

PGNode* newNode(size_t size, PGNodeTag type);

void outNode(struct PGStringInfoData *str, const void *obj);
void outToken(struct PGStringInfoData *str, const char *s);
void outBitmapset(struct PGStringInfoData *str, const struct PGBitmapset *bms);
void outDatum(struct PGStringInfoData *str, uintptr_t value, int typlen, bool typbyval);
char *nodeToString(const void *obj);
char *bmsToString(const struct PGBitmapset *bms);

/*
 * nodes/{readfuncs.c,read.c}
 */
void *stringToNode(char *str);
struct PGBitmapset *readBitmapset(void);
uintptr_t readDatum(bool typbyval);
bool *readBoolCols(int numCols);
int *readIntCols(int numCols);
PGOid *readOidCols(int numCols);
int16_t *readAttrNumberCols(int numCols);

/*
 * nodes/copyfuncs.c
 */
void *copyObjectImpl(const void *obj);

/* cast result back to argument type, if supported by compiler */
//#ifdef HAVE_TYPEOF
//#define copyObject(obj) ((typeof(obj)) copyObjectImpl(obj))
//#else
//#define copyObject(obj) copyObjectImpl(obj)
//#endif

/*
 * nodes/equalfuncs.c
 */
// extern bool equal(const void *a, const void *b);

/*
 * Typedefs for identifying qualifier selectivities and plan costs as such.
 * These are just plain "double"s, but declaring a variable as Selectivity
 * or Cost makes the intent more obvious.
 *
 * These could have gone into plannodes.h or some such, but many files
 * depend on them...
 */
typedef double Selectivity; /* fraction of tuples a qualifier will pass */
typedef double Cost;        /* execution cost (in page-access units) */

/*
 * PGCmdType -
 *	  enums for type of operation represented by a PGQuery or PGPlannedStmt
 *
 * This is needed in both parsenodes.h and plannodes.h, so put it here...
 */
typedef enum PGCmdType {
	PG_CMD_UNKNOWN,
	PG_CMD_SELECT, /* select stmt */
	PG_CMD_UPDATE, /* update stmt */
	PG_CMD_INSERT, /* insert stmt */
	PG_CMD_DELETE,
	PG_CMD_UTILITY, /* cmds like create, destroy, copy, vacuum,
								 * etc. */
	PG_CMD_NOTHING  /* dummy command for instead nothing rules
								 * with qual */
} PGCmdType;

/*
 * PGJoinType -
 *	  enums for types of relation joins
 *
 * PGJoinType determines the exact semantics of joining two relations using
 * a matching qualification.  For example, it tells what to do with a tuple
 * that has no match in the other relation.
 *
 * This is needed in both parsenodes.h and plannodes.h, so put it here...
 */
typedef enum PGJoinType {
	/*
	 * The canonical kinds of joins according to the SQL JOIN syntax. Only
	 * these codes can appear in parser output (e.g., PGJoinExpr nodes).
	 */
	PG_JOIN_INNER, /* matching tuple pairs only */
	PG_JOIN_LEFT,  /* pairs + unmatched LHS tuples */
	PG_JOIN_FULL,  /* pairs + unmatched LHS + unmatched RHS */
	PG_JOIN_RIGHT, /* pairs + unmatched RHS tuples */

	/*
	 * Semijoins and anti-semijoins (as defined in relational theory) do not
	 * appear in the SQL JOIN syntax, but there are standard idioms for
	 * representing them (e.g., using EXISTS).  The planner recognizes these
	 * cases and converts them to joins.  So the planner and executor must
	 * support these codes.  NOTE: in PG_JOIN_SEMI output, it is unspecified
	 * which matching RHS row is joined to.  In PG_JOIN_ANTI output, the row is
	 * guaranteed to be null-extended.
	 */
	PG_JOIN_SEMI, /* 1 copy of each LHS row that has match(es) */
	PG_JOIN_ANTI, /* 1 copy of each LHS row that has no match */

	/*
	 * These codes are used internally in the planner, but are not supported
	 * by the executor (nor, indeed, by most of the planner).
	 */
	PG_JOIN_UNIQUE_OUTER, /* LHS path must be made unique */
	PG_JOIN_UNIQUE_INNER  /* RHS path must be made unique */

	/*
	 * We might need additional join types someday.
	 */
} PGJoinType;

/*
 * OUTER joins are those for which pushed-down quals must behave differently
 * from the join's own quals.  This is in fact everything except INNER and
 * SEMI joins.  However, this macro must also exclude the JOIN_UNIQUE symbols
 * since those are temporary proxies for what will eventually be an INNER
 * join.
 *
 * Note: semijoins are a hybrid case, but we choose to treat them as not
 * being outer joins.  This is okay principally because the SQL syntax makes
 * it impossible to have a pushed-down qual that refers to the inner relation
 * of a semijoin; so there is no strong need to distinguish join quals from
 * pushed-down quals.  This is convenient because for almost all purposes,
 * quals attached to a semijoin can be treated the same as innerjoin quals.
 */
#define IS_OUTER_JOIN(jointype) \
	(((1 << (jointype)) & ((1 << PG_JOIN_LEFT) | (1 << PG_JOIN_FULL) | (1 << PG_JOIN_RIGHT) | (1 << PG_JOIN_ANTI))) != 0)

/*
 * PGAggStrategy -
 *	  overall execution strategies for PGAgg plan nodes
 *
 * This is needed in both plannodes.h and relation.h, so put it here...
 */
typedef enum PGAggStrategy {
	PG_AGG_PLAIN,  /* simple agg across all input rows */
	PG_AGG_SORTED, /* grouped agg, input must be sorted */
	PG_AGG_HASHED, /* grouped agg, use internal hashtable */
	AGG_MIXED      /* grouped agg, hash and sort both used */
} PGAggStrategy;

/*
 * PGAggSplit -
 *	  splitting (partial aggregation) modes for PGAgg plan nodes
 *
 * This is needed in both plannodes.h and relation.h, so put it here...
 */

/* Primitive options supported by nodeAgg.c: */
#define AGGSPLITOP_COMBINE 0x01 /* substitute combinefn for transfn */
#define AGGSPLITOP_SKIPFINAL 0x02 /* skip finalfn, return state as-is */
#define AGGSPLITOP_SERIALIZE 0x04 /* apply serializefn to output */
#define AGGSPLITOP_DESERIALIZE 0x08 /* apply deserializefn to input */

/* Supported operating modes (i.e., useful combinations of these options): */
typedef enum PGAggSplit {
	/* Basic, non-split aggregation: */
	PG_AGGSPLIT_SIMPLE = 0,
	/* Initial phase of partial aggregation, with serialization: */
	PG_AGGSPLIT_INITIAL_SERIAL = AGGSPLITOP_SKIPFINAL | AGGSPLITOP_SERIALIZE,
	/* Final phase of partial aggregation, with deserialization: */
	PG_AGGSPLIT_FINAL_DESERIAL = AGGSPLITOP_COMBINE | AGGSPLITOP_DESERIALIZE
} PGAggSplit;

/* Test whether an PGAggSplit value selects each primitive option: */
#define DO_AGGSPLIT_COMBINE(as) (((as)&AGGSPLITOP_COMBINE) != 0)
#define DO_AGGSPLIT_SKIPFINAL(as) (((as)&AGGSPLITOP_SKIPFINAL) != 0)
#define DO_AGGSPLIT_SERIALIZE(as) (((as)&AGGSPLITOP_SERIALIZE) != 0)
#define DO_AGGSPLIT_DESERIALIZE(as) (((as)&AGGSPLITOP_DESERIALIZE) != 0)

/*
 * PGSetOpCmd and PGSetOpStrategy -
 *	  overall semantics and execution strategies for PGSetOp plan nodes
 *
 * This is needed in both plannodes.h and relation.h, so put it here...
 */
typedef enum PGSetOpCmd {
	PG_SETOPCMD_INTERSECT,
	PG_SETOPCMD_INTERSECT_ALL,
	PG_SETOPCMD_EXCEPT,
	PG_SETOPCMD_EXCEPT_ALL
} PGSetOpCmd;

typedef enum PGSetOpStrategy {
	PG_SETOP_SORTED, /* input must be sorted */
	PG_SETOP_HASHED  /* use internal hashtable */
} PGSetOpStrategy;

/*
 * PGOnConflictAction -
 *	  "ON CONFLICT" clause type of query
 *
 * This is needed in both parsenodes.h and plannodes.h, so put it here...
 */
typedef enum PGOnConflictAction {
	PG_ONCONFLICT_NONE,    /* No "ON CONFLICT" clause */
	PG_ONCONFLICT_NOTHING, /* ON CONFLICT ... DO NOTHING */
	PG_ONCONFLICT_UPDATE   /* ON CONFLICT ... DO UPDATE */
} PGOnConflictAction;

}
