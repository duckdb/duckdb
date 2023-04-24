//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		traceflags.h
//
//	@doc:
//		enum of traceflags which can be used in a task's context
//---------------------------------------------------------------------------
#ifndef GPOPT_traceflags_H
#define GPOPT_traceflags_H

#include "duckdb/optimizer/cascade/common/CBitSet.h"

namespace gpos
{
enum EOptTraceFlag
{
	// reserve range 100000-199999 for GPOPT

	////////////////////////////////////////////////////////
	///////////////// debug printing flags /////////////////
	////////////////////////////////////////////////////////

	// print input query
	EopttracePrintQuery = 101000,

	// print output plan
	EopttracePrintPlan = 101001,

	// print xform info
	EopttracePrintXform = 101002,

	// print input and output of xforms
	EopttracePrintXformResults = 101003,

	// print MEMO after exploration
	EopttracePrintMemoAfterExploration = 101004,

	// print MEMO after implementation
	EopttracePrintMemoAfterImplementation = 101005,

	// print MEMO after optimization
	EopttracePrintMemoAfterOptimization = 101006,

	// print jobs in scheduler on each job completion
	EopttracePrintJobScheduler = 101007,

	// print expression properties
	EopttracePrintExpressionProperties = 101008,

	// print group properties
	EopttracePrintGroupProperties = 101009,

	// print optimization context
	EopttracePrintOptimizationContext = 101010,

	// print xform pattern
	EopttracePrintXformPattern = 101011,

	// print optimizer's stats
	EopttracePrintOptimizationStatistics = 101012,

	// enable plan enumeration
	EopttraceEnumeratePlans = 101013,

	// enable plan sampling
	EopttraceSamplePlans = 101014,

	// print MEMO during property enforcement process
	EopttracePrintMemoEnforcement = 101015,

	// print required columns
	EopttracePrintRequiredColumns = 101016,

	// print equivalent distribution specs
	EopttracePrintEquivDistrSpecs = 101017,

	///////////////////////////////////////////////////////
	////////////////// transformations flags //////////////
	///////////////////////////////////////////////////////

	// base for calculating flag ID to disable xform;
	// use: flag = EopttraceDisableXformBase + xformID;
	EopttraceDisableXformBase = 102000,

	// range from 102000 - 102999 is reserved for xform disable flags

	///////////////////////////////////////////////////////
	///////////////////// engine flags ////////////////////
	///////////////////////////////////////////////////////

	// first engine flag
	EopttraceFirstEngineFlag = 103000,	// not currently used

	// produce a minidump
	EopttraceMinidump = 103001,

	// CTE inlining flags
	EopttraceEnableCTEInlining = 103002,

	// Disable all Motion nodes
	EopttraceDisableMotions = 103003,

	// Disable MotionBroadcast nodes
	EopttraceDisableMotionBroadcast = 103004,

	// Disable MotionGather nodes
	EopttraceDisableMotionGather = 103005,

	// Disable MotionHashDistribute nodes
	EopttraceDisableMotionHashDistribute = 103006,

	// Disable MotionHashRandom nodes
	EopttraceDisableMotionRandom = 103007,

	// Disable MotionHashRoutedDistribute nodes
	EopttraceDisableMotionRountedDistribute = 103008,

	// Disable Sort nodes
	EopttraceDisableSort = 103009,

	// Disable Spool nodes
	EopttraceDisableSpool = 103010,

	// Disable partition propagation
	EopttraceDisablePartPropagation = 103011,

	// Disable partition selection
	EopttraceDisablePartSelection = 103012,

	// Disable outer-join To inner-join rewrite
	EopttraceDisableOuterJoin2InnerJoinRewrite = 103013,

	// Enable plan space pruning
	EopttraceEnableSpacePruning = 103014,

	// Always pick multi-stage aggregation whenever such a plan is generated
	EopttraceForceMultiStageAgg = 103015,

	// Enable generating (Redistribute, Broadcast) alternative for hash join children
	EopttraceEnableRedistributeBroadcastHashJoin = 103016,

	// Apply LeftOuter2InnerUnionAllLeftAntiSemiJoin without looking at stats
	EopttraceApplyLeftOuter2InnerUnionAllLeftAntiSemiJoinDisregardingStats =
		103017,

	// Disable sort below Insert for Parquet tables
	EopttraceDisableSortForDMLOnParquet = 103018,

	// Do not keep an order-by, even if it is right under a DML operator
	EopttraceRemoveOrderBelowDML = 103019,

	// prevent plan alternatives where NLJ's outer child is replicated
	EopttraceDisableReplicateInnerNLJOuterChild = 103020,

	// enforce evaluating subqueries using correlated joins (subplans in GPDB)
	EopttraceEnforceCorrelatedExecution = 103021,

	// Always pick plans that expand multiple distinct qualified aggregates into join of single distinct aggregates
	EopttraceForceExpandedMDQAs = 103022,

	// prevent optimizing CTE producer side based on requirements enforced on top of CTE consumer
	EopttraceDisablePushingCTEConsumerReqsToCTEProducer = 103023,

	// prune unused computed columns
	EopttraceDisablePruneUnusedComputedColumns = 103024,

	// enable parallel append
	EopttraceEnableParallelAppend = 103025,

	// create constraint intervals from array expressions in preprocessing
	EopttraceArrayConstraints = 103026,

	// enable motion hazard handling during NLJ optimization
	EopttraceMotionHazardHandling = 103027,

	// non-master gather enforcement for DML queries
	EopttraceDisableNonMasterGatherForDML = 103028,

	// Force the optimizer to pick a plan that minimizes skew but adds an extra motion node when aggs are used
	EopttraceForceAggSkewAvoidance = 103029,

	// Eager Agg
	EopttraceEnableEagerAgg = 103030,

	// Translate unused colrefs. specifically translate all colrefs, including ones
	// that are not referenced in the query.
	EopttraceTranslateUnusedColrefs = 103031,

	// ExpandFullJoin
	EopttraceExpandFullJoin = 103032,

	// Expand LOJs in N-aryjoin
	EopttraceEnableLOJInNAryJoin = 103033,

	// Generate only query join order in DPv2 transform
	EopttraceQueryOnlyInDPv2 = 103034,

	// Generate only Greedy join order in DPv2 transform
	EopttraceGreedyOnlyInDPv2 = 103035,

	// Generate only MinCard join order in DPv2 transform
	EopttraceMinCardOnlyInDPv2 = 103036,

	// Consider non-equality predicates in Dynamic partition selection
	EopttraceAllowGeneralPredicatesforDPE = 103037,

	///////////////////////////////////////////////////////
	///////////////////// statistics flags ////////////////
	//////////////////////////////////////////////////////

	// extract statistics
	EopttraceExtractDXLStats = 104000,

	// extract statistics for all physical DXL nodes
	EopttraceExtractDXLStatsAllNodes = 104001,

	// derive new statistics for dynamic scans when partition elimination applies
	EopttraceDeriveStatsForDPE = 104002,

	// print information about columns with missing statistics
	EopttracePrintColsWithMissingStats = 104003,

	// do not trigger stats derivation for all groups after exploration
	EopttraceDonotDeriveStatsForAllGroups = 104004,

	// Always pick plans that split scalar DQA into a plan with 3-stage aggregation
	EopttraceForceThreeStageScalarDQA = 104005,

	// Penalize HashJoins with a skewed hash distribute under them
	EopttracePenalizeSkewedHashJoin = 104006,

	// Use calibrated bitmap index cost model
	EopttraceCalibratedBitmapIndexCostModel = 104007,
	///////////////////////////////////////////////////////
	/////////// constant expression evaluator flags ///////
	///////////////////////////////////////////////////////

	EopttraceEnableConstantExpressionEvaluation = 105000,

	// do not use the built-in evaluators for integers in constraint derivation
	EopttraceUseExternalConstantExpressionEvaluationForInts = 105001,

	// is nestloop params enabled, it is only enabled in GPDB 6.x onwards.
	EopttraceIndexedNLJOuterRefAsParams = 106000,

	// max
	EopttraceSentinel = 199999
};
}

#ifdef __cplusplus
extern "C" {
#endif	// __cplusplus

// set trace flags based on given bit set, and return two output bit sets of old trace flags values
void SetTraceflags(gpos::CMemoryPool *mp, const gpos::CBitSet *input_bitset,
				   gpos::CBitSet **enable_bitset,
				   gpos::CBitSet **disabled_bitset);

// restore trace flags values based on given bit sets
void ResetTraceflags(gpos::CBitSet *enable_bitset,
					 gpos::CBitSet *disabled_bitset);

#ifdef __cplusplus
}
#endif	// __cplusplus

#endif
