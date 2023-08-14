//---------------------------------------------------------------------------
//	@filename:
//		CXform.h
//
//	@doc:
//		Base class for all transformations: substitution, exploration,
//		and implementation
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/xforms/CXformContext.h"
#include "duckdb/optimizer/cascade/xforms/CXformResult.h"

#include <bitset>

namespace gpopt {

using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXform
//
//	@doc:
//		base class for all transformations
//
//---------------------------------------------------------------------------
class CXform {
public:
	// pattern
	duckdb::unique_ptr<Operator> m_operator;

public:
	// private copy ctor
	CXform(CXform &);
	// ctor
	explicit CXform(duckdb::unique_ptr<Operator> expression) : m_operator(std::move(expression)) {};
	// dtor
	virtual ~CXform() = default;

public:
	// identification
	//
	// IMPORTANT: when adding new Xform Ids, please add them near the end of the enum (before ExfInvalid). Xform Ids are
	// sometimes referenced using their location in the array (e.g. when disabling xforms using traceflags), so shifting
	// these ids may result in accidentally disabling the wrong xform
	enum EXformId {
		/* I comment here */
		ExfGet2TableScan = 0,
		ExfLogicalProj2PhysicalProj,
		ExfOrderImplementation,
		ExfFilterImplementation,
		ExfDummyScanImplementation,
		ExfInnerJoin2HashJoin, 
		/* */
		ExfProject2ComputeScalarExfn,
		ExfSelect2Filter,
		ExfIndexGet2IndexScan,
		ExfDynamicGet2DynamicTableScan,
		ExfDynamicIndexGet2DynamicIndexScan,
		ExfImplementSequence,
		ExfImplementConstTableGet,
		ExfUnnestTVF,
		ExfImplementTVF,
		ExfImplementTVFNoArgs,
		ExfSelect2IndexGet,
		ExfSelect2DynamicIndexGet,
		ExfSelect2PartialDynamicIndexGet,
		ExfSimplifySelectWithSubquery,
		ExfSimplifyProjectWithSubquery,
		ExfSelect2Apply,
		ExfProject2Apply,
		ExfGbAgg2Apply,
		ExfSubqJoin2Apply,
		ExfInnerJoin2IndexGetApply,
		ExfInnerJoin2DynamicIndexGetApply,
		ExfInnerApplyWithOuterKey2InnerJoin,
		ExfInnerJoin2NLJoin,
		ExfImplementIndexApply,
		ExfInnerApply2InnerJoin,
		ExfInnerApply2InnerJoinNoCorrelations,
		ExfImplementInnerCorrelatedApply,
		ExfLeftOuterApply2LeftOuterJoin,
		ExfLeftOuterApply2LeftOuterJoinNoCorrelations,
		ExfImplementLeftOuterCorrelatedApply,
		ExfLeftSemiApply2LeftSemiJoin,
		ExfLeftSemiApplyWithExternalCorrs2InnerJoin,
		ExfLeftSemiApply2LeftSemiJoinNoCorrelations,
		ExfLeftAntiSemiApply2LeftAntiSemiJoin,
		ExfLeftAntiSemiApply2LeftAntiSemiJoinNoCorrelations,
		ExfLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn,
		ExfLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotInNoCorrelations,
		ExfPushDownLeftOuterJoin,
		ExfSimplifyLeftOuterJoin,
		ExfLeftOuterJoin2NLJoin,
		ExfLeftOuterJoin2HashJoin,
		ExfLeftSemiJoin2NLJoin,
		ExfLeftSemiJoin2HashJoin,
		ExfLeftAntiSemiJoin2CrossProduct,
		ExfLeftAntiSemiJoinNotIn2CrossProduct,
		ExfLeftAntiSemiJoin2NLJoin,
		ExfLeftAntiSemiJoinNotIn2NLJoinNotIn,
		ExfLeftAntiSemiJoin2HashJoin,
		ExfLeftAntiSemiJoinNotIn2HashJoinNotIn,
		ExfGbAgg2HashAgg,
		ExfGbAgg2StreamAgg,
		ExfGbAgg2ScalarAgg,
		ExfGbAggDedup2HashAggDedup,
		ExfGbAggDedup2StreamAggDedup,
		ExfImplementLimit,
		ExfImplementLogicalGet,
		ExfIntersectAll2LeftSemiJoin,
		ExfIntersect2Join,
		ExfDifference2LeftAntiSemiJoin,
		ExfDifferenceAll2LeftAntiSemiJoin,
		ExfUnion2UnionAll,
		ExfImplementUnionAll,
		ExfInsert2DML,
		ExfDelete2DML,
		ExfUpdate2DML,
		ExfImplementDML,
		ExfImplementRowTrigger,
		ExfImplementSplit,
		ExfJoinCommutativity,
		ExfJoinAssociativity,
		ExfSemiJoinSemiJoinSwap,
		ExfSemiJoinAntiSemiJoinSwap,
		ExfSemiJoinAntiSemiJoinNotInSwap,
		ExfSemiJoinInnerJoinSwap,
		ExfAntiSemiJoinAntiSemiJoinSwap,
		ExfAntiSemiJoinAntiSemiJoinNotInSwap,
		ExfAntiSemiJoinSemiJoinSwap,
		ExfAntiSemiJoinInnerJoinSwap,
		ExfAntiSemiJoinNotInAntiSemiJoinSwap,
		ExfAntiSemiJoinNotInAntiSemiJoinNotInSwap,
		ExfAntiSemiJoinNotInSemiJoinSwap,
		ExfAntiSemiJoinNotInInnerJoinSwap,
		ExfInnerJoinSemiJoinSwap,
		ExfInnerJoinAntiSemiJoinSwap,
		ExfInnerJoinAntiSemiJoinNotInSwap,
		ExfLeftSemiJoin2InnerJoin,
		ExfLeftSemiJoin2InnerJoinUnderGb,
		ExfLeftSemiJoin2CrossProduct,
		ExfSplitLimit,
		ExfSimplifyGbAgg,
		ExfCollapseGbAgg,
		ExfPushGbBelowJoin,
		ExfPushGbDedupBelowJoin,
		ExfPushGbWithHavingBelowJoin,
		ExfPushGbBelowUnion,
		ExfPushGbBelowUnionAll,
		ExfSplitGbAgg,
		ExfSplitGbAggDedup,
		ExfSplitDQA,
		ExfSequenceProject2Apply,
		ExfImplementSequenceProject,
		ExfImplementAssert,
		ExfCTEAnchor2Sequence,
		ExfCTEAnchor2TrivialSelect,
		ExfInlineCTEConsumer,
		ExfInlineCTEConsumerUnderSelect,
		ExfImplementCTEProducer,
		ExfImplementCTEConsumer,
		ExfExpandFullOuterJoin,
		ExfExternalGet2ExternalScan,
		ExfSelect2BitmapBoolOp,
		ExfSelect2DynamicBitmapBoolOp,
		ExfImplementBitmapTableGet,
		ExfImplementDynamicBitmapTableGet,
		ExfInnerJoin2PartialDynamicIndexGetApply,
		ExfLeftOuter2InnerUnionAllLeftAntiSemiJoin,
		ExfImplementLeftSemiCorrelatedApply,
		ExfImplementLeftSemiCorrelatedApplyIn,
		ExfImplementLeftAntiSemiCorrelatedApply,
		ExfImplementLeftAntiSemiCorrelatedApplyNotIn,
		ExfLeftSemiApplyIn2LeftSemiJoin,
		ExfLeftSemiApplyInWithExternalCorrs2InnerJoin,
		ExfLeftSemiApplyIn2LeftSemiJoinNoCorrelations,
		ExfInnerJoin2BitmapIndexGetApply,
		ExfImplementPartitionSelector,
		ExfMaxOneRow2Assert,
		ExfInnerJoinWithInnerSelect2IndexGetApply,
		ExfInnerJoinWithInnerSelect2DynamicIndexGetApply,
		ExfInnerJoinWithInnerSelect2PartialDynamicIndexGetApply,
		ExfInnerJoin2DynamicBitmapIndexGetApply,
		ExfInnerJoinWithInnerSelect2BitmapIndexGetApply,
		ExfInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply,
		ExfGbAggWithMDQA2Join,
		ExfCollapseProject,
		ExfRemoveSubqDistinct,
		ExfLeftOuterJoin2BitmapIndexGetApply,
		ExfLeftOuterJoin2IndexGetApply,
		ExfLeftOuterJoinWithInnerSelect2BitmapIndexGetApply,
		ExfLeftOuterJoinWithInnerSelect2IndexGetApply,
		ExfEagerAgg,
		ExfImplementFullOuterMergeJoin,
		ExfLeftOuterJoin2DynamicBitmapIndexGetApply,
		ExfLeftOuterJoin2DynamicIndexGetApply,
		ExfLeftOuterJoinWithInnerSelect2DynamicBitmapIndexGetApply,
		ExfLeftOuterJoinWithInnerSelect2DynamicIndexGetApply,
		ExfImplementColumnDataGet,
		ExfInvalid,
		ExfSentinel = ExfInvalid
	};
	// promise levels; used for prioritizing xforms as well as bypassing inapplicable xforms
	enum EXformPromise {
		ExfpNone,
		ExfpLow,
		ExfpMedium,
		ExfpHigh // xform has high priority
	};

public:
	// ident accessors
	virtual EXformId ID() const = 0;
	// return a string for xform name
	virtual const CHAR *Name() const = 0;
	// return true if xform should be applied only once. For expression of type CPatternTree, in deep trees, the number
	// of expressions generated for group expression can be significantly large causing the Xform to be applied many
	// times. This can lead to significantly long planning time, so such Xform should only be applied once
	virtual bool IsApplyOnce();

public:
	// the following functions check xform type
	// is xform substitution?
	virtual bool FSubstitution() const {
		return false;
	}
	// is xform exploration?
	virtual bool FExploration() const {
		return false;
	}
	// is xform implementation?
	virtual bool FImplementation() const {
		return false;
	}
	// actual transformation
	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres, Operator *pexpr) const = 0;
	// check compatibility with another xform
	virtual bool FCompatible(CXform::EXformId) {
		return true;
	}
	// compute xform promise for a given expression handle
	virtual EXformPromise XformPromise(CExpressionHandle &exprhdl) const = 0;

public:
	// equality function over xform ids
	static bool FEqualIds(const CHAR *sz_id_one, const CHAR *sz_id_two);
	// returns a set containing all xforms related to index join
	// caller takes ownership of the returned set
	static bitset<ExfSentinel> PbsIndexJoinXforms();
	// returns a set containing all xforms related to bitmap indexes
	// caller takes ownership of the returned set
	static bitset<ExfSentinel> PbsBitmapIndexXforms();
	// returns a set containing all xforms related to heterogeneous indexes
	// caller takes ownership of the returned set
	static bitset<ExfSentinel> PbsHeterogeneousIndexXforms();
	// returns a set containing all xforms that generate a plan with a hash join
	// caller takes ownership of the returned set
	static bitset<ExfSentinel> PbsHashJoinXforms();
	// returns a set containing xforms to use only the join order as available
	// in the query
	static bitset<ExfSentinel> PbsJoinOrderInQueryXforms();
	// returns a set containing xforms to use combination of greedy xforms
	// for join order
	static bitset<ExfSentinel> PbsJoinOrderOnGreedyXforms();
	// returns a set containing xforms to use for exhaustive join order
	static bitset<ExfSentinel> PbsJoinOrderOnExhaustiveXforms();
	// returns a set containing xforms to use for exhaustive2 join order
	static bitset<ExfSentinel> PbsJoinOrderOnExhaustive2Xforms();
}; // class CXform
typedef bitset<CXform::ExfSentinel> CXform_set;
} // namespace gpopt