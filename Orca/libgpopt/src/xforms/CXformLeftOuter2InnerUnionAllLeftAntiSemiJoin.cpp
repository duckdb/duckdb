//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin.cpp
//
//	@doc:
//		Transform
//      LOJ
//        |--Small
//        +--Big
//
// 		to
//
//      UnionAll
//      |---CTEConsumer(A)
//      +---Project_{append nulls)
//          +---LASJ_(key(Small))
//                   |---CTEConsumer(B)
//                   +---Gb(keys(Small))
//                        +---CTEConsumer(A)
//
//		where B is the CTE that produces Small
//		and A is the CTE that produces InnerJoin(Big, CTEConsumer(B)).
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin.h"

#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/traceflags/traceflags.h"

using namespace gpopt;

// if ratio of the cardinalities outer/inner is below this value, we apply the xform
const DOUBLE
	CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::m_dOuterInnerRatioThreshold =
		0.001;

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::
	CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin(CMemoryPool *mp)
	:  // pattern
	  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalLeftOuterJoin(mp),
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp)),  // left child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp)),  // right child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate
		  ))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle.
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::Exfp(
	CExpressionHandle &exprhdl) const
{
	CColRefSet *pcrsInner = exprhdl.DeriveOutputColumns(1 /*child_index*/);
	CExpression *pexprScalar = exprhdl.PexprScalarExactChild(2 /*child_index*/);
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	if (NULL == pexprScalar ||
		!CPredicateUtils::FSimpleEqualityUsingCols(mp, pexprScalar, pcrsInner))
	{
		return ExfpNone;
	}

	if (GPOS_FTRACE(
			gpos::
				EopttraceApplyLeftOuter2InnerUnionAllLeftAntiSemiJoinDisregardingStats) ||
		NULL == exprhdl.Pgexpr())
	{
		return CXform::ExfpHigh;
	}

	// check if stats are derivable on child groups
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CGroup *pgroupChild = (*exprhdl.Pgexpr())[ul];
		if (!pgroupChild->FScalar() && !pgroupChild->FStatsDerivable(mp))
		{
			// stats must be derivable on every child
			return CXform::ExfpNone;
		}
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::FCheckStats
//
//	@doc:
//		Check the stats ratio to decide whether to apply the xform or not.
//
//---------------------------------------------------------------------------
BOOL
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::FApplyXformUsingStatsInfo(
	const IStatistics *outer_stats, const IStatistics *inner_side_stats) const
{
	if (GPOS_FTRACE(
			gpos::
				EopttraceApplyLeftOuter2InnerUnionAllLeftAntiSemiJoinDisregardingStats))
	{
		return true;
	}

	if (NULL == outer_stats || NULL == inner_side_stats)
	{
		return false;
	}

	DOUBLE num_rows_outer = outer_stats->Rows().Get();
	DOUBLE dRowsInner = inner_side_stats->Rows().Get();
	GPOS_ASSERT(0 < dRowsInner);

	return num_rows_outer / dRowsInner <= m_dOuterInnerRatioThreshold;
}


// Apply the transformation, e.g.
//
// clang-format off
// Input:
//  +--CLogicalLeftOuterJoin
//     |--CLogicalGet "items", Columns: ["i_item_sk" (95)]
//     |--CLogicalGet "store_sales", Columns: ["ss_item_sk" (124)]
//     +--CScalarCmp (=)
//        |--CScalarIdent "i_item_sk"
//        +--CScalarIdent "ss_item_sk"
//  Output:
//  Alternatives:
//  0:
//  +--CLogicalCTEAnchor (2)
//     +--CLogicalCTEAnchor (3)
//        +--CLogicalUnionAll ["i_item_sk" (95), "ss_item_sk" (124)]
//           |--CLogicalCTEConsumer (3), Columns: ["i_item_sk" (95), "ss_item_sk" (124)]
//           +--CLogicalProject
//              |--CLogicalLeftAntiSemiJoin
//              |  |--CLogicalCTEConsumer (2), Columns: ["i_item_sk" (342)]
//              |  |--CLogicalGbAgg( GetGlobalMemoryPool ) Grp Cols: ["i_item_sk" (343)][Global], Minimal Grp Cols: [], Generates Duplicates :[ 0 ]
//              |  |  |--CLogicalCTEConsumer (3), Columns: ["i_item_sk" (343), "ss_item_sk" (344)]
//              |  |  +--CScalarProjectList
//              |  +--CScalarBoolOp (EboolopNot)
//              |     +--CScalarIsDistinctFrom (=)
//              |        |--CScalarIdent "i_item_sk" (342)
//              |        +--CScalarIdent "i_item_sk" (343)
//              +--CScalarProjectList
//                 +--CScalarProjectElement "ss_item_sk" (466)
//                    +--CScalarConst (null)
//
//  +--CLogicalCTEProducer (2), Columns: ["i_item_sk" (190)]
//     +--CLogicalGet "items", Columns: ["i_item_sk" (190)]
//
//  +--CLogicalCTEProducer (3), Columns: ["i_item_sk" (247), "ss_item_sk" (248)]
//      +--CLogicalInnerJoin
//         |--CLogicalCTEConsumer (0), Columns: ["ss_item_sk" (248)]
//         |--CLogicalCTEConsumer (2), Columns: ["i_item_sk" (247)]
//         +--CScalarCmp (=)
//            |--CScalarIdent "i_item_sk" (247)
//            +--CScalarIdent "ss_item_sk" (248)
//
// clang-format on
void
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::Transform(
	CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();
	// extract components
	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprInner = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	if (!FValidInnerExpr(pexprInner))
	{
		return;
	}

	if (!FApplyXformUsingStatsInfo(pexprOuter->Pstats(), pexprInner->Pstats()))
	{
		return;
	}

	const ULONG ulCTEOuterId = COptCtxt::PoctxtFromTLS()->Pcteinfo()->next_id();
	CColRefSet *outer_refs = pexprOuter->DeriveOutputColumns();
	CColRefArray *pdrgpcrOuter = outer_refs->Pdrgpcr(mp);
	(void) CXformUtils::PexprAddCTEProducer(mp, ulCTEOuterId, pdrgpcrOuter,
											pexprOuter);

	// invert the order of the branches of the original join, so that the small one becomes
	// inner
	pexprInner->AddRef();
	pexprScalar->AddRef();
	CExpression *pexprInnerJoin = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalInnerJoin(mp), pexprInner,
		CXformUtils::PexprCTEConsumer(mp, ulCTEOuterId, pdrgpcrOuter),
		pexprScalar);

	CColRefSet *pcrsJoinOutput = pexpr->DeriveOutputColumns();
	CColRefArray *pdrgpcrJoinOutput = pcrsJoinOutput->Pdrgpcr(mp);
	const ULONG ulCTEJoinId = COptCtxt::PoctxtFromTLS()->Pcteinfo()->next_id();
	(void) CXformUtils::PexprAddCTEProducer(mp, ulCTEJoinId, pdrgpcrJoinOutput,
											pexprInnerJoin);

	CColRefSet *pcrsScalar = pexprScalar->DeriveUsedColumns();
	CColRefSet *pcrsInner = pexprInner->DeriveOutputColumns();

	CColRefArray *pdrgpcrProjectOutput = NULL;
	CExpression *pexprProjectAppendNulls = PexprProjectOverLeftAntiSemiJoin(
		mp, pdrgpcrOuter, pcrsScalar, pcrsInner, pdrgpcrJoinOutput, ulCTEJoinId,
		ulCTEOuterId, &pdrgpcrProjectOutput);
	GPOS_ASSERT(NULL != pdrgpcrProjectOutput);

	CColRef2dArray *pdrgpdrgpcrUnionInput = GPOS_NEW(mp) CColRef2dArray(mp);
	pdrgpcrJoinOutput->AddRef();
	pdrgpdrgpcrUnionInput->Append(pdrgpcrJoinOutput);
	pdrgpdrgpcrUnionInput->Append(pdrgpcrProjectOutput);
	pdrgpcrJoinOutput->AddRef();

	CExpression *pexprUnionAll = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp)
			CLogicalUnionAll(mp, pdrgpcrJoinOutput, pdrgpdrgpcrUnionInput),
		CXformUtils::PexprCTEConsumer(mp, ulCTEJoinId, pdrgpcrJoinOutput),
		pexprProjectAppendNulls);
	CExpression *pexprJoinAnchor = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalCTEAnchor(mp, ulCTEJoinId), pexprUnionAll);
	CExpression *pexprOuterAnchor = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalCTEAnchor(mp, ulCTEOuterId), pexprJoinAnchor);
	pexprInnerJoin->Release();

	pxfres->Add(pexprOuterAnchor);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::FValidInnerExpr
//
//	@doc:
//		Check if the inner expression is of a type which should be considered
//		by this xform.
//
//---------------------------------------------------------------------------
BOOL
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::FValidInnerExpr(
	CExpression *pexprInner)
{
	GPOS_ASSERT(NULL != pexprInner);

	// set of inner operator ids that should not be considered because they usually
	// generate a relatively small number of tuples
	COperator::EOperatorId rgeopids[] = {
		COperator::EopLogicalConstTableGet,
		COperator::EopLogicalGbAgg,
		COperator::EopLogicalLimit,
	};

	const COperator::EOperatorId op_id = pexprInner->Pop()->Eopid();
	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(rgeopids); ++ul)
	{
		if (rgeopids[ul] == op_id)
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::PexprLeftAntiSemiJoinWithInnerGroupBy
//
//	@doc:
//		Construct a left anti semi join with the CTE consumer (ulCTEJoinId) as outer
//		and a group by as inner.
//
//---------------------------------------------------------------------------
CExpression *
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::
	PexprLeftAntiSemiJoinWithInnerGroupBy(CMemoryPool *mp,
										  CColRefArray *pdrgpcrOuter,
										  CColRefArray *pdrgpcrOuterCopy,
										  CColRefSet *pcrsScalar,
										  CColRefSet *pcrsInner,
										  CColRefArray *pdrgpcrJoinOutput,
										  ULONG ulCTEJoinId, ULONG ulCTEOuterId)
{
	// compute the original outer keys and their correspondent keys on the two branches
	// of the LASJ
	CColRefSet *pcrsOuterKeys = GPOS_NEW(mp) CColRefSet(mp);
	pcrsOuterKeys->Include(pcrsScalar);
	pcrsOuterKeys->Difference(pcrsInner);
	CColRefArray *pdrgpcrOuterKeys = pcrsOuterKeys->Pdrgpcr(mp);

	CColRefArray *pdrgpcrConsumer2Output =
		CUtils::PdrgpcrCopy(mp, pdrgpcrJoinOutput);
	ULongPtrArray *pdrgpulIndexesOfOuterInGby =
		pdrgpcrJoinOutput->IndexesOfSubsequence(pdrgpcrOuterKeys);

	GPOS_ASSERT(NULL != pdrgpulIndexesOfOuterInGby);
	CColRefArray *pdrgpcrGbyKeys = CXformUtils::PdrgpcrReorderedSubsequence(
		mp, pdrgpcrConsumer2Output, pdrgpulIndexesOfOuterInGby);

	CExpression *pexprGby = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp)
			CLogicalGbAgg(mp, pdrgpcrGbyKeys, COperator::EgbaggtypeGlobal),
		CXformUtils::PexprCTEConsumer(mp, ulCTEJoinId, pdrgpcrConsumer2Output),
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp)));

	ULongPtrArray *pdrgpulIndexesOfOuterKeys =
		pdrgpcrOuter->IndexesOfSubsequence(pdrgpcrOuterKeys);
	GPOS_ASSERT(NULL != pdrgpulIndexesOfOuterKeys);
	CColRefArray *pdrgpcrKeysInOuterCopy =
		CXformUtils::PdrgpcrReorderedSubsequence(mp, pdrgpcrOuterCopy,
												 pdrgpulIndexesOfOuterKeys);

	CColRef2dArray *pdrgpdrgpcrLASJInput = GPOS_NEW(mp) CColRef2dArray(mp);
	pdrgpdrgpcrLASJInput->Append(pdrgpcrKeysInOuterCopy);
	pdrgpcrGbyKeys->AddRef();
	pdrgpdrgpcrLASJInput->Append(pdrgpcrGbyKeys);

	pcrsOuterKeys->Release();
	pdrgpcrOuterKeys->Release();

	CExpression *pexprLeftAntiSemi = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalLeftAntiSemiJoin(mp),
		CXformUtils::PexprCTEConsumer(mp, ulCTEOuterId, pdrgpcrOuterCopy),
		pexprGby, CUtils::PexprConjINDFCond(mp, pdrgpdrgpcrLASJInput));

	pdrgpdrgpcrLASJInput->Release();
	pdrgpulIndexesOfOuterInGby->Release();
	pdrgpulIndexesOfOuterKeys->Release();

	return pexprLeftAntiSemi;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::PexprProjectOverLeftAntiSemiJoin
//
//	@doc:
//		Return a project over a left anti semi join that appends nulls for all
//		columns in the original inner child.
//
//---------------------------------------------------------------------------
CExpression *
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::PexprProjectOverLeftAntiSemiJoin(
	CMemoryPool *mp, CColRefArray *pdrgpcrOuter, CColRefSet *pcrsScalar,
	CColRefSet *pcrsInner, CColRefArray *pdrgpcrJoinOutput, ULONG ulCTEJoinId,
	ULONG ulCTEOuterId, CColRefArray **ppdrgpcrProjectOutput)
{
	GPOS_ASSERT(NULL != pdrgpcrOuter);
	GPOS_ASSERT(NULL != pcrsScalar);
	GPOS_ASSERT(NULL != pcrsInner);
	GPOS_ASSERT(NULL != pdrgpcrJoinOutput);

	// make a copy of outer for the second CTE consumer (outer of LASJ)
	CColRefArray *pdrgpcrOuterCopy = CUtils::PdrgpcrCopy(mp, pdrgpcrOuter);

	CExpression *pexprLeftAntiSemi = PexprLeftAntiSemiJoinWithInnerGroupBy(
		mp, pdrgpcrOuter, pdrgpcrOuterCopy, pcrsScalar, pcrsInner,
		pdrgpcrJoinOutput, ulCTEJoinId, ulCTEOuterId);

	ULongPtrArray *pdrgpulIndexesOfOuter =
		pdrgpcrJoinOutput->IndexesOfSubsequence(pdrgpcrOuter);
	GPOS_ASSERT(NULL != pdrgpulIndexesOfOuter);

	UlongToColRefMap *colref_mapping = GPOS_NEW(mp) UlongToColRefMap(mp);
	const ULONG ulOuterCopyLength = pdrgpcrOuterCopy->Size();

	for (ULONG ul = 0; ul < ulOuterCopyLength; ++ul)
	{
		ULONG ulOrigIndex = *(*pdrgpulIndexesOfOuter)[ul];
		CColRef *pcrOriginal = (*pdrgpcrJoinOutput)[ulOrigIndex];
#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif
			colref_mapping->Insert(GPOS_NEW(mp) ULONG(pcrOriginal->Id()),
								   (*pdrgpcrOuterCopy)[ul]);
		GPOS_ASSERT(fInserted);
	}

	CColRefArray *pdrgpcrInner = pcrsInner->Pdrgpcr(mp);
	CExpression *pexprProject = CUtils::PexprLogicalProjectNulls(
		mp, pdrgpcrInner, pexprLeftAntiSemi, colref_mapping);

	// compute the output array in the order needed by the union-all above the projection
	*ppdrgpcrProjectOutput = CUtils::PdrgpcrRemap(
		mp, pdrgpcrJoinOutput, colref_mapping, true /*must_exist*/);

	pdrgpcrInner->Release();
	colref_mapping->Release();
	pdrgpulIndexesOfOuter->Release();

	return pexprProject;
}

BOOL
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::IsApplyOnce()
{
	return true;
}
// EOF
