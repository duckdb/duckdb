//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformLeftSemiJoin2InnerJoinUnderGb.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftSemiJoin2InnerJoinUnderGb.h"

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/operators/ops.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiJoin2InnerJoinUnderGb::CXformLeftSemiJoin2InnerJoinUnderGb
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformLeftSemiJoin2InnerJoinUnderGb::CXformLeftSemiJoin2InnerJoinUnderGb(
	CMemoryPool *mp)
	:  // pattern
	  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalLeftSemiJoin(mp),
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // right child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // predicate
		  ))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiJoin2InnerJoinUnderGb::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftSemiJoin2InnerJoinUnderGb::Exfp(CExpressionHandle &exprhdl) const
{
	CColRefSet *pcrsInnerOutput = exprhdl.DeriveOutputColumns(1);
	CExpression *pexprScalar = exprhdl.PexprScalarExactChild(2);
	CAutoMemoryPool amp;
	if (exprhdl.HasOuterRefs() || NULL == exprhdl.DeriveKeyCollection(0) ||
		NULL == pexprScalar ||
		CPredicateUtils::FSimpleEqualityUsingCols(amp.Pmp(), pexprScalar,
												  pcrsInnerOutput))
	{
		return ExfpNone;
	}

	return ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiJoin2InnerJoinUnderGb::Transform
//
//	@doc:
//		actual transformation
//
//---------------------------------------------------------------------------
void
CXformLeftSemiJoin2InnerJoinUnderGb::Transform(CXformContext *pxfctxt,
											   CXformResult *pxfres,
											   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprInner = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	pexprOuter->AddRef();
	pexprInner->AddRef();
	pexprScalar->AddRef();

	CColRefArray *pdrgpcrKeys = NULL;
	CColRefArray *pdrgpcrGrouping =
		CUtils::PdrgpcrGroupingKey(mp, pexprOuter, &pdrgpcrKeys);
	GPOS_ASSERT(NULL != pdrgpcrKeys);

	CExpression *pexprInnerJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
		mp, pexprOuter, pexprInner, pexprScalar);

	CExpression *pexprGb = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CLogicalGbAggDeduplicate(
			mp, pdrgpcrGrouping, COperator::EgbaggtypeGlobal /*egbaggtype*/,
			pdrgpcrKeys),
		pexprInnerJoin,
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp)));

	pxfres->Add(pexprGb);
}

// EOF
