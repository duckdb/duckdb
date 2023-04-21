//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformDifference2LeftAntiSemiJoin.cpp
//
//	@doc:
//		Implementation of the transformation that takes a logical difference and
//		converts it into an aggregate over a left anti-semi join
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformDifference2LeftAntiSemiJoin.h"

#include "gpos/base.h"

#include "gpopt/exception.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformDifference2LeftAntiSemiJoin::CXformDifference2LeftAntiSemiJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformDifference2LeftAntiSemiJoin::CXformDifference2LeftAntiSemiJoin(
	CMemoryPool *mp)
	:  // pattern
	  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalDifference(mp),
		  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternMultiLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformDifference2LeftAntiSemiJoin::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformDifference2LeftAntiSemiJoin::Transform(CXformContext *pxfctxt,
											 CXformResult *pxfres,
											 CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// TODO: Oct 24th 2012, we currently only handle difference all
	//  operators with two children
	GPOS_ASSERT(2 == pexpr->Arity());

	// extract components
	CExpression *pexprLeftChild = (*pexpr)[0];
	CExpression *pexprRightChild = (*pexpr)[1];

	CLogicalDifference *popDifference =
		CLogicalDifference::PopConvert(pexpr->Pop());
	CColRefArray *pdrgpcrOutput = popDifference->PdrgpcrOutput();
	CColRef2dArray *pdrgpdrgpcrInput = popDifference->PdrgpdrgpcrInput();

	// generate the scalar condition for the left anti-semi join
	CExpression *pexprScCond = CUtils::PexprConjINDFCond(mp, pdrgpdrgpcrInput);

	pexprLeftChild->AddRef();
	pexprRightChild->AddRef();

	// assemble the new left anti-semi join logical operator
	CExpression *pexprLASJ =
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalLeftAntiSemiJoin(mp),
								 pexprLeftChild, pexprRightChild, pexprScCond);

	// assemble the aggregate operator
	pdrgpcrOutput->AddRef();

	CExpression *pexprProjList =
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
								 GPOS_NEW(mp) CExpressionArray(mp));

	CExpression *pexprAgg = CUtils::PexprLogicalGbAggGlobal(
		mp, pdrgpcrOutput, pexprLASJ, pexprProjList);

	// add alternative to results
	pxfres->Add(pexprAgg);
}

// EOF
