//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformIntersectAll2LeftSemiJoin.cpp
//
//	@doc:
//		Implement the transformation of CLogicalIntersectAll into a left semi join
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformIntersectAll2LeftSemiJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefComputed.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/exception.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/ops.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformIntersectAll2LeftSemiJoin::CXformIntersectAll2LeftSemiJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformIntersectAll2LeftSemiJoin::CXformIntersectAll2LeftSemiJoin(
	CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalIntersectAll(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left relational child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp))  // right relational child
			  ))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformIntersectAll2LeftSemiJoin::Transform
//
//	@doc:
//		Actual transformation that transforms a intersect all into a left semi
//		join over a window operation over the inputs
//
//---------------------------------------------------------------------------
void
CXformIntersectAll2LeftSemiJoin::Transform(CXformContext *pxfctxt,
										   CXformResult *pxfres,
										   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// TODO: we currently only handle intersect all operators that
	// have two children
	GPOS_ASSERT(2 == pexpr->Arity());

	// extract components
	CExpression *pexprLeftChild = (*pexpr)[0];
	CExpression *pexprRightChild = (*pexpr)[1];

	CLogicalIntersectAll *popIntersectAll =
		CLogicalIntersectAll::PopConvert(pexpr->Pop());
	CColRef2dArray *pdrgpdrgpcrInput = popIntersectAll->PdrgpdrgpcrInput();

	CExpression *pexprLeftWindow = CXformUtils::PexprWindowWithRowNumber(
		mp, pexprLeftChild, (*pdrgpdrgpcrInput)[0]);
	CExpression *pexprRightWindow = CXformUtils::PexprWindowWithRowNumber(
		mp, pexprRightChild, (*pdrgpdrgpcrInput)[1]);

	CColRef2dArray *pdrgpdrgpcrInputNew = GPOS_NEW(mp) CColRef2dArray(mp);
	CColRefArray *pdrgpcrLeftNew =
		CUtils::PdrgpcrExactCopy(mp, (*pdrgpdrgpcrInput)[0]);
	pdrgpcrLeftNew->Append(CXformUtils::PcrProjectElement(
		pexprLeftWindow, 0 /* row_number window function*/));

	CColRefArray *pdrgpcrRightNew =
		CUtils::PdrgpcrExactCopy(mp, (*pdrgpdrgpcrInput)[1]);
	pdrgpcrRightNew->Append(CXformUtils::PcrProjectElement(
		pexprRightWindow, 0 /* row_number window function*/));

	pdrgpdrgpcrInputNew->Append(pdrgpcrLeftNew);
	pdrgpdrgpcrInputNew->Append(pdrgpcrRightNew);

	CExpression *pexprScCond =
		CUtils::PexprConjINDFCond(mp, pdrgpdrgpcrInputNew);

	// assemble the new logical operator
	CExpression *pexprLSJ = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CLogicalLeftSemiJoin(mp), pexprLeftWindow,
					pexprRightWindow, pexprScCond);

	// clean up
	pdrgpdrgpcrInputNew->Release();

	pxfres->Add(pexprLSJ);
}

// EOF
