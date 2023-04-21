//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformLeftAntiSemiJoin2HashJoin.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftAntiSemiJoin2HashJoin.h"

#include "gpos/base.h"

#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiJoin2HashJoin::CXformLeftAntiSemiJoin2HashJoin
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformLeftAntiSemiJoin2HashJoin::CXformLeftAntiSemiJoin2HashJoin(
	CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalLeftAntiSemiJoin(mp),
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // right child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate
		  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiJoin2HashJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftAntiSemiJoin2HashJoin::Exfp(CExpressionHandle &exprhdl) const
{
	return CXformUtils::ExfpLogicalJoin2PhysicalJoin(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiJoin2HashJoin::Transform
//
//	@doc:
//		actual transformation
//
//---------------------------------------------------------------------------
void
CXformLeftAntiSemiJoin2HashJoin::Transform(CXformContext *pxfctxt,
										   CXformResult *pxfres,
										   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CXformUtils::ImplementHashJoin<CPhysicalLeftAntiSemiHashJoin>(
		pxfctxt, pxfres, pexpr);

	if (pxfres->Pdrgpexpr()->Size() == 0)
	{
		CExpression *pexprProcessed = NULL;
		if (CXformUtils::FProcessGPDBAntiSemiHashJoin(pxfctxt->Pmp(), pexpr,
													  &pexprProcessed))
		{
			// try again after simplifying join predicate
			CXformUtils::ImplementHashJoin<CPhysicalLeftAntiSemiHashJoin>(
				pxfctxt, pxfres, pexprProcessed);
			pexprProcessed->Release();
		}
	}
}


// EOF
