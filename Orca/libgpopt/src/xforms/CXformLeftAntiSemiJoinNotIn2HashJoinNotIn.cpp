//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformLeftAntiSemiJoinNotIn2HashJoinNotIn.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftAntiSemiJoinNotIn2HashJoinNotIn.h"

#include "gpos/base.h"

#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiJoinNotIn2HashJoinNotIn::CXformLeftAntiSemiJoinNotIn2HashJoinNotIn
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformLeftAntiSemiJoinNotIn2HashJoinNotIn::
	CXformLeftAntiSemiJoinNotIn2HashJoinNotIn(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalLeftAntiSemiJoinNotIn(mp),
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
//		CXformLeftAntiSemiJoinNotIn2HashJoinNotIn::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftAntiSemiJoinNotIn2HashJoinNotIn::Exfp(
	CExpressionHandle &exprhdl) const
{
	return CXformUtils::ExfpLogicalJoin2PhysicalJoin(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiJoinNotIn2HashJoinNotIn::Transform
//
//	@doc:
//		actual transformation
//
//---------------------------------------------------------------------------
void
CXformLeftAntiSemiJoinNotIn2HashJoinNotIn::Transform(CXformContext *pxfctxt,
													 CXformResult *pxfres,
													 CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CXformUtils::ImplementHashJoin<CPhysicalLeftAntiSemiHashJoinNotIn>(
		pxfctxt, pxfres, pexpr);

	if (pxfres->Pdrgpexpr()->Size() == 0)
	{
		CExpression *pexprProcessed = NULL;
		if (CXformUtils::FProcessGPDBAntiSemiHashJoin(pxfctxt->Pmp(), pexpr,
													  &pexprProcessed))
		{
			// try again after simplifying join predicate
			CXformUtils::ImplementHashJoin<CPhysicalLeftAntiSemiHashJoinNotIn>(
				pxfctxt, pxfres, pexprProcessed);
			pexprProcessed->Release();
		}
	}
}

// EOF
