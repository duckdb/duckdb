//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformLeftAntiSemiJoinNotIn2NLJoinNotIn.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftAntiSemiJoinNotIn2NLJoinNotIn.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiJoinNotIn2NLJoinNotIn::CXformLeftAntiSemiJoinNotIn2NLJoinNotIn
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformLeftAntiSemiJoinNotIn2NLJoinNotIn::
	CXformLeftAntiSemiJoinNotIn2NLJoinNotIn(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalLeftAntiSemiJoinNotIn(mp),
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // right child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)))  // predicate
	  )
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiJoinNotIn2NLJoinNotIn::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftAntiSemiJoinNotIn2NLJoinNotIn::Exfp(CExpressionHandle &exprhdl) const
{
	return CXformUtils::ExfpLogicalJoin2PhysicalJoin(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiJoinNotIn2NLJoinNotIn::Transform
//
//	@doc:
//		actual transformation
//
//---------------------------------------------------------------------------
void
CXformLeftAntiSemiJoinNotIn2NLJoinNotIn::Transform(CXformContext *pxfctxt,
												   CXformResult *pxfres,
												   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CXformUtils::ImplementNLJoin<CPhysicalLeftAntiSemiNLJoinNotIn>(
		pxfctxt, pxfres, pexpr);
}


// EOF
