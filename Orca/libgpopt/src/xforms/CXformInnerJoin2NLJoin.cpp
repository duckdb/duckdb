//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CXformInnerJoin2NLJoin.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformInnerJoin2NLJoin.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformInnerJoin2HashJoin.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoin2NLJoin::CXformInnerJoin2NLJoin
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformInnerJoin2NLJoin::CXformInnerJoin2NLJoin(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
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
//		CXformInnerJoin2NLJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformInnerJoin2NLJoin::Exfp(CExpressionHandle &exprhdl) const
{
	return CXformUtils::ExfpLogicalJoin2PhysicalJoin(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoin2NLJoin::Transform
//
//	@doc:
//		actual transformation
//
//---------------------------------------------------------------------------
void
CXformInnerJoin2NLJoin::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
								  CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CXformUtils::ImplementNLJoin<CPhysicalInnerNLJoin>(pxfctxt, pxfres, pexpr);
}


// EOF
