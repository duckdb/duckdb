//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformInnerJoin2HashJoin.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformInnerJoin2HashJoin.h"

#include "gpos/base.h"

#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoin2HashJoin::CXformInnerJoin2HashJoin
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformInnerJoin2HashJoin::CXformInnerJoin2HashJoin(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
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
//		CXformInnerJoin2HashJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformInnerJoin2HashJoin::Exfp(CExpressionHandle &exprhdl) const
{
	return CXformUtils::ExfpLogicalJoin2PhysicalJoin(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoin2HashJoin::Transform
//
//	@doc:
//		actual transformation
//
//---------------------------------------------------------------------------
void
CXformInnerJoin2HashJoin::Transform(CXformContext *pxfctxt,
									CXformResult *pxfres,
									CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CXformUtils::ImplementHashJoin<CPhysicalInnerHashJoin>(pxfctxt, pxfres,
														   pexpr);
}

// EOF
