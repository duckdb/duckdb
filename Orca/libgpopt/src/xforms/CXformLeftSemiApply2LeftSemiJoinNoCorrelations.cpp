//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformLeftSemiApply2LeftSemiJoinNoCorrelations.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftSemiApply2LeftSemiJoinNoCorrelations.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiApply2LeftSemiJoinNoCorrelations::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		outer references are not allowed
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftSemiApply2LeftSemiJoinNoCorrelations::Exfp(
	CExpressionHandle &exprhdl) const
{
	// if there are no outer references, or if all outer refs do not reference outer
	// child, the transformation is applicable
	if (!exprhdl.HasOuterRefs(1 /*child_index*/) ||
		CUtils::FInnerUsesExternalColsOnly(exprhdl))
	{
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiApply2LeftSemiJoinNoCorrelations::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformLeftSemiApply2LeftSemiJoinNoCorrelations::Transform(
	CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CreateJoinAlternative(pxfctxt, pxfres, pexpr);
}


// EOF
