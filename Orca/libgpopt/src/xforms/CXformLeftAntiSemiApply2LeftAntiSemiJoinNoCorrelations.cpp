//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformLeftAntiSemiApply2LeftAntiSemiJoinNoCorrelations.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftAntiSemiApply2LeftAntiSemiJoinNoCorrelations.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiApply2LeftAntiSemiJoinNoCorrelations::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		outer references are not allowed
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftAntiSemiApply2LeftAntiSemiJoinNoCorrelations::Exfp(
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
//		CXformLeftAntiSemiApply2LeftAntiSemiJoinNoCorrelations::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformLeftAntiSemiApply2LeftAntiSemiJoinNoCorrelations::Transform(
	CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CreateJoinAlternative(pxfctxt, pxfres, pexpr);
}


// EOF
