//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		outer references are not allowed
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn::Exfp(
	CExpressionHandle &exprhdl) const
{
	// if there are outer refs that include columns from the immediate outer child, the
	// transformation is applicable
	if (exprhdl.HasOuterRefs(1 /*child_index*/) &&
		!CUtils::FInnerUsesExternalColsOnly(exprhdl))
	{
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn::Transform(
	CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	Decorrelate(pxfctxt, pxfres, pexpr);
}

// EOF
