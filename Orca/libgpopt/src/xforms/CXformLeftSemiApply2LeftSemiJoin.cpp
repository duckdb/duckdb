//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformLeftSemiApply2LeftSemiJoin.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftSemiApply2LeftSemiJoin.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiApply2LeftSemiJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		outer references are not allowed
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftSemiApply2LeftSemiJoin::Exfp(CExpressionHandle &exprhdl) const
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
//		CXformLeftSemiApply2LeftSemiJoin::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformLeftSemiApply2LeftSemiJoin::Transform(CXformContext *pxfctxt,
											CXformResult *pxfres,
											CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	Decorrelate(pxfctxt, pxfres, pexpr);
}


// EOF
