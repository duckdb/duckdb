//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformInnerApply2InnerJoin.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformInnerApply2InnerJoin.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerApply2InnerJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		outer references must exist
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformInnerApply2InnerJoin::Exfp(CExpressionHandle &exprhdl) const
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
//		CXformInnerApply2InnerJoin::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformInnerApply2InnerJoin::Transform(CXformContext *pxfctxt,
									  CXformResult *pxfres,
									  CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	Decorrelate(pxfctxt, pxfres, pexpr);
}


// EOF
