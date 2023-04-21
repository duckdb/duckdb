//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformLeftOuterApply2LeftOuterJoin.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftOuterApply2LeftOuterJoin.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuterApply2LeftOuterJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		outer references are not allowed
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftOuterApply2LeftOuterJoin::Exfp(CExpressionHandle &exprhdl) const
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
//		CXformLeftOuterApply2LeftOuterJoin::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformLeftOuterApply2LeftOuterJoin::Transform(CXformContext *pxfctxt,
											  CXformResult *pxfres,
											  CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	Decorrelate(pxfctxt, pxfres, pexpr);
}


// EOF
