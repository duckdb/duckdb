//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CXformImplementSequence.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementSequence.h"

#include "gpos/base.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementSequence::CXformImplementSequence
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementSequence::CXformImplementSequence(CMemoryPool *mp)
	: CXformImplementation(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalSequence(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternMultiLeaf(mp))))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementSequence::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementSequence::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
								   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	CExpressionArray *pdrgpexpr = pexpr->PdrgPexpr();
	pdrgpexpr->AddRef();

	// create alternative expression
	CExpression *pexprAlt = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CPhysicalSequence(mp), pdrgpexpr);
	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}


// EOF
