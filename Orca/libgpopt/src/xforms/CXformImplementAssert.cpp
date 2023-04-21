//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementAssert.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementAssert.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementAssert::CXformImplementAssert
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementAssert::CXformImplementAssert(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalAssert(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // predicate
		  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementAssert::Exfp
//
//	@doc:
//		Compute xform promise level for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementAssert::Exfp(CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementAssert::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementAssert::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
								 CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CLogicalAssert *popAssert = CLogicalAssert::PopConvert(pexpr->Pop());
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];
	CException *pexc = popAssert->Pexc();

	// addref all children
	pexprRelational->AddRef();
	pexprScalar->AddRef();

	// assemble physical operator
	CPhysicalAssert *popPhysicalAssert = GPOS_NEW(mp) CPhysicalAssert(
		mp, GPOS_NEW(mp) CException(pexc->Major(), pexc->Minor(),
									pexc->Filename(), pexc->Line()));

	CExpression *pexprAssert = GPOS_NEW(mp)
		CExpression(mp, popPhysicalAssert, pexprRelational, pexprScalar);

	// add alternative to results
	pxfres->Add(pexprAssert);
}


// EOF
