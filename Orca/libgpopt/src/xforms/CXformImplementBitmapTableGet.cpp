//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformImplementBitmapTableGet.cpp
//
//	@doc:
//		Implement BitmapTableGet
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementBitmapTableGet.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/ops.h"

using namespace gpopt;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementBitmapTableGet::CXformImplementBitmapTableGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementBitmapTableGet::CXformImplementBitmapTableGet(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalBitmapTableGet(mp),
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // predicate tree
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp))  // bitmap index expression
		  ))
{
}

CXform::EXformPromise
CXformImplementBitmapTableGet::Exfp(CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasSubquery(0) || exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}


	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementBitmapTableGet::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementBitmapTableGet::Transform(CXformContext *pxfctxt,
										 CXformResult *pxfres,
										 CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();
	CLogicalBitmapTableGet *popLogical =
		CLogicalBitmapTableGet::PopConvert(pexpr->Pop());

	CTableDescriptor *ptabdesc = popLogical->Ptabdesc();
	ptabdesc->AddRef();

	CColRefArray *pdrgpcrOutput = popLogical->PdrgpcrOutput();
	pdrgpcrOutput->AddRef();

	CPhysicalBitmapTableScan *popPhysical = GPOS_NEW(mp)
		CPhysicalBitmapTableScan(mp, ptabdesc, pexpr->Pop()->UlOpId(),
								 GPOS_NEW(mp)
									 CName(mp, *popLogical->PnameTableAlias()),
								 pdrgpcrOutput);

	CExpression *pexprCondition = (*pexpr)[0];
	CExpression *pexprIndexPath = (*pexpr)[1];
	pexprCondition->AddRef();
	pexprIndexPath->AddRef();

	CExpression *pexprPhysical = GPOS_NEW(mp)
		CExpression(mp, popPhysical, pexprCondition, pexprIndexPath);
	pxfres->Add(pexprPhysical);
}

// EOF
