//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementSplit.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementSplit.h"

#include "gpos/base.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementSplit::CXformImplementSplit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementSplit::CXformImplementSplit(CMemoryPool *mp)
	: CXformImplementation(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalSplit(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementSplit::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementSplit::Exfp(CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}
	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementSplit::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementSplit::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
								CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalSplit *popSplit = CLogicalSplit::PopConvert(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components for alternative
	CColRefArray *pdrgpcrDelete = popSplit->PdrgpcrDelete();
	pdrgpcrDelete->AddRef();

	CColRefArray *pdrgpcrInsert = popSplit->PdrgpcrInsert();
	pdrgpcrInsert->AddRef();

	CColRef *pcrAction = popSplit->PcrAction();
	CColRef *pcrCtid = popSplit->PcrCtid();
	CColRef *pcrSegmentId = popSplit->PcrSegmentId();
	CColRef *pcrTupleOid = popSplit->PcrTupleOid();

	// child of Split operator
	CExpression *pexprChild = (*pexpr)[0];
	CExpression *pexprProjList = (*pexpr)[1];
	pexprChild->AddRef();
	pexprProjList->AddRef();

	// create physical Split
	CExpression *pexprAlt = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CPhysicalSplit(mp, pdrgpcrDelete, pdrgpcrInsert, pcrCtid,
									pcrSegmentId, pcrAction, pcrTupleOid),
		pexprChild, pexprProjList);
	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF
