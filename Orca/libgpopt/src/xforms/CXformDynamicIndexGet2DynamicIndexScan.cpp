//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformDynamicIndexGet2DynamicIndexScan.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformDynamicIndexGet2DynamicIndexScan.h"

#include "gpos/base.h"

#include "gpopt/metadata/CPartConstraint.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformDynamicIndexGet2DynamicIndexScan::CXformDynamicIndexGet2DynamicIndexScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformDynamicIndexGet2DynamicIndexScan::CXformDynamicIndexGet2DynamicIndexScan(
	CMemoryPool *mp)
	: CXformImplementation(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalDynamicIndexGet(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp))  // index lookup predicate
			  ))
{
}

CXform::EXformPromise
CXformDynamicIndexGet2DynamicIndexScan::Exfp(CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasSubquery(0))
	{
		return CXform::ExfpNone;
	}
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformDynamicIndexGet2DynamicIndexScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformDynamicIndexGet2DynamicIndexScan::Transform(CXformContext *pxfctxt,
												  CXformResult *pxfres,
												  CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalDynamicIndexGet *popIndexGet =
		CLogicalDynamicIndexGet::PopConvert(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();

	// create/extract components for alternative
	CName *pname = GPOS_NEW(mp) CName(mp, popIndexGet->Name());

	// extract components
	CExpression *pexprIndexCond = (*pexpr)[0];
	pexprIndexCond->AddRef();

	CTableDescriptor *ptabdesc = popIndexGet->Ptabdesc();
	ptabdesc->AddRef();

	CIndexDescriptor *pindexdesc = popIndexGet->Pindexdesc();
	pindexdesc->AddRef();

	CColRefArray *pdrgpcrOutput = popIndexGet->PdrgpcrOutput();
	GPOS_ASSERT(NULL != pdrgpcrOutput);
	pdrgpcrOutput->AddRef();

	CColRef2dArray *pdrgpdrgpcrPart = popIndexGet->PdrgpdrgpcrPart();
	pdrgpdrgpcrPart->AddRef();

	CPartConstraint *ppartcnstr = popIndexGet->Ppartcnstr();
	ppartcnstr->AddRef();

	CPartConstraint *ppartcnstrRel = popIndexGet->PpartcnstrRel();
	ppartcnstrRel->AddRef();

	COrderSpec *pos = popIndexGet->Pos();
	pos->AddRef();

	// create alternative expression
	CExpression *pexprAlt = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CPhysicalDynamicIndexScan(
			mp, popIndexGet->IsPartial(), pindexdesc, ptabdesc,
			pexpr->Pop()->UlOpId(), pname, pdrgpcrOutput, popIndexGet->ScanId(),
			pdrgpdrgpcrPart, popIndexGet->UlSecondaryScanId(), ppartcnstr,
			ppartcnstrRel, pos),
		pexprIndexCond);
	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}


// EOF
