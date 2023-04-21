//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementCTEConsumer.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementCTEConsumer.h"

#include "gpos/base.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementCTEConsumer::CXformImplementCTEConsumer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementCTEConsumer::CXformImplementCTEConsumer(CMemoryPool *mp)
	: CXformImplementation(
		  // pattern
		  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalCTEConsumer(mp)))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementCTEConsumer::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementCTEConsumer::Exfp(CExpressionHandle &  // exprhdl
) const
{
	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementCTEConsumer::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementCTEConsumer::Transform(CXformContext *pxfctxt,
									  CXformResult *pxfres,
									  CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalCTEConsumer *popCTEConsumer =
		CLogicalCTEConsumer::PopConvert(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components for alternative
	ULONG id = popCTEConsumer->UlCTEId();

	CColRefArray *colref_array = popCTEConsumer->Pdrgpcr();
	colref_array->AddRef();

	UlongToColRefMap *colref_mapping = popCTEConsumer->Phmulcr();
	GPOS_ASSERT(NULL != colref_mapping);
	colref_mapping->AddRef();

	// create physical CTE Consumer
	CExpression *pexprAlt =
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPhysicalCTEConsumer(
										 mp, id, colref_array, colref_mapping));

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF
