//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementDML.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementDML.h"

#include "gpos/base.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementDML::CXformImplementDML
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementDML::CXformImplementDML(CMemoryPool *mp)
	: CXformImplementation(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalDML(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementDML::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementDML::Exfp(CExpressionHandle &  // exprhdl
) const
{
	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementDML::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementDML::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
							  CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalDML *popDML = CLogicalDML::PopConvert(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components for alternative

	CLogicalDML::EDMLOperator edmlop = popDML->Edmlop();

	CTableDescriptor *ptabdesc = popDML->Ptabdesc();
	ptabdesc->AddRef();

	CColRefArray *pdrgpcrSource = popDML->PdrgpcrSource();
	pdrgpcrSource->AddRef();
	CBitSet *pbsModified = popDML->PbsModified();
	pbsModified->AddRef();

	CColRef *pcrAction = popDML->PcrAction();
	CColRef *pcrTableOid = popDML->PcrTableOid();
	CColRef *pcrCtid = popDML->PcrCtid();
	CColRef *pcrSegmentId = popDML->PcrSegmentId();
	CColRef *pcrTupleOid = popDML->PcrTupleOid();

	// child of DML operator
	CExpression *pexprChild = (*pexpr)[0];
	pexprChild->AddRef();

	// create physical DML
	CExpression *pexprAlt = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CPhysicalDML(mp, edmlop, ptabdesc, pdrgpcrSource,
								  pbsModified, pcrAction, pcrTableOid, pcrCtid,
								  pcrSegmentId, pcrTupleOid),
		pexprChild);
	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF
