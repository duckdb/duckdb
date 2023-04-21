//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformDelete2DML.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformDelete2DML.h"

#include "gpos/base.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformDelete2DML::CXformDelete2DML
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformDelete2DML::CXformDelete2DML(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalDelete(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformDelete2DML::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformDelete2DML::Exfp(CExpressionHandle &	// exprhdl
) const
{
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformDelete2DML::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformDelete2DML::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
							CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalDelete *popDelete = CLogicalDelete::PopConvert(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components for alternative

	CTableDescriptor *ptabdesc = popDelete->Ptabdesc();
	ptabdesc->AddRef();

	CColRefArray *colref_array = popDelete->Pdrgpcr();
	colref_array->AddRef();

	CColRef *pcrCtid = popDelete->PcrCtid();

	CColRef *pcrSegmentId = popDelete->PcrSegmentId();

	// child of delete operator
	CExpression *pexprChild = (*pexpr)[0];
	pexprChild->AddRef();

	// create logical DML
	CExpression *pexprAlt = CXformUtils::PexprLogicalDMLOverProject(
		mp, pexprChild, CLogicalDML::EdmlDelete, ptabdesc, colref_array,
		pcrCtid, pcrSegmentId);

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF
