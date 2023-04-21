//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformInsert2DML.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformInsert2DML.h"

#include "gpos/base.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformInsert2DML::CXformInsert2DML
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformInsert2DML::CXformInsert2DML(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalInsert(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformInsert2DML::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformInsert2DML::Exfp(CExpressionHandle &	// exprhdl
) const
{
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformInsert2DML::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformInsert2DML::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
							CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalInsert *popInsert = CLogicalInsert::PopConvert(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components for alternative

	CTableDescriptor *ptabdesc = popInsert->Ptabdesc();
	ptabdesc->AddRef();

	CColRefArray *pdrgpcrSource = popInsert->PdrgpcrSource();
	pdrgpcrSource->AddRef();

	// child of insert operator
	CExpression *pexprChild = (*pexpr)[0];
	pexprChild->AddRef();

	// create logical DML
	CExpression *pexprAlt = CXformUtils::PexprLogicalDMLOverProject(
		mp, pexprChild, CLogicalDML::EdmlInsert, ptabdesc, pdrgpcrSource,
		NULL,  //pcrCtid
		NULL   //pcrSegmentId
	);

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF
