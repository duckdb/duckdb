//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementTVF.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementTVF.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementTVF::CXformImplementTVF
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementTVF::CXformImplementTVF(CMemoryPool *mp)
	: CXformImplementation(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalTVF(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternMultiLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementTVF::CXformImplementTVF
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementTVF::CXformImplementTVF(CExpression *pexpr)
	: CXformImplementation(pexpr)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementTVF::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementTVF::Exfp(CExpressionHandle &exprhdl) const
{
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (exprhdl.DeriveHasSubquery(ul))
		{
			// xform is inapplicable if TVF argument is a subquery
			return CXform::ExfpNone;
		}
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementTVF::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementTVF::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
							  CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalTVF *popTVF = CLogicalTVF::PopConvert(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();

	// create/extract components for alternative
	IMDId *mdid_func = popTVF->FuncMdId();
	mdid_func->AddRef();

	IMDId *mdid_return_type = popTVF->ReturnTypeMdId();
	mdid_return_type->AddRef();

	CWStringConst *str =
		GPOS_NEW(mp) CWStringConst(popTVF->Pstr()->GetBuffer());

	CColumnDescriptorArray *pdrgpcoldesc = popTVF->Pdrgpcoldesc();
	pdrgpcoldesc->AddRef();

	CColRefArray *pdrgpcrOutput = popTVF->PdrgpcrOutput();
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(pdrgpcrOutput);

	CExpressionArray *pdrgpexpr = pexpr->PdrgPexpr();

	CPhysicalTVF *pphTVF = GPOS_NEW(mp)
		CPhysicalTVF(mp, mdid_func, mdid_return_type, str, pdrgpcoldesc, pcrs);

	CExpression *pexprAlt = NULL;
	// create alternative expression
	if (NULL == pdrgpexpr || 0 == pdrgpexpr->Size())
	{
		pexprAlt = GPOS_NEW(mp) CExpression(mp, pphTVF);
	}
	else
	{
		pdrgpexpr->AddRef();
		pexprAlt = GPOS_NEW(mp) CExpression(mp, pphTVF, pdrgpexpr);
	}

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}


// EOF
