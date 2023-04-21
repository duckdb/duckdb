//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformUnion2UnionAll.cpp
//
//	@doc:
//		Implementation of the transformation that takes a logical union and
//		coverts it into an aggregate over a logical union all
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformUnion2UnionAll.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformUnion2UnionAll::CXformUnion2UnionAll
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformUnion2UnionAll::CXformUnion2UnionAll(CMemoryPool *mp)
	:  // pattern
	  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalUnion(mp),
		  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternMultiLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUnion2UnionAll::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformUnion2UnionAll::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
								CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CLogicalUnion *popUnion = CLogicalUnion::PopConvert(pexpr->Pop());
	CColRefArray *pdrgpcrOutput = popUnion->PdrgpcrOutput();
	CColRef2dArray *pdrgpdrgpcrInput = popUnion->PdrgpdrgpcrInput();

	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG arity = pexpr->Arity();

	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);
	}

	pdrgpcrOutput->AddRef();
	pdrgpdrgpcrInput->AddRef();

	// assemble new logical operator
	CExpression *pexprUnionAll = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalUnionAll(mp, pdrgpcrOutput, pdrgpdrgpcrInput),
		pdrgpexpr);

	pdrgpcrOutput->AddRef();

	CExpression *pexprProjList =
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
								 GPOS_NEW(mp) CExpressionArray(mp));

	CExpression *pexprAgg = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CLogicalGbAgg(mp, pdrgpcrOutput,
								   COperator::EgbaggtypeGlobal /*egbaggtype*/),
		pexprUnionAll, pexprProjList);

	// add alternative to results
	pxfres->Add(pexprAgg);
}

// EOF
