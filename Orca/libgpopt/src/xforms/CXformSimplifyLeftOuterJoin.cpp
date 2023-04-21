//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformSimplifyLeftOuterJoin.cpp
//
//	@doc:
//		Simplify Left Outer Join with constant false predicate
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformSimplifyLeftOuterJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/ops.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyLeftOuterJoin::CXformSimplifyLeftOuterJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSimplifyLeftOuterJoin::CXformSimplifyLeftOuterJoin(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalLeftOuterJoin(mp),
			  GPOS_NEW(mp)
				  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // right child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate tree
			  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyLeftOuterJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSimplifyLeftOuterJoin::Exfp(CExpressionHandle &exprhdl) const
{
	CExpression *pexprScalar = exprhdl.PexprScalarExactChild(2 /*child_index*/);
	if (NULL != pexprScalar && CUtils::FScalarConstFalse(pexprScalar))
	{
		// if LOJ predicate is False, we can replace inner child with empty table
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyLeftOuterJoin::Transform
//
//	@doc:
//		Actual transformation to simplify left outer join
//
//---------------------------------------------------------------------------
void
CXformSimplifyLeftOuterJoin::Transform(CXformContext *pxfctxt,
									   CXformResult *pxfres,
									   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprInner = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	pexprOuter->AddRef();
	pexprScalar->AddRef();
	CExpression *pexprResult = NULL;

	// inner child of LOJ can be replaced with empty table
	GPOS_ASSERT(CUtils::FScalarConstFalse(pexprScalar));

	// extract output columns of inner child
	CColRefArray *colref_array = pexprInner->DeriveOutputColumns()->Pdrgpcr(mp);

	// generate empty constant table with the same columns
	COperator *popCTG = GPOS_NEW(mp)
		CLogicalConstTableGet(mp, colref_array, GPOS_NEW(mp) IDatum2dArray(mp));
	pexprResult = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CLogicalLeftOuterJoin(mp), pexprOuter,
					GPOS_NEW(mp) CExpression(mp, popCTG), pexprScalar);

	pxfres->Add(pexprResult);
}

// EOF
