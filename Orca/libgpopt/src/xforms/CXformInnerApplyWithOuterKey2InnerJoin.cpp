//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformInnerApplyWithOuterKey2InnerJoin.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformInnerApplyWithOuterKey2InnerJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CKeyCollection.h"
#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerApplyWithOuterKey2InnerJoin::CXformInnerApplyWithOuterKey2InnerJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformInnerApplyWithOuterKey2InnerJoin::CXformInnerApplyWithOuterKey2InnerJoin(
	CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalInnerApply(mp),
			  GPOS_NEW(mp)
				  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
				  GPOS_NEW(mp) CExpression(
					  mp,
					  GPOS_NEW(mp) CPatternTree(mp)),  // relational child of Gb
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp)
							  CPatternLeaf(mp))	 // scalar project list of Gb
				  ),							 // right child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp))  // Apply predicate
			  ))
{
}



//---------------------------------------------------------------------------
//	@function:
//		CXformInnerApplyWithOuterKey2InnerJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformInnerApplyWithOuterKey2InnerJoin::Exfp(CExpressionHandle &exprhdl) const
{
	// check if outer child has key and inner child has outer references
	if (NULL == exprhdl.DeriveKeyCollection(0) ||
		0 == exprhdl.DeriveOuterReferences(1)->Size())
	{
		return ExfpNone;
	}

	return ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerApplyWithOuterKey2InnerJoin::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformInnerApplyWithOuterKey2InnerJoin::Transform(CXformContext *pxfctxt,
												  CXformResult *pxfres,
												  CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprGb = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	if (0 < CLogicalGbAgg::PopConvert(pexprGb->Pop())->Pdrgpcr()->Size())
	{
		// xform is not applicable if inner Gb has grouping columns
		return;
	}

	if (CUtils::FHasSubqueryOrApply((*pexprGb)[0]))
	{
		// Subquery/Apply must be unnested before reaching here
		return;
	}

	// decorrelate Gb's relational child
	(*pexprGb)[0]->ResetDerivedProperties();
	CExpression *pexprInner = NULL;
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	if (!CDecorrelator::FProcess(mp, (*pexprGb)[0], false /*fEqualityOnly*/,
								 &pexprInner, pdrgpexpr,
								 pexprOuter->DeriveOutputColumns()))
	{
		pdrgpexpr->Release();
		return;
	}

	GPOS_ASSERT(NULL != pexprInner);
	CExpression *pexprPredicate =
		CPredicateUtils::PexprConjunction(mp, pdrgpexpr);

	// join outer child with Gb's decorrelated child
	pexprOuter->AddRef();
	CExpression *pexprInnerJoin =
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
								 pexprOuter, pexprInner, pexprPredicate);

	// create grouping columns from the output of outer child
	CColRefArray *pdrgpcrKey = NULL;
	CColRefArray *colref_array =
		CUtils::PdrgpcrGroupingKey(mp, pexprOuter, &pdrgpcrKey);
	pdrgpcrKey->Release();	// key is not used here

	CLogicalGbAgg *popGbAgg = GPOS_NEW(mp) CLogicalGbAgg(
		mp, colref_array, COperator::EgbaggtypeGlobal /*egbaggtype*/);
	CExpression *pexprPrjList = (*pexprGb)[1];
	pexprPrjList->AddRef();
	CExpression *pexprNewGb =
		GPOS_NEW(mp) CExpression(mp, popGbAgg, pexprInnerJoin, pexprPrjList);

	// add Apply predicate in a top Select node
	pexprScalar->AddRef();
	CExpression *pexprSelect =
		CUtils::PexprLogicalSelect(mp, pexprNewGb, pexprScalar);

	pxfres->Add(pexprSelect);
}


// EOF
