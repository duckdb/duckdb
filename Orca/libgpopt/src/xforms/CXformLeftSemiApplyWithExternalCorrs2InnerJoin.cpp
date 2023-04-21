//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformLeftSemiApplyWithExternalCorrs2InnerJoin.cpp
//
//	@doc:
//		Implementation of transform;
//		external correlations are correlations in the inner child of LSA
//		that use columns not defined by the outer child of LSA
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftSemiApplyWithExternalCorrs2InnerJoin.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"


using namespace gpopt;


//
//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiApplyWithExternalCorrs2InnerJoin::FSplitCorrelations
//
//	@doc:
//		Helper for splitting correlations into external and residual
//
//---------------------------------------------------------------------------
BOOL
CXformLeftSemiApplyWithExternalCorrs2InnerJoin::FSplitCorrelations(
	CMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprInner,
	CExpressionArray *pdrgpexprAllCorr, CExpressionArray **ppdrgpexprExternal,
	CExpressionArray **ppdrgpexprResidual, CColRefSet **ppcrsInnerUsed)
{
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);
	GPOS_ASSERT(NULL != pdrgpexprAllCorr);
	GPOS_ASSERT(NULL != ppdrgpexprExternal);
	GPOS_ASSERT(NULL != ppdrgpexprResidual);
	GPOS_ASSERT(NULL != ppcrsInnerUsed);

	// collect output columns of all children
	CColRefSet *pcrsOuterOuput = pexprOuter->DeriveOutputColumns();
	CColRefSet *pcrsInnerOuput = pexprInner->DeriveOutputColumns();
	CColRefSet *pcrsChildren = GPOS_NEW(mp) CColRefSet(mp, *pcrsOuterOuput);
	pcrsChildren->Union(pcrsInnerOuput);

	// split correlations into external correlations and residual correlations
	CExpressionArray *pdrgpexprExternal = GPOS_NEW(mp) CExpressionArray(mp);
	CExpressionArray *pdrgpexprResidual = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG ulCorrs = pdrgpexprAllCorr->Size();
	CColRefSet *pcrsUsed = GPOS_NEW(mp)
		CColRefSet(mp);	 // set of inner columns used in external correlations
	BOOL fSuccess = true;
	for (ULONG ul = 0; fSuccess && ul < ulCorrs; ul++)
	{
		CExpression *pexprCurrent = (*pdrgpexprAllCorr)[ul];
		CColRefSet *pcrsCurrent =
			GPOS_NEW(mp) CColRefSet(mp, *pexprCurrent->DeriveUsedColumns());
		if (pcrsCurrent->IsDisjoint(pcrsOuterOuput) ||
			pcrsCurrent->IsDisjoint(pcrsInnerOuput))
		{
			// add current correlation to external correlation
			pexprCurrent->AddRef();
			pdrgpexprExternal->Append(pexprCurrent);

			// filter used columns from external columns
			pcrsCurrent->Intersection(pcrsInnerOuput);
			pcrsUsed->Union(pcrsCurrent);
		}
		else if (pcrsChildren->ContainsAll(pcrsCurrent))
		{
			// add current correlation to regular correlations
			pexprCurrent->AddRef();
			pdrgpexprResidual->Append(pexprCurrent);
		}
		else
		{
			// a mixed correlation (containing both outer columns and external columns)
			fSuccess = false;
		}
		pcrsCurrent->Release();
	}
	pcrsChildren->Release();

	if (!fSuccess || 0 == pdrgpexprExternal->Size())
	{
		// failed to find external correlations
		pcrsUsed->Release();
		pdrgpexprExternal->Release();
		pdrgpexprResidual->Release();

		return false;
	}

	*ppcrsInnerUsed = pcrsUsed;
	*ppdrgpexprExternal = pdrgpexprExternal;
	*ppdrgpexprResidual = pdrgpexprResidual;

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiApplyWithExternalCorrs2InnerJoin::FDecorrelate
//
//	@doc:
//		Helper for collecting correlations from LSA expression
//
//---------------------------------------------------------------------------
BOOL
CXformLeftSemiApplyWithExternalCorrs2InnerJoin::FDecorrelate(
	CMemoryPool *mp, CExpression *pexpr, CExpression **ppexprInnerNew,
	CExpressionArray **ppdrgpexprCorr)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(COperator::EopLogicalLeftSemiApply == pexpr->Pop()->Eopid() ||
				COperator::EopLogicalLeftSemiApplyIn == pexpr->Pop()->Eopid());

	GPOS_ASSERT(NULL != ppexprInnerNew);
	GPOS_ASSERT(NULL != ppdrgpexprCorr);

	// extract children
	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprInner = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	// collect all correlations from inner child
	pexprInner->ResetDerivedProperties();
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	if (!CDecorrelator::FProcess(mp, pexprInner, true /* fEqualityOnly */,
								 ppexprInnerNew, pdrgpexpr,
								 pexprOuter->DeriveOutputColumns()))
	{
		// decorrelation failed
		pdrgpexpr->Release();
		CRefCount::SafeRelease(*ppexprInnerNew);

		return false;
	}

	// add all original scalar conjuncts to correlations
	CExpressionArray *pdrgpexprOriginal =
		CPredicateUtils::PdrgpexprConjuncts(mp, pexprScalar);
	CUtils::AddRefAppend<CExpression>(pdrgpexpr, pdrgpexprOriginal);
	pdrgpexprOriginal->Release();

	*ppdrgpexprCorr = pdrgpexpr;

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiApplyWithExternalCorrs2InnerJoin::PexprDecorrelate
//
//	@doc:
//		Transform LSA into a Gb on top of an InnerJoin, where
//		the grouping columns of Gb are the key of LSA's outer child
//		in addition to columns from LSA's inner child that are used
//		in external correlation;
//		this transformation exposes LSA's inner child columns so that
//		correlations can be pulled-up
//
//		Example:
//
//		Input:
//			LS-Apply
//				|----Get(B)
//				+----Select (C.c = A.a)
//						+----Get(C)
//
//		Output:
//			Select (C.c = A.a)
//				+--Gb(B.key, c)
//					+--InnerJoin
//						|--Get(B)
//						+--Get(C)
//
//---------------------------------------------------------------------------
CExpression *
CXformLeftSemiApplyWithExternalCorrs2InnerJoin::PexprDecorrelate(
	CMemoryPool *mp, CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(COperator::EopLogicalLeftSemiApply == pexpr->Pop()->Eopid() ||
				COperator::EopLogicalLeftSemiApplyIn == pexpr->Pop()->Eopid());

	CExpressionHandle exprhdl(mp);
	exprhdl.Attach(pexpr);

	if (NULL == exprhdl.DeriveKeyCollection(0 /*child_index*/) ||
		!CUtils::FInnerUsesExternalCols(exprhdl))
	{
		// outer child must have a key and inner child must have external correlations
		return NULL;
	}

	CExpression *pexprInnerNew = NULL;
	CExpressionArray *pdrgpexprAllCorr = NULL;
	if (!FDecorrelate(mp, pexpr, &pexprInnerNew, &pdrgpexprAllCorr))
	{
		// decorrelation failed
		return NULL;
	}
	GPOS_ASSERT(NULL != pdrgpexprAllCorr);

	CExpressionArray *pdrgpexprExternal = NULL;
	CExpressionArray *pdrgpexprResidual = NULL;
	CColRefSet *pcrsInnerUsed = NULL;
	if (!FSplitCorrelations(mp, (*pexpr)[0], pexprInnerNew, pdrgpexprAllCorr,
							&pdrgpexprExternal, &pdrgpexprResidual,
							&pcrsInnerUsed))
	{
		// splitting correlations failed
		pdrgpexprAllCorr->Release();
		CRefCount::SafeRelease(pexprInnerNew);

		return NULL;
	}
	GPOS_ASSERT(pdrgpexprExternal->Size() + pdrgpexprResidual->Size() ==
				pdrgpexprAllCorr->Size());

	pdrgpexprAllCorr->Release();

	// create an inner join between outer child and decorrelated inner child
	CExpression *pexprOuter = (*pexpr)[0];
	pexprOuter->AddRef();
	CExpression *pexprInnerJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
		mp, pexprOuter, pexprInnerNew,
		CPredicateUtils::PexprConjunction(mp, pdrgpexprResidual));

	// add a group by on outer columns + inner columns appearing in external correlations
	CColRefArray *pdrgpcrUsed = pcrsInnerUsed->Pdrgpcr(mp);
	pcrsInnerUsed->Release();

	CColRefArray *pdrgpcrKey = NULL;
	CColRefArray *pdrgpcrGrpCols =
		CUtils::PdrgpcrGroupingKey(mp, pexprOuter, &pdrgpcrKey);
	pdrgpcrKey->Release();	// key is not used here

	pdrgpcrGrpCols->AppendArray(pdrgpcrUsed);
	pdrgpcrUsed->Release();
	CExpression *pexprGb = CUtils::PexprLogicalGbAggGlobal(
		mp, pdrgpcrGrpCols, pexprInnerJoin,
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp)));

	// add a top filter for external correlations
	return CUtils::PexprLogicalSelect(
		mp, pexprGb, CPredicateUtils::PexprConjunction(mp, pdrgpexprExternal));
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiApplyWithExternalCorrs2InnerJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftSemiApplyWithExternalCorrs2InnerJoin::Exfp(
	CExpressionHandle &exprhdl) const
{
	// expression must have outer references with external correlations
	if (exprhdl.HasOuterRefs(1 /*child_index*/) &&
		CUtils::FInnerUsesExternalCols(exprhdl))
	{
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiApplyWithExternalCorrs2InnerJoin::Transform
//
//	@doc:
//		Actual transformation
//
//
//---------------------------------------------------------------------------
void
CXformLeftSemiApplyWithExternalCorrs2InnerJoin::Transform(
	CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();
	CExpression *pexprResult = PexprDecorrelate(mp, pexpr);
	if (NULL != pexprResult)
	{
		pxfres->Add(pexprResult);
	}
}

// EOF
