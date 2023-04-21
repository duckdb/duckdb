//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformUnnestTVF.cpp
//
//	@doc:
//		Implementation of TVF unnesting xform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformUnnestTVF.h"

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformUnnestTVF::CXformUnnestTVF
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformUnnestTVF::CXformUnnestTVF(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalTVF(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternMultiTree(
						  mp))	// variable number of args, each is a deep tree
			  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUnnestTVF::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformUnnestTVF::Exfp(CExpressionHandle &exprhdl) const
{
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (exprhdl.DeriveHasSubquery(ul))
		{
			// xform is applicable if TVF argument is a subquery
			return CXform::ExfpHigh;
		}
	}

	return CXform::ExfpNone;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUnnestTVF::PdrgpcrSubqueries
//
//	@doc:
//		Return array of subquery column references in CTE consumer output
//		after mapping to consumer output
//
//---------------------------------------------------------------------------
CColRefArray *
CXformUnnestTVF::PdrgpcrSubqueries(CMemoryPool *mp, CExpression *pexprCTEProd,
								   CExpression *pexprCTECons)
{
	CExpression *pexprProject = (*pexprCTEProd)[0];
	GPOS_ASSERT(COperator::EopLogicalProject == pexprProject->Pop()->Eopid());

	CColRefArray *pdrgpcrProdOutput =
		pexprCTEProd->DeriveOutputColumns()->Pdrgpcr(mp);
	CColRefArray *pdrgpcrConsOutput =
		pexprCTECons->DeriveOutputColumns()->Pdrgpcr(mp);
	GPOS_ASSERT(pdrgpcrProdOutput->Size() == pdrgpcrConsOutput->Size());

	CColRefArray *colref_array = GPOS_NEW(mp) CColRefArray(mp);
	const ULONG ulPrjElems = (*pexprProject)[1]->Arity();
	for (ULONG ulOuter = 0; ulOuter < ulPrjElems; ulOuter++)
	{
		CExpression *pexprPrjElem = (*(*pexprProject)[1])[ulOuter];
		if ((*pexprPrjElem)[0]->DeriveHasSubquery())
		{
			CColRef *pcrProducer =
				CScalarProjectElement::PopConvert(pexprPrjElem->Pop())->Pcr();
			CColRef *pcrConsumer = CUtils::PcrMap(
				pcrProducer, pdrgpcrProdOutput, pdrgpcrConsOutput);
			GPOS_ASSERT(NULL != pcrConsumer);

			colref_array->Append(pcrConsumer);
		}
	}

	pdrgpcrProdOutput->Release();
	pdrgpcrConsOutput->Release();

	return colref_array;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUnnestTVF::PexprProjectSubqueries
//
//	@doc:
//		Collect subquery arguments and return a Project expression with
//		collected subqueries in project list
//
//---------------------------------------------------------------------------
CExpression *
CXformUnnestTVF::PexprProjectSubqueries(CMemoryPool *mp, CExpression *pexprTVF)
{
	GPOS_ASSERT(COperator::EopLogicalTVF == pexprTVF->Pop()->Eopid());

	// collect subquery arguments
	CExpressionArray *pdrgpexprSubqueries = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG arity = pexprTVF->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprScalarChild = (*pexprTVF)[ul];
		if (pexprScalarChild->DeriveHasSubquery())
		{
			pexprScalarChild->AddRef();
			pdrgpexprSubqueries->Append(pexprScalarChild);
		}
	}
	GPOS_ASSERT(0 < pdrgpexprSubqueries->Size());

	CExpression *pexprCTG = CUtils::PexprLogicalCTGDummy(mp);
	CExpression *pexprProject =
		CUtils::PexprAddProjection(mp, pexprCTG, pdrgpexprSubqueries);
	pdrgpexprSubqueries->Release();

	return pexprProject;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUnnestTVF::Transform
//
//	@doc:
//		Actual transformation
//		for queries of the form 'SELECT * FROM func(1, (select 5))'
//		we generate a correlated CTE expression to execute subquery args
//		in different subplans, the resulting expression looks like this
//
//		+--CLogicalCTEAnchor (0)
//		   +--CLogicalLeftOuterCorrelatedApply
//			  |--CLogicalTVF (func) Columns: ["a" (0), "b" (1)]
//			  |  |--CScalarConst (1)     <-- constant arg
//			  |  +--CScalarIdent "ColRef_0005" (11)    <-- subquery arg replaced by column
//			  |--CLogicalCTEConsumer (0), Columns: ["ColRef_0005" (11)]
//			  +--CScalarConst (1)
//
//		where CTE(0) is a Project expression on subquery args
//
//
//---------------------------------------------------------------------------
void
CXformUnnestTVF::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// create a project expression on subquery arguments
	CExpression *pexprProject = PexprProjectSubqueries(mp, pexpr);

	// create a CTE producer on top of the project
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	const ULONG ulCTEId = pcteinfo->next_id();

	// construct CTE producer output from subquery columns
	CColRefArray *pdrgpcrOutput = GPOS_NEW(mp) CColRefArray(mp);
	const ULONG ulPrjElems = (*pexprProject)[1]->Arity();
	for (ULONG ulOuter = 0; ulOuter < ulPrjElems; ulOuter++)
	{
		CExpression *pexprPrjElem = (*(*pexprProject)[1])[ulOuter];
		if ((*pexprPrjElem)[0]->DeriveHasSubquery())
		{
			CColRef *pcrSubq =
				CScalarProjectElement::PopConvert(pexprPrjElem->Pop())->Pcr();
			pdrgpcrOutput->Append(pcrSubq);
		}
	}

	CExpression *pexprCTEProd = CXformUtils::PexprAddCTEProducer(
		mp, ulCTEId, pdrgpcrOutput, pexprProject);
	pdrgpcrOutput->Release();
	pexprProject->Release();

	// create CTE consumer
	CColRefArray *pdrgpcrProducerOutput =
		pexprCTEProd->DeriveOutputColumns()->Pdrgpcr(mp);
	CColRefArray *pdrgpcrConsumerOutput =
		CUtils::PdrgpcrCopy(mp, pdrgpcrProducerOutput);
	CLogicalCTEConsumer *popConsumer =
		GPOS_NEW(mp) CLogicalCTEConsumer(mp, ulCTEId, pdrgpcrConsumerOutput);
	CExpression *pexprCTECons = GPOS_NEW(mp) CExpression(mp, popConsumer);
	pcteinfo->IncrementConsumers(ulCTEId);
	pdrgpcrProducerOutput->Release();

	// find columns corresponding to subqueries in consumer's output
	CColRefArray *pdrgpcrSubqueries =
		PdrgpcrSubqueries(mp, pexprCTEProd, pexprCTECons);

	// create new function arguments by replacing subqueries with columns in CTE consumer output
	CExpressionArray *pdrgpexprNewArgs = GPOS_NEW(mp) CExpressionArray(mp);
	ULONG ulIndex = 0;
	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprScalarChild = (*pexpr)[ul];
		if (pexprScalarChild->DeriveHasSubquery())
		{
			CColRef *colref = (*pdrgpcrSubqueries)[ulIndex];
			pdrgpexprNewArgs->Append(CUtils::PexprScalarIdent(mp, colref));
			ulIndex++;
		}
		else
		{
			(*pexpr)[ul]->AddRef();
			pdrgpexprNewArgs->Append((*pexpr)[ul]);
		}
	}

	// finally, create correlated apply expression
	CLogicalTVF *popTVF = CLogicalTVF::PopConvert(pexpr->Pop());
	popTVF->AddRef();
	CExpression *pexprCorrApply =
		CUtils::PexprLogicalApply<CLogicalLeftOuterCorrelatedApply>(
			mp, GPOS_NEW(mp) CExpression(mp, popTVF, pdrgpexprNewArgs),
			pexprCTECons, pdrgpcrSubqueries, COperator::EopScalarSubquery,
			CPredicateUtils::PexprConjunction(
				mp, NULL /*pdrgpexpr*/)	 // scalar expression is const True
		);

	CExpression *pexprAlt = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalCTEAnchor(mp, ulCTEId), pexprCorrApply);

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}


// EOF
