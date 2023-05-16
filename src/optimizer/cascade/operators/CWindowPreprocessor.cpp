//---------------------------------------------------------------------------
//	@filename:
//		CWindowPreprocessor.cpp
//
//	@doc:
//		Preprocessing routines of window functions
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CWindowPreprocessor.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CPredicateUtils.h"
#include "duckdb/optimizer/cascade/operators/ops.h"
#include "duckdb/optimizer/cascade/xforms/CXformUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CWindowPreprocessor::SplitPrjList
//
//	@doc:
//		Iterate over project elements and split them elements between
//		Distinct Aggs list, and Others list
//
//---------------------------------------------------------------------------
void
CWindowPreprocessor::SplitPrjList(
	CMemoryPool *mp, CExpression *pexprSeqPrj,
	CExpressionArray **
		ppdrgpexprDistinctAggsPrEl,	 // output: list of project elements with Distinct Aggs
	CExpressionArray **
		ppdrgpexprOtherPrEl,  // output: list of project elements with Other window functions
	COrderSpecArray **
		ppdrgposOther,	// output: array of order specs of window functions used in Others list
	CWindowFrameArray **
		ppdrgpwfOther  // output: array of frame specs of window functions used in Others list
)
{
	GPOS_ASSERT(NULL != pexprSeqPrj);
	GPOS_ASSERT(NULL != ppdrgpexprDistinctAggsPrEl);
	GPOS_ASSERT(NULL != ppdrgpexprOtherPrEl);
	GPOS_ASSERT(NULL != ppdrgposOther);
	GPOS_ASSERT(NULL != ppdrgpwfOther);

	CLogicalSequenceProject *popSeqPrj =
		CLogicalSequenceProject::PopConvert(pexprSeqPrj->Pop());
	CExpression *pexprPrjList = (*pexprSeqPrj)[1];

	COrderSpecArray *pdrgpos = popSeqPrj->Pdrgpos();
	BOOL fHasOrderSpecs = popSeqPrj->FHasOrderSpecs();

	CWindowFrameArray *pdrgpwf = popSeqPrj->Pdrgpwf();
	BOOL fHasFrameSpecs = popSeqPrj->FHasFrameSpecs();

	CExpressionArray *pdrgpexprDistinctAggsPrEl =
		GPOS_NEW(mp) CExpressionArray(mp);

	CExpressionArray *pdrgpexprOtherPrEl = GPOS_NEW(mp) CExpressionArray(mp);
	COrderSpecArray *pdrgposOther = GPOS_NEW(mp) COrderSpecArray(mp);
	CWindowFrameArray *pdrgpwfOther = GPOS_NEW(mp) CWindowFrameArray(mp);

	// iterate over project list and split project elements between
	// Distinct Aggs list, and Others list
	const ULONG arity = pexprPrjList->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprPrjEl = (*pexprPrjList)[ul];
		CExpression *pexprWinFunc = (*pexprPrjEl)[0];
		CScalarWindowFunc *popScWinFunc = CScalarWindowFunc::PopConvert(pexprWinFunc->Pop());
		CScalarProjectElement *popScPrjElem =
			CScalarProjectElement::PopConvert(pexprPrjEl->Pop());
		CColRef *pcrPrjElem = popScPrjElem->Pcr();

		if (popScWinFunc->IsDistinct() && popScWinFunc->FAgg())
		{
			CExpression *pexprAgg = CXformUtils::PexprWinFuncAgg2ScalarAgg(mp, pexprWinFunc);
			CExpression *pexprNewPrjElem =
				CUtils::PexprScalarProjectElement(mp, pcrPrjElem, pexprAgg);
			pdrgpexprDistinctAggsPrEl->Append(pexprNewPrjElem);
		}
		else
		{
			if (fHasOrderSpecs)
			{
				(*pdrgpos)[ul]->AddRef();
				pdrgposOther->Append((*pdrgpos)[ul]);
			}

			if (fHasFrameSpecs)
			{
				(*pdrgpwf)[ul]->AddRef();
				pdrgpwfOther->Append((*pdrgpwf)[ul]);
			}

			pexprWinFunc->AddRef();
			CExpression *pexprNewPrjElem =
				CUtils::PexprScalarProjectElement(mp, pcrPrjElem, pexprWinFunc);
			pdrgpexprOtherPrEl->Append(pexprNewPrjElem);
		}
	}

	*ppdrgpexprDistinctAggsPrEl = pdrgpexprDistinctAggsPrEl;
	*ppdrgpexprOtherPrEl = pdrgpexprOtherPrEl;
	*ppdrgposOther = pdrgposOther;
	*ppdrgpwfOther = pdrgpwfOther;
}


//---------------------------------------------------------------------------
//	@function:
//		CWindowPreprocessor::SplitSeqPrj
//
//	@doc:
//		Split SeqPrj expression into:
//		- A GbAgg expression containing distinct Aggs, and
//		- A SeqPrj expression containing all remaining window functions
//
//---------------------------------------------------------------------------
void
CWindowPreprocessor::SplitSeqPrj(
	CMemoryPool *mp, CExpression *pexprSeqPrj,
	CExpression *
		*ppexprGbAgg,  // output: GbAgg expression containing distinct Aggs
	CExpression **
		ppexprOutputSeqPrj	// output: SeqPrj expression containing all remaining window functions
)
{
	GPOS_ASSERT(NULL != pexprSeqPrj);
	GPOS_ASSERT(NULL != ppexprGbAgg);
	GPOS_ASSERT(NULL != ppexprOutputSeqPrj);

	// split project elements between Distinct Aggs list, and Others list
	CExpressionArray *pdrgpexprDistinctAggsPrEl = NULL;
	CExpressionArray *pdrgpexprOtherPrEl = NULL;
	COrderSpecArray *pdrgposOther = NULL;
	CWindowFrameArray *pdrgpwfOther = NULL;
	SplitPrjList(mp, pexprSeqPrj, &pdrgpexprDistinctAggsPrEl,
				 &pdrgpexprOtherPrEl, &pdrgposOther, &pdrgpwfOther);

	// check distribution spec of original SeqPrj and extract grouping columns
	// from window (PARTITION BY) clause
	CLogicalSequenceProject *popSeqPrj =
		CLogicalSequenceProject::PopConvert(pexprSeqPrj->Pop());
	CDistributionSpec *pds = popSeqPrj->Pds();
	CColRefArray *pdrgpcrGrpCols = NULL;
	if (CDistributionSpec::EdtHashed == pds->Edt())
	{
		CColRefSet *pcrs = CUtils::PcrsExtractColumns(
			mp, CDistributionSpecHashed::PdsConvert(pds)->Pdrgpexpr());
		pdrgpcrGrpCols = pcrs->Pdrgpcr(mp);
		pcrs->Release();
	}
	else
	{
		// no (PARTITION BY) clause
		pdrgpcrGrpCols = GPOS_NEW(mp) CColRefArray(mp);
	}

	CExpression *pexprSeqPrjChild = (*pexprSeqPrj)[0];
	pexprSeqPrjChild->AddRef();
	*ppexprGbAgg = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp)
			CLogicalGbAgg(mp, pdrgpcrGrpCols, COperator::EgbaggtypeGlobal),
		pexprSeqPrjChild,
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
								 pdrgpexprDistinctAggsPrEl));

	pexprSeqPrjChild->AddRef();
	if (0 == pdrgpexprOtherPrEl->Size())
	{
		// no remaining window functions after excluding distinct aggs,
		// reuse the original SeqPrj child in this case
		pdrgpexprOtherPrEl->Release();
		pdrgposOther->Release();
		pdrgpwfOther->Release();
		*ppexprOutputSeqPrj = pexprSeqPrjChild;

		return;
	}

	// create a new SeqPrj expression for remaining window functions
	pds->AddRef();
	*ppexprOutputSeqPrj = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp)
			CLogicalSequenceProject(mp, pds, pdrgposOther, pdrgpwfOther),
		pexprSeqPrjChild,
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
								 pdrgpexprOtherPrEl));
}


//---------------------------------------------------------------------------
//	@function:
//		CWindowPreprocessor::CreateCTE
//
//	@doc:
//		Create a CTE with two consumers using the child expression of
//		Sequence Project
//
//---------------------------------------------------------------------------
void
CWindowPreprocessor::CreateCTE(CMemoryPool *mp, CExpression *pexprSeqPrj,
							   CExpression **ppexprFirstConsumer,
							   CExpression **ppexprSecondConsumer)
{
	GPOS_ASSERT(NULL != pexprSeqPrj);
	GPOS_ASSERT(COperator::EopLogicalSequenceProject ==
				pexprSeqPrj->Pop()->Eopid());
	GPOS_ASSERT(NULL != ppexprFirstConsumer);
	GPOS_ASSERT(NULL != ppexprSecondConsumer);

	CExpression *pexprChild = (*pexprSeqPrj)[0];
	CColRefSet *pcrsChildOutput = pexprChild->DeriveOutputColumns();
	CColRefArray *pdrgpcrChildOutput = pcrsChildOutput->Pdrgpcr(mp);

	// create a CTE producer based on SeqPrj child expression
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	const ULONG ulCTEId = pcteinfo->next_id();
	CExpression *pexprCTEProd = CXformUtils::PexprAddCTEProducer(mp, ulCTEId, pdrgpcrChildOutput, pexprChild);
	CColRefArray *pdrgpcrProducerOutput =
		pexprCTEProd->DeriveOutputColumns()->Pdrgpcr(mp);

	// first consumer creates new output columns to be used later as input to GbAgg expression
	*ppexprFirstConsumer = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalCTEConsumer(
				mp, ulCTEId, CUtils::PdrgpcrCopy(mp, pdrgpcrProducerOutput)));
	pcteinfo->IncrementConsumers(ulCTEId);
	pdrgpcrProducerOutput->Release();

	// second consumer reuses the same output columns of SeqPrj child to be able to provide any requested columns upstream
	*ppexprSecondConsumer = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalCTEConsumer(mp, ulCTEId, pdrgpcrChildOutput));
	pcteinfo->IncrementConsumers(ulCTEId);
}


//---------------------------------------------------------------------------
//	@function:
//		CWindowPreprocessor::PdrgpcrGrpCols
//
//	@doc:
//		Extract grouping columns from given expression,
//		we expect expression to be either a GbAgg expression or a join
//		whose inner child is a GbAgg expression
//
//---------------------------------------------------------------------------
CColRefArray* CWindowPreprocessor::PdrgpcrGrpCols(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();

	if (COperator::EopLogicalGbAgg == pop->Eopid())
	{
		// passed expression is a Group By, return grouping columns
		return CLogicalGbAgg::PopConvert(pop)->Pdrgpcr();
	}

	if (CUtils::FLogicalJoin(pop))
	{
		// pass expression is a join, we expect a Group By on the inner side
		COperator *popInner = (*pexpr)[1]->Pop();
		if (COperator::EopLogicalGbAgg == popInner->Eopid())
		{
			return CLogicalGbAgg::PopConvert(popInner)->Pdrgpcr();
		}
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CWindowPreprocessor::PexprSeqPrj2Join
//
//	@doc:
//		Transform sequence project expression with distinct aggregates
//		into an inner join expression,
//			- the outer child of the join is a GbAgg expression that computes
//			distinct aggs,
//			- the inner child of the join is a SeqPrj expression that computes
//			remaining window functions
//
//		we use a CTE to compute the input to both join children, while maintaining
//		all column references upstream by reusing the same computed columns in the
//		original SeqPrj expression
//
//---------------------------------------------------------------------------
CExpression *
CWindowPreprocessor::PexprSeqPrj2Join(CMemoryPool *mp, CExpression *pexprSeqPrj)
{
	GPOS_ASSERT(NULL != pexprSeqPrj);
	GPOS_ASSERT(COperator::EopLogicalSequenceProject ==
				pexprSeqPrj->Pop()->Eopid());
	GPOS_ASSERT(0 < (*pexprSeqPrj)[1]->DeriveTotalDistinctAggs());

	// split SeqPrj expression into a GbAgg expression (for distinct Aggs), and
	// another SeqPrj expression (for remaining window functions)
	CExpression *pexprGbAgg = NULL;
	CExpression *pexprWindow = NULL;
	SplitSeqPrj(mp, pexprSeqPrj, &pexprGbAgg, &pexprWindow);

	// create CTE using SeqPrj child expression
	CExpression *pexprGbAggConsumer = NULL;
	CExpression *pexprWindowConsumer = NULL;
	CreateCTE(mp, pexprSeqPrj, &pexprGbAggConsumer, &pexprWindowConsumer);

	// extract output columns of SeqPrj child expression
	CExpression *pexprChild = (*pexprSeqPrj)[0];
	CColRefArray *pdrgpcrChildOutput =
		pexprChild->DeriveOutputColumns()->Pdrgpcr(mp);

	// to match requested columns upstream, we have to re-use the same computed
	// columns that define the aggregates, we avoid recreating new columns during
	// expression copy by passing must_exist as false
	CColRefArray *pdrgpcrConsumerOutput =
		CLogicalCTEConsumer::PopConvert(pexprGbAggConsumer->Pop())->Pdrgpcr();
	UlongToColRefMap *colref_mapping =
		CUtils::PhmulcrMapping(mp, pdrgpcrChildOutput, pdrgpcrConsumerOutput);
	CExpression *pexprGbAggRemapped = pexprGbAgg->PexprCopyWithRemappedColumns(
		mp, colref_mapping, false /*must_exist*/);
	colref_mapping->Release();
	pdrgpcrChildOutput->Release();
	pexprGbAgg->Release();

	// finalize GbAgg expression by replacing its child with CTE consumer
	pexprGbAggRemapped->Pop()->AddRef();
	(*pexprGbAggRemapped)[1]->AddRef();
	CExpression *pexprGbAggWithConsumer =
		GPOS_NEW(mp) CExpression(mp, pexprGbAggRemapped->Pop(),
								 pexprGbAggConsumer, (*pexprGbAggRemapped)[1]);
	pexprGbAggRemapped->Release();

	// in case of multiple Distinct Aggs, we need to expand the GbAgg expression
	// into a join expression where leaves carry single Distinct Aggs
	CExpression *pexprJoinDQAs = CXformUtils::PexprGbAggOnCTEConsumer2Join(mp, pexprGbAggWithConsumer);
	pexprGbAggWithConsumer->Release();

	CExpression *pexprWindowFinal = NULL;
	if (COperator::EopLogicalSequenceProject == pexprWindow->Pop()->Eopid())
	{
		// create a new SeqPrj expression for remaining window functions,
		// and replace expression child withCTE consumer
		pexprWindow->Pop()->AddRef();
		(*pexprWindow)[1]->AddRef();
		pexprWindowFinal = GPOS_NEW(mp) CExpression(
			mp, pexprWindow->Pop(), pexprWindowConsumer, (*pexprWindow)[1]);
	}
	else
	{
		// no remaining window functions, simply reuse created CTE consumer
		pexprWindowFinal = pexprWindowConsumer;
	}
	pexprWindow->Release();

	// extract grouping columns from created join expression
	CColRefArray *pdrgpcrGrpCols = PdrgpcrGrpCols(pexprJoinDQAs);

	// create final join condition
	CExpression *pexprJoinCondition = NULL;

	if (NULL != pdrgpcrGrpCols && 0 < pdrgpcrGrpCols->Size())
	{
		// extract PARTITION BY columns from original SeqPrj expression
		CLogicalSequenceProject *popSeqPrj =
			CLogicalSequenceProject::PopConvert(pexprSeqPrj->Pop());
		CDistributionSpec *pds = popSeqPrj->Pds();
		CColRefSet *pcrs = CUtils::PcrsExtractColumns(
			mp, CDistributionSpecHashed::PdsConvert(pds)->Pdrgpexpr());
		CColRefArray *pdrgpcrPartitionBy = pcrs->Pdrgpcr(mp);
		pcrs->Release();
		GPOS_ASSERT(
			pdrgpcrGrpCols->Size() == pdrgpcrPartitionBy->Size() &&
			"Partition By columns in window function are not the same as grouping columns in created Aggs");

		// create a conjunction of INDF expressions comparing a GROUP BY column to a PARTITION BY column
		pexprJoinCondition = CPredicateUtils::PexprINDFConjunction(
			mp, pdrgpcrGrpCols, pdrgpcrPartitionBy);
		pdrgpcrPartitionBy->Release();
	}
	else
	{
		// no PARTITION BY, join condition is const True
		pexprJoinCondition = CUtils::PexprScalarConstBool(mp, true /*value*/);
	}

	// create a join between expanded DQAs and Window expressions
	CExpression *pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
		mp, pexprJoinDQAs, pexprWindowFinal, pexprJoinCondition);

	ULONG ulCTEId =
		CLogicalCTEConsumer::PopConvert(pexprGbAggConsumer->Pop())->UlCTEId();
	return GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CLogicalCTEAnchor(mp, ulCTEId), pexprJoin);
}


//---------------------------------------------------------------------------
//	@function:
//		CWindowPreprocessor::PexprPreprocess
//
//	@doc:
//		Main driver
//
//---------------------------------------------------------------------------
CExpression *
CWindowPreprocessor::PexprPreprocess(CMemoryPool *mp, CExpression *pexpr)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	if (COperator::EopLogicalSequenceProject == pop->Eopid() &&
		0 < (*pexpr)[1]->DeriveTotalDistinctAggs())
	{
		CExpression *pexprJoin = PexprSeqPrj2Join(mp, pexpr);

		// recursively process the resulting expression
		CExpression *pexprResult = PexprPreprocess(mp, pexprJoin);
		pexprJoin->Release();

		return pexprResult;
	}

	// recursively process child expressions
	const ULONG arity = pexpr->Arity();
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprPreprocess(mp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}