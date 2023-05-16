//---------------------------------------------------------------------------
//	@filename:
//		CXformUtils.cpp
//
//	@doc:
//		Implementation of xform utilities
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformUtils.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/error/CMessage.h"
#include "duckdb/optimizer/cascade/error/CMessageRepository.h"
#include "duckdb/optimizer/cascade/memory/CAutoMemoryPool.h"
#include "duckdb/optimizer/cascade/base/CConstraintConjunction.h"
#include "duckdb/optimizer/cascade/base/CConstraintNegation.h"
#include "duckdb/optimizer/cascade/base/CKeyCollection.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/engine/CHint.h"
#include "duckdb/optimizer/cascade/exception.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/xforms/CDecorrelator.h"
#include "duckdb/optimizer/cascade/xforms/CXformExploration.h"
#include "duckdb/optimizer/cascade/md/CMDIdGPDB.h"
#include "duckdb/optimizer/cascade/md/IMDCheckConstraint.h"
#include "duckdb/optimizer/cascade/md/IMDScalarOp.h"
#include "duckdb/optimizer/cascade/md/IMDTrigger.h"
#include "duckdb/optimizer/cascade/md/IMDTypeInt8.h"
#include "duckdb/optimizer/cascade/md/IMDTypeOid.h"
#include "duckdb/optimizer/cascade/statistics/CFilterStatsProcessor.h"
#include "duckdb/optimizer/cascade/operators/CScalarWindowFunc.h"

using namespace gpopt;

// predicates less selective than this threshold
// (selectivity is greater than this number) lead to
// disqualification of a btree index on an AO table
#define AO_TABLE_BTREE_INDEX_SELECTIVITY_THRESHOLD 0.10

//---------------------------------------------------------------------------
//      @function:
//              CXformUtils::FXformInArray
//
//      @doc:
//          Check if given xform id is in the given array of xforms
//
//---------------------------------------------------------------------------
BOOL CXformUtils::FXformInArray(CXform::EXformId exfid, CXform::EXformId rgXforms[], ULONG ulXforms)
{
	for (ULONG ul = 0; ul < ulXforms; ul++)
	{
		if (rgXforms[ul] == exfid)
		{
			return true;
		}
	}
	return false;
}

//---------------------------------------------------------------------------
//      @function:
//              CXformUtils::FDeriveStatsBeforeXform
//
//      @doc:
//          Return true if stats derivation is needed for this xform
//
//---------------------------------------------------------------------------
BOOL CXformUtils::FDeriveStatsBeforeXform(CXform *pxform)
{
	GPOS_ASSERT(NULL != pxform);
	return pxform->FExploration() && CXformExploration::Pxformexp(pxform)->FNeedsStats();
}

//---------------------------------------------------------------------------
//      @function:
//              CXformUtils::FSubqueryDecorrelation
//
//      @doc:
//          Check if xform is a subquery decorrelation xform
//
//---------------------------------------------------------------------------
BOOL CXformUtils::FSubqueryDecorrelation(CXform *pxform)
{
	GPOS_ASSERT(NULL != pxform);
	return pxform->FExploration() && CXformExploration::Pxformexp(pxform)->FApplyDecorrelating();
}

//---------------------------------------------------------------------------
//      @function:
//              CXformUtils::FSubqueryUnnesting
//
//      @doc:
//          Check if xform is a subquery unnesting xform
//
//---------------------------------------------------------------------------
BOOL CXformUtils::FSubqueryUnnesting(CXform *pxform)
{
	GPOS_ASSERT(NULL != pxform);
	return pxform->FExploration() && CXformExploration::Pxformexp(pxform)->FSubqueryUnnesting();
}

//---------------------------------------------------------------------------
//      @function:
//              CXformUtils::FApplyToNextBinding
//
//      @doc:
//         Check if xform should be applied to the next binding
//
//---------------------------------------------------------------------------
BOOL CXformUtils::FApplyToNextBinding(CXform* pxform, CExpression* pexprLastBinding /* last extracted xform pattern */)
{
	GPOS_ASSERT(NULL != pxform);
	if (FSubqueryDecorrelation(pxform))
	{
		// if last binding is free from Subquery or Apply operators, we do not
		// need to apply the xform further
		return CUtils::FHasSubqueryOrApply(pexprLastBinding, false) || CUtils::FHasCorrelatedApply(pexprLastBinding, false);
	}
	// set of transformations that should be applied once
	CXform::EXformId rgXforms[] = {CXform::ExfJoinAssociativity, CXform::ExfExpandFullOuterJoin, CXform::ExfUnnestTVF, CXform::ExfLeftSemiJoin2CrossProduct, CXform::ExfLeftAntiSemiJoin2CrossProduct, CXform::ExfLeftAntiSemiJoinNotIn2CrossProduct,};
	CXform::EXformId exfid = pxform->Exfid();
	BOOL fApplyOnce = FSubqueryUnnesting(pxform) || FXformInArray(exfid, rgXforms, GPOS_ARRAY_SIZE(rgXforms));
	return !fApplyOnce;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::MapPrjElemsWithDistinctAggs
//
//	@doc:
//		Given a project list, create a map whose key is the argument of
//		distinct Agg, and value is the set of project elements that define
//		distinct Aggs on that argument,
//		non-distinct Aggs are grouped together in one set with key 'True',
//		for example,
//
//		Input: (x : count(distinct a),
//				y : sum(distinct a),
//				z : avg(distinct b),
//				w : max(c))
//
//		Output: (a --> {x,y},
//				 b --> {z},
//				 true --> {w})
//
//---------------------------------------------------------------------------
void CXformUtils::MapPrjElemsWithDistinctAggs(CMemoryPool *mp, CExpression *pexprPrjList, ExprToExprArrayMap **pphmexprdrgpexpr, ULONG *pulDifferentDQAs)
{
	GPOS_ASSERT(NULL != pexprPrjList);
	GPOS_ASSERT(NULL != pphmexprdrgpexpr);
	GPOS_ASSERT(NULL != pulDifferentDQAs);
	ExprToExprArrayMap *phmexprdrgpexpr = GPOS_NEW(mp) ExprToExprArrayMap(mp);
	ULONG ulDifferentDQAs = 0;
	CExpression *pexprTrue = CUtils::PexprScalarConstBool(mp, true /*value*/);
	const ULONG arity = pexprPrjList->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprPrjEl = (*pexprPrjList)[ul];
		CExpression *pexprChild = (*pexprPrjEl)[0];
		COperator *popChild = pexprChild->Pop();
		COperator::EOperatorId eopidChild = popChild->Eopid();
		if (COperator::EopScalarAggFunc != eopidChild &&
			COperator::EopScalarWindowFunc != eopidChild)
		{
			continue;
		}
		BOOL is_distinct = false;
		if (COperator::EopScalarAggFunc == eopidChild)
		{
			is_distinct = CScalarAggFunc::PopConvert(popChild)->IsDistinct();
		}
		else
		{
			is_distinct = CScalarWindowFunc::PopConvert(popChild)->IsDistinct();
		}
		CExpression *pexprKey = NULL;
		if (is_distinct && 1 == pexprChild->Arity())
		{
			// use first argument of Distinct Agg as key
			pexprKey = (*pexprChild)[0];
		}
		else
		{
			// use constant True as key
			pexprKey = pexprTrue;
		}
		CExpressionArray *pdrgpexpr = const_cast<CExpressionArray *>(phmexprdrgpexpr->Find(pexprKey));
		BOOL fExists = (NULL != pdrgpexpr);
		if (!fExists)
		{
			// first occurrence, create a new expression array
			pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
		}
		pexprPrjEl->AddRef();
		pdrgpexpr->Append(pexprPrjEl);
		if (!fExists)
		{
			pexprKey->AddRef();
#ifdef GPOS_DEBUG
			BOOL fSuccess =
#endif	// GPOS_DEBUG
				phmexprdrgpexpr->Insert(pexprKey, pdrgpexpr);
			GPOS_ASSERT(fSuccess);
			if (pexprKey != pexprTrue)
			{
				// first occurrence of a new DQA, increment counter
				ulDifferentDQAs++;
			}
		}
	}
	pexprTrue->Release();
	*pphmexprdrgpexpr = phmexprdrgpexpr;
	*pulDifferentDQAs = ulDifferentDQAs;
}

//	@function:
//		CXformUtils::PexprWinFuncAgg2ScalarAgg
//
//	@doc:
//		Converts an Agg window function into regular Agg
//
//---------------------------------------------------------------------------
CExpression* CXformUtils::PexprWinFuncAgg2ScalarAgg(CMemoryPool *mp, CExpression *pexprWinFunc)
{
	GPOS_ASSERT(NULL != pexprWinFunc);
	GPOS_ASSERT(COperator::EopScalarWindowFunc == pexprWinFunc->Pop()->Eopid());
	CExpressionArray *pdrgpexprWinFuncArgs = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG ulArgs = pexprWinFunc->Arity();
	for (ULONG ul = 0; ul < ulArgs; ul++)
	{
		CExpression *pexprArg = (*pexprWinFunc)[ul];
		pexprArg->AddRef();
		pdrgpexprWinFuncArgs->Append(pexprArg);
	}
	CScalarWindowFunc *popScWinFunc = CScalarWindowFunc::PopConvert(pexprWinFunc->Pop());
	IMDId *mdid_func = popScWinFunc->FuncMdId();
	mdid_func->AddRef();
	return GPOS_NEW(mp) CExpression(mp, CUtils::PopAggFunc(mp, mdid_func, GPOS_NEW(mp) CWStringConst(mp, popScWinFunc->PstrFunc()->GetBuffer()), popScWinFunc->IsDistinct(), EaggfuncstageGlobal, false), pdrgpexprWinFuncArgs);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::AddCTEProducer
//
//	@doc:
//		Helper to create a CTE producer expression and add it to global
//		CTE info structure
//		Does not take ownership of pexpr
//
//---------------------------------------------------------------------------
CExpression* CXformUtils::PexprAddCTEProducer(CMemoryPool *mp, ULONG ulCTEId, CColRefArray *colref_array, CExpression *pexpr)
{
	CColRefArray *pdrgpcrProd = CUtils::PdrgpcrCopy(mp, colref_array);
	UlongToColRefMap *colref_mapping = CUtils::PhmulcrMapping(mp, colref_array, pdrgpcrProd);
	CExpression *pexprRemapped = pexpr->PexprCopyWithRemappedColumns(mp, colref_mapping, true);
	colref_mapping->Release();
	CExpression *pexprProducer = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalCTEProducer(mp, ulCTEId, pdrgpcrProd), pexprRemapped);
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	pcteinfo->AddCTEProducer(pexprProducer);
	pexprProducer->Release();
	return pcteinfo->PexprCTEProducer(ulCTEId);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::ICmpPrjElemsArr
//
//	@doc:
//		Comparator used in sorting arrays of project elements
//		based on the column id of the first entry
//
//---------------------------------------------------------------------------
INT CXformUtils::ICmpPrjElemsArr(const void *pvFst, const void *pvSnd)
{
	GPOS_ASSERT(NULL != pvFst);
	GPOS_ASSERT(NULL != pvSnd);
	const CExpressionArray *pdrgpexprFst = *(const CExpressionArray **) (pvFst);
	const CExpressionArray *pdrgpexprSnd = *(const CExpressionArray **) (pvSnd);
	CExpression *pexprPrjElemFst = (*pdrgpexprFst)[0];
	CExpression *pexprPrjElemSnd = (*pdrgpexprSnd)[0];
	ULONG ulIdFst = CScalarProjectElement::PopConvert(pexprPrjElemFst->Pop())->Pcr()->Id();
	ULONG ulIdSnd = CScalarProjectElement::PopConvert(pexprPrjElemSnd->Pop())->Pcr()->Id();
	if (ulIdFst < ulIdSnd)
	{
		return -1;
	}
	if (ulIdFst > ulIdSnd)
	{
		return 1;
	}
	return 0;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PdrgpdrgpexprSortedPrjElemsArray
//
//	@doc:
//		Iterate over given hash map and return array of arrays of project
//		elements sorted by the column id of the first entries
//
//---------------------------------------------------------------------------
CExpressionArrays* CXformUtils::PdrgpdrgpexprSortedPrjElemsArray(CMemoryPool *mp, ExprToExprArrayMap *phmexprdrgpexpr)
{
	GPOS_ASSERT(NULL != phmexprdrgpexpr);
	CExpressionArrays *pdrgpdrgpexprPrjElems = GPOS_NEW(mp) CExpressionArrays(mp);
	ExprToExprArrayMapIter hmexprdrgpexpriter(phmexprdrgpexpr);
	while (hmexprdrgpexpriter.Advance())
	{
		CExpressionArray *pdrgpexprPrjElems = const_cast<CExpressionArray *>(hmexprdrgpexpriter.Value());
		pdrgpexprPrjElems->AddRef();
		pdrgpdrgpexprPrjElems->Append(pdrgpexprPrjElems);
	}
	pdrgpdrgpexprPrjElems->Sort(ICmpPrjElemsArr);
	return pdrgpdrgpexprPrjElems;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprGbAggOnCTEConsumer2Join
//
//	@doc:
//		Convert GbAgg with distinct aggregates to a join expression
//
//		each leaf node of the resulting join expression is a GbAgg on a single
//		distinct aggs, we also create a GbAgg leaf for all remaining (non-distinct)
//		aggregates, for example
//
//		Input:
//			GbAgg_(count(distinct a), max(distinct a), sum(distinct b), avg(a))
//				+---CTEConsumer
//
//		Output:
//			InnerJoin
//				|--InnerJoin
//				|		|--GbAgg_(count(distinct a), max(distinct a))
//				|		|		+---CTEConsumer
//				|		+--GbAgg_(sum(distinct b))
//				|				+---CTEConsumer
//				+--GbAgg_(avg(a))
//						+---CTEConsumer
//
//---------------------------------------------------------------------------
CExpression* CXformUtils::PexprGbAggOnCTEConsumer2Join(CMemoryPool *mp, CExpression *pexprGbAgg)
{
	GPOS_ASSERT(NULL != pexprGbAgg);
	GPOS_ASSERT(COperator::EopLogicalGbAgg == pexprGbAgg->Pop()->Eopid());
	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(pexprGbAgg->Pop());
	CColRefArray *pdrgpcrGrpCols = popGbAgg->Pdrgpcr();
	GPOS_ASSERT(popGbAgg->FGlobal());
	if (COperator::EopLogicalCTEConsumer != (*pexprGbAgg)[0]->Pop()->Eopid())
	{
		// child of GbAgg must be a CTE consumer
		return NULL;
	}
	CExpression *pexprPrjList = (*pexprGbAgg)[1];
	ULONG ulDistinctAggs = pexprPrjList->DeriveTotalDistinctAggs();
	if (1 == ulDistinctAggs)
	{
		// if only one distinct agg is used, return input expression
		pexprGbAgg->AddRef();
		return pexprGbAgg;
	}
	ExprToExprArrayMap *phmexprdrgpexpr = NULL;
	ULONG ulDifferentDQAs = 0;
	MapPrjElemsWithDistinctAggs(mp, pexprPrjList, &phmexprdrgpexpr, &ulDifferentDQAs);
	if (1 == phmexprdrgpexpr->Size())
	{
		// if all distinct aggs use the same argument, return input expression
		phmexprdrgpexpr->Release();
		pexprGbAgg->AddRef();
		return pexprGbAgg;
	}
	CExpression *pexprCTEConsumer = (*pexprGbAgg)[0];
	CLogicalCTEConsumer *popConsumer = CLogicalCTEConsumer::PopConvert(pexprCTEConsumer->Pop());
	const ULONG ulCTEId = popConsumer->UlCTEId();
	CColRefArray *pdrgpcrConsumerOutput = popConsumer->Pdrgpcr();
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	CExpression *pexprLastGbAgg = NULL;
	CColRefArray *pdrgpcrLastGrpCols = NULL;
	CExpression *pexprJoin = NULL;
	CExpression *pexprTrue = CUtils::PexprScalarConstBool(mp, true /*value*/);
	// iterate over map to extract sorted array of array of project elements,
	// we need to sort arrays here since hash map iteration is non-deterministic,
	// which may create non-deterministic ordering of join children leading to
	// changing the plan of the same query when run multiple times
	CExpressionArrays *pdrgpdrgpexprPrjElems = PdrgpdrgpexprSortedPrjElemsArray(mp, phmexprdrgpexpr);
	// counter of consumers
	ULONG ulConsumers = 0;
	const ULONG size = pdrgpdrgpexprPrjElems->Size();
	for (ULONG ulPrjElemsArr = 0; ulPrjElemsArr < size; ulPrjElemsArr++)
	{
		CExpressionArray *pdrgpexprPrjElems =
			(*pdrgpdrgpexprPrjElems)[ulPrjElemsArr];

		CExpression *pexprNewGbAgg = NULL;
		if (0 == ulConsumers)
		{
			// reuse input consumer
			pdrgpcrGrpCols->AddRef();
			pexprCTEConsumer->AddRef();
			pdrgpexprPrjElems->AddRef();
			pexprNewGbAgg = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalGbAgg(mp, pdrgpcrGrpCols, COperator::EgbaggtypeGlobal), pexprCTEConsumer, GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), pdrgpexprPrjElems));
		}
		else
		{
			// create a new consumer
			CColRefArray *pdrgpcrNewConsumerOutput = CUtils::PdrgpcrCopy(mp, pdrgpcrConsumerOutput);
			CExpression *pexprNewConsumer = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalCTEConsumer(mp, ulCTEId, pdrgpcrNewConsumerOutput));
			pcteinfo->IncrementConsumers(ulCTEId);
			// fix Aggs arguments to use new consumer output column
			UlongToColRefMap *colref_mapping = CUtils::PhmulcrMapping(mp, pdrgpcrConsumerOutput, pdrgpcrNewConsumerOutput);
			CExpressionArray *pdrgpexprNewPrjElems = GPOS_NEW(mp) CExpressionArray(mp);
			const ULONG ulPrjElems = pdrgpexprPrjElems->Size();
			for (ULONG ul = 0; ul < ulPrjElems; ul++)
			{
				CExpression *pexprPrjEl = (*pdrgpexprPrjElems)[ul];
				// to match requested columns upstream, we have to re-use the same computed
				// columns that define the aggregates, we avoid creating new columns during
				// expression copy by passing must_exist as false
				CExpression *pexprNewPrjEl = pexprPrjEl->PexprCopyWithRemappedColumns(mp, colref_mapping, false);
				pdrgpexprNewPrjElems->Append(pexprNewPrjEl);
			}
			// re-map grouping columns
			CColRefArray *pdrgpcrNewGrpCols = CUtils::PdrgpcrRemap(mp, pdrgpcrGrpCols, colref_mapping, true);
			// create new GbAgg expression
			pexprNewGbAgg = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalGbAgg(mp, pdrgpcrNewGrpCols, COperator::EgbaggtypeGlobal), pexprNewConsumer, GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), pdrgpexprNewPrjElems));
			colref_mapping->Release();
		}
		ulConsumers++;
		CColRefArray *pdrgpcrNewGrpCols = CLogicalGbAgg::PopConvert(pexprNewGbAgg->Pop())->Pdrgpcr();
		if (NULL != pexprLastGbAgg)
		{
			CExpression *pexprJoinCondition = NULL;
			if (0 == pdrgpcrLastGrpCols->Size())
			{
				GPOS_ASSERT(0 == pdrgpcrNewGrpCols->Size());
				pexprTrue->AddRef();
				pexprJoinCondition = pexprTrue;
			}
			else
			{
				GPOS_ASSERT(pdrgpcrLastGrpCols->Size() == pdrgpcrNewGrpCols->Size());
				pexprJoinCondition = CPredicateUtils::PexprINDFConjunction(mp, pdrgpcrLastGrpCols, pdrgpcrNewGrpCols);
			}
			if (NULL == pexprJoin)
			{
				// create first join
				pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp, pexprLastGbAgg, pexprNewGbAgg, pexprJoinCondition);
			}
			else
			{
				// cascade joins
				pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp, pexprJoin, pexprNewGbAgg, pexprJoinCondition);
			}
		}
		pexprLastGbAgg = pexprNewGbAgg;
		pdrgpcrLastGrpCols = pdrgpcrNewGrpCols;
	}
	pdrgpdrgpexprPrjElems->Release();
	phmexprdrgpexpr->Release();
	pexprTrue->Release();
	return pexprJoin;
}