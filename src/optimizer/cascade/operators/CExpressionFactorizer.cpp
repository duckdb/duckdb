//---------------------------------------------------------------------------
//	@filename:
//		CExpressionFactorizer.cpp
//
//	@doc:
//		,
//
//	@owner:
//		Utility functions for expression factorization
//
//	@test:
//
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CExpressionFactorizer.h"
#include "duckdb/optimizer/cascade/base/CColRefSetIter.h"
#include "duckdb/optimizer/cascade/base/CColRefTable.h"
#include "duckdb/optimizer/cascade/base/CConstraintInterval.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/exception.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessor.h"
#include "duckdb/optimizer/cascade/operators/CExpressionUtils.h"
#include "duckdb/optimizer/cascade/operators/CPredicateUtils.h"
#include "duckdb/optimizer/cascade/operators/ops.h"
#include "duckdb/optimizer/cascade/md/IMDFunction.h"
#include "duckdb/optimizer/cascade/md/IMDScalarOp.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"
#include "duckdb/optimizer/cascade/statistics/CStatistics.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprProcessDisjDescendents
//
//	@doc:
//		Visitor-like function to process descendents that are OR operators.
//
//
//---------------------------------------------------------------------------
CExpression* CExpressionFactorizer::PexprProcessDisjDescendents(CMemoryPool *mp, CExpression *pexpr, CExpression *pexprLowestLogicalAncestor, PexprProcessDisj pfnpepdFunction)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);
	CExpression *pexprLogicalAncestor = pexprLowestLogicalAncestor;
	if (pexpr->Pop()->FLogical())
	{
		pexprLogicalAncestor = pexpr;
	}

	if (CPredicateUtils::FOr(pexpr))
	{
		return (*pfnpepdFunction)(mp, pexpr, pexprLogicalAncestor);
	}
	// recursively process children
	const ULONG arity = pexpr->Arity();
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprProcessDisjDescendents(mp, (*pexpr)[ul], pexprLogicalAncestor, pfnpepdFunction);
		pdrgpexprChildren->Append(pexprChild);
	}
	COperator *pop = pexpr->Pop();
	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::AddFactor
//
//	@doc:
//		Helper for adding a given expression either to the given factors
//		array or to a residuals array
//
//
//---------------------------------------------------------------------------
void CExpressionFactorizer::AddFactor(CMemoryPool *mp, CExpression *pexpr, CExpressionArray *pdrgpexprFactors, CExpressionArray *pdrgpexprResidual, ExprMap *pexprmapFactors, ULONG
#ifdef GPOS_DEBUG
									 ulDisjuncts
#endif	// GPOS_DEBUG
)
{
	ULONG *pul = pexprmapFactors->Find(pexpr);
	GPOS_ASSERT_IMP(NULL != pul, ulDisjuncts == *pul);

	if (NULL != pul)
	{
		// check if factor already exist in factors array
		BOOL fFound = false;
		const ULONG size = pdrgpexprFactors->Size();
		for (ULONG ul = 0; !fFound && ul < size; ul++)
		{
			fFound = CUtils::Equals(pexpr, (*pdrgpexprFactors)[ul]);
		}

		if (!fFound)
		{
			pexpr->AddRef();
			pdrgpexprFactors->Append(pexpr);
		}

		// replace factor with constant True in the residuals array
		pdrgpexprResidual->Append(
			CPredicateUtils::PexprConjunction(mp, NULL /*pdrgpexpr*/));
	}
	else
	{
		pexpr->AddRef();
		pdrgpexprResidual->Append(pexpr);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprmapFactors
//
//	@doc:
//		Helper for building a factors map
//
//		Example:
//		input:  (A=B AND B=C AND B>0) OR (A=B AND B=C AND A>0)
//		output: [(A=B, 2), (B=C, 2)]
//
//---------------------------------------------------------------------------
CExpressionFactorizer::ExprMap *
CExpressionFactorizer::PexprmapFactors(CMemoryPool *mp, CExpression *pexpr)
{
	GPOS_ASSERT(CPredicateUtils::FOr(pexpr) &&
				"input must be an OR expression");

	// a global (expression -> count) map
	ExprMap *pexprmapGlobal = GPOS_NEW(mp) ExprMap(mp);

	// factors map
	ExprMap *pexprmapFactors = GPOS_NEW(mp) ExprMap(mp);

	// iterate over child disjuncts;
	// if a disjunct is an AND tree, iterate over its children
	const ULONG ulDisjuncts = pexpr->Arity();
	for (ULONG ulOuter = 0; ulOuter < ulDisjuncts; ulOuter++)
	{
		CExpression *pexprDisj = (*pexpr)[ulOuter];

		CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
		pexprDisj->AddRef();
		pdrgpexpr->Append(pexprDisj);

		if (CPredicateUtils::FAnd(pexprDisj))
		{
			pdrgpexpr->Release();
			pexprDisj->PdrgPexpr()->AddRef();
			pdrgpexpr = pexprDisj->PdrgPexpr();
		}

		const ULONG size = pdrgpexpr->Size();
		for (ULONG ulInner = 0; ulInner < size; ulInner++)
		{
			CExpression *pexprConj = (*pdrgpexpr)[ulInner];
			ULONG *pul = pexprmapGlobal->Find(pexprConj);
			if (NULL == pul)
			{
				pexprConj->AddRef();
				(void) pexprmapGlobal->Insert(pexprConj, GPOS_NEW(mp) ULONG(1));
			}
			else
			{
				(*pul)++;
			}

			pul = pexprmapGlobal->Find(pexprConj);
			if (*pul == ulDisjuncts)
			{
				// reached the count of initial disjuncts, add expression to factors map
				pexprConj->AddRef();
				(void) pexprmapFactors->Insert(pexprConj,
											   GPOS_NEW(mp) ULONG(ulDisjuncts));
			}
		}
		pdrgpexpr->Release();
	}
	pexprmapGlobal->Release();

	return pexprmapFactors;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprFactorizeDisj
//
//	@doc:
//		Factorize common expressions in an OR tree;
//		the result is a conjunction of factors and a residual Or tree
//
//		Example:
//		input:  [(A=B AND C>0) OR (A=B AND A>0) OR (A=B AND B>0)]
//		output: [(A=B) AND (C>0 OR A>0 OR B>0)]
//
//---------------------------------------------------------------------------
CExpression *
CExpressionFactorizer::PexprFactorizeDisj(CMemoryPool *mp, CExpression *pexpr,
										  CExpression *	 //pexprLogical
)
{
	GPOS_ASSERT(CPredicateUtils::FOr(pexpr) &&
				"input must be an OR expression");

	// build factors map
	ExprMap *pexprmapFactors = PexprmapFactors(mp, pexpr);

	CExpressionArray *pdrgpexprResidual = GPOS_NEW(mp) CExpressionArray(mp);
	CExpressionArray *pdrgpexprFactors = GPOS_NEW(mp) CExpressionArray(mp);

	// iterate over child expressions and factorize them
	const ULONG ulDisjuncts = pexpr->Arity();
	for (ULONG ulOuter = 0; ulOuter < ulDisjuncts; ulOuter++)
	{
		CExpression *pexprDisj = (*pexpr)[ulOuter];
		if (CPredicateUtils::FAnd(pexprDisj))
		{
			CExpressionArray *pdrgpexprConjuncts =
				GPOS_NEW(mp) CExpressionArray(mp);
			const ULONG size = pexprDisj->Arity();
			for (ULONG ulInner = 0; ulInner < size; ulInner++)
			{
				CExpression *pexprConj = (*pexprDisj)[ulInner];
				AddFactor(mp, pexprConj, pdrgpexprFactors, pdrgpexprConjuncts,
						  pexprmapFactors, ulDisjuncts);
			}

			if (0 < pdrgpexprConjuncts->Size())
			{
				pdrgpexprResidual->Append(
					CPredicateUtils::PexprConjunction(mp, pdrgpexprConjuncts));
			}
			else
			{
				pdrgpexprConjuncts->Release();
			}
		}
		else
		{
			AddFactor(mp, pexprDisj, pdrgpexprFactors, pdrgpexprResidual,
					  pexprmapFactors, ulDisjuncts);
		}
	}
	pexprmapFactors->Release();

	if (0 < pdrgpexprResidual->Size())
	{
		// residual becomes a new factor
		pdrgpexprFactors->Append(
			CPredicateUtils::PexprDisjunction(mp, pdrgpexprResidual));
	}
	else
	{
		// no residuals, release created array
		pdrgpexprResidual->Release();
	}

	// return a conjunction of all factors
	return CPredicateUtils::PexprConjunction(mp, pdrgpexprFactors);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprDiscoverFactors
//
//	@doc:
//		Discover common factors in scalar expressions
//
//
//---------------------------------------------------------------------------
CExpression *
CExpressionFactorizer::PexprDiscoverFactors(CMemoryPool *mp, CExpression *pexpr)
{
	return PexprProcessDisjDescendents(
		mp, pexpr, NULL /*pexprLowestLogicalAncestor*/, PexprFactorizeDisj);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprFactorize
//
//	@doc:
//		Factorize common scalar expressions
//
//
//---------------------------------------------------------------------------
CExpression *
CExpressionFactorizer::PexprFactorize(CMemoryPool *mp, CExpression *pexpr)
{
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	CExpression *pexprFactorized = PexprDiscoverFactors(mp, pexpr);

	// factorization might reveal unnested AND/OR
	CExpression *pexprUnnested =
		CExpressionUtils::PexprUnnest(mp, pexprFactorized);
	pexprFactorized->Release();

	// eliminate duplicate AND/OR children
	CExpression *pexprDeduped =
		CExpressionUtils::PexprDedupChildren(mp, pexprUnnested);
	pexprUnnested->Release();

	return pexprDeduped;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PcrsUsedByPushableScalar
//
//	@doc:
//		If the given expression is a scalar that can be pushed, it returns
//		the set of used columns.
//
//---------------------------------------------------------------------------
CColRefSet *
CExpressionFactorizer::PcrsUsedByPushableScalar(CExpression *pexpr)
{
	if (!pexpr->Pop()->FScalar())
	{
		return NULL;
	}

	if (0 < pexpr->DeriveDefinedColumns()->Size() ||
		pexpr->DeriveHasSubquery() ||
		IMDFunction::EfsVolatile ==
			pexpr->DeriveScalarFunctionProperties()->Efs() ||
		IMDFunction::EfdaNoSQL !=
			pexpr->DeriveScalarFunctionProperties()->Efda())
	{
		return NULL;
	}

	return pexpr->DeriveUsedColumns();
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::FOpSourceIdOrComputedColumn
//
//	@doc:
//		If the given expression is a non volatile scalar expression using table columns
//		created by the same operator (or using the same computed column)
//		return true and set ulOpSourceId to the id of the operator that created those
//		columns (set *ppcrComputedColumn to the computed column used),
//		otherwise return false.
//
//---------------------------------------------------------------------------
BOOL
CExpressionFactorizer::FOpSourceIdOrComputedColumn(CExpression *pexpr,
												   ULONG *ulOpSourceId,
												   CColRef **ppcrComputedColumn)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != ulOpSourceId);
	GPOS_ASSERT(NULL != ppcrComputedColumn);

	*ulOpSourceId = gpos::ulong_max;
	*ppcrComputedColumn = NULL;

	CColRefSet *pcrsUsed = PcrsUsedByPushableScalar(pexpr);
	if (NULL == pcrsUsed || 0 == pcrsUsed->Size())
	{
		return false;
	}

	ULONG ulComputedOpSourceId = gpos::ulong_max;
	CColRefSetIter crsi(*pcrsUsed);
	while (crsi.Advance())
	{
		CColRef *colref = crsi.Pcr();
		if (CColRef::EcrtTable != colref->Ecrt())
		{
			if (NULL == *ppcrComputedColumn)
			{
				*ppcrComputedColumn = colref;
			}
			else if (colref != *ppcrComputedColumn)
			{
				return false;
			}

			continue;
		}
		else if (NULL != *ppcrComputedColumn)
		{
			// don't allow a mix of computed columns and table columns
			return false;
		}

		const CColRefTable *pcrTable = CColRefTable::PcrConvert(colref);
		if (gpos::ulong_max == ulComputedOpSourceId)
		{
			ulComputedOpSourceId = pcrTable->UlSourceOpId();
		}
		else if (ulComputedOpSourceId != pcrTable->UlSourceOpId())
		{
			// expression refers to columns coming from different operators
			return false;
		}
	}
	*ulOpSourceId = ulComputedOpSourceId;

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PdrgPdrgpexprDisjunctArrayForSourceId
//
//	@doc:
//		Find the array of expression arrays corresponding to the given
//		operator source id in the given source to array position map
//		or construct a new array and add it to the map.
//
//---------------------------------------------------------------------------
CExpressionArrays *
CExpressionFactorizer::PdrgPdrgpexprDisjunctArrayForSourceId(
	CMemoryPool *mp, SourceToArrayPosMap *psrc2array, BOOL fAllowNewSources,
	ULONG ulOpSourceId)
{
	GPOS_ASSERT(NULL != psrc2array);
	CExpressionArrays *pdrgpdrgpexpr = psrc2array->Find(&ulOpSourceId);

	// if there is no entry, we start recording expressions that will become disjuncts
	// corresponding to the source operator we are considering
	if (NULL == pdrgpdrgpexpr)
	{
		// checking this flag allows us to disable adding new entries: if a source operator
		// does not appear in the first disjunct, there is no need to add it later since it
		// will not cover the entire disjunction
		if (!fAllowNewSources)
		{
			return NULL;
		}
		pdrgpdrgpexpr = GPOS_NEW(mp) CExpressionArrays(mp);
#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif	// GPOS_DEBUG
			psrc2array->Insert(GPOS_NEW(mp) ULONG(ulOpSourceId), pdrgpdrgpexpr);
		GPOS_ASSERT(fInserted);
	}

	return pdrgpdrgpexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PdrgPdrgpexprDisjunctArrayForColumn
//
//	@doc:
// 		Find the array of expression arrays corresponding to the given
// 		column in the given column to array position map
// 		or construct a new array and add it to the map.
//
//---------------------------------------------------------------------------
CExpressionArrays *
CExpressionFactorizer::PdrgPdrgpexprDisjunctArrayForColumn(
	CMemoryPool *mp, ColumnToArrayPosMap *pcol2array, BOOL fAllowNewSources,
	CColRef *colref)
{
	GPOS_ASSERT(NULL != pcol2array);
	CExpressionArrays *pdrgpdrgpexpr = pcol2array->Find(colref);

	// if there is no entry, we start recording expressions that will become disjuncts
	// corresponding to the computed column we are considering
	if (NULL == pdrgpdrgpexpr)
	{
		// checking this flag allows us to disable adding new entries: if a column
		// does not appear in the first disjunct, there is no need to add it later since it
		// will not cover the entire disjunction
		if (!fAllowNewSources)
		{
			return NULL;
		}
		pdrgpdrgpexpr = GPOS_NEW(mp) CExpressionArrays(mp);
#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif	// GPOS_DEBUG
			pcol2array->Insert(colref, pdrgpdrgpexpr);
		GPOS_ASSERT(fInserted);
	}

	return pdrgpdrgpexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::RecordComparison
//
//	@doc:
//		If the given expression is a table column to constant comparison,
//		record it in the 'psrc2array' map.
//		If 'fAllowNewSources' is false, no new entries can be created in the
//		map. 'ulPosition' indicates the position in the entry where to add
//		the expression.
//
//---------------------------------------------------------------------------
void
CExpressionFactorizer::StoreBaseOpToColumnExpr(
	CMemoryPool *mp, CExpression *pexpr, SourceToArrayPosMap *psrc2array,
	ColumnToArrayPosMap *pcol2array, const CColRefSet *pcrsProducedByChildren,
	BOOL fAllowNewSources, ULONG ulPosition)
{
	ULONG ulOpSourceId;
	CColRef *pcrComputed = NULL;
	if (!FOpSourceIdOrComputedColumn(pexpr, &ulOpSourceId, &pcrComputed))
	{
		return;
	}

	CExpressionArrays *pdrgpdrgpexpr = NULL;

	if (gpos::ulong_max != ulOpSourceId)
	{
		pdrgpdrgpexpr = PdrgPdrgpexprDisjunctArrayForSourceId(
			mp, psrc2array, fAllowNewSources, ulOpSourceId);
	}
	else
	{
		GPOS_ASSERT(NULL != pcrComputed);
		if (NULL != pcrsProducedByChildren &&
			pcrsProducedByChildren->FMember(pcrComputed))
		{
			// do not create filters for columns produced by the scalar tree of
			// a logical operator immediately under the current logical operator
			return;
		}

		pdrgpdrgpexpr = PdrgPdrgpexprDisjunctArrayForColumn(
			mp, pcol2array, fAllowNewSources, pcrComputed);
	}

	if (NULL == pdrgpdrgpexpr)
	{
		return;
	}

	CExpressionArray *pdrgpexpr = NULL;
	// there are only two cases we need to consider
	// the first one is that we found the current source operator in all previous disjuncts
	// and now we are starting a new sub-array for a new disjunct
	if (ulPosition == pdrgpdrgpexpr->Size())
	{
		pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
		pdrgpdrgpexpr->Append(pdrgpexpr);
	}
	// the second case is that we found additional conjuncts for the current source operator
	// inside the current disjunct
	else if (ulPosition == pdrgpdrgpexpr->Size() - 1)
	{
		pdrgpexpr = (*pdrgpdrgpexpr)[ulPosition];
	}
	// otherwise, this source operator is not covered by all disjuncts, so there is no need to
	// keep recording it since it will not lead to a valid pre-filter
	else
	{
		return;
	}

	pexpr->AddRef();
	pdrgpexpr->Append(pexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprAddInferredFilters
//
//	@doc:
//		Create a conjunction of the given expression and inferred filters constructed out
//		of the given map.
//
//---------------------------------------------------------------------------
CExpression *
CExpressionFactorizer::PexprAddInferredFilters(CMemoryPool *mp,
											   CExpression *pexpr,
											   SourceToArrayPosMap *psrc2array,
											   ColumnToArrayPosMap *pcol2array)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CPredicateUtils::FOr(pexpr));
	GPOS_ASSERT(NULL != psrc2array);

	SourceToArrayPosMapIter src2arrayIter(psrc2array);
	CExpressionArray *pdrgpexprPrefilters = GPOS_NEW(mp) CExpressionArray(mp);
	pexpr->AddRef();
	pdrgpexprPrefilters->Append(pexpr);
	const ULONG ulDisjChildren = pexpr->Arity();

	while (src2arrayIter.Advance())
	{
		AddInferredFiltersFromArray(mp, src2arrayIter.Value(), ulDisjChildren,
									pdrgpexprPrefilters);
	}

	ColumnToArrayPosMapIter col2arrayIter(pcol2array);
	while (col2arrayIter.Advance())
	{
		AddInferredFiltersFromArray(mp, col2arrayIter.Value(), ulDisjChildren,
									pdrgpexprPrefilters);
	}

	return CPredicateUtils::PexprConjunction(mp, pdrgpexprPrefilters);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprAddInferredFilters
//
//	@doc:
// 		Construct a filter based on the expressions from 'pdrgpdrgpexpr'
// 		and add to the array 'pdrgpexprPrefilters'.
//
//---------------------------------------------------------------------------
void
CExpressionFactorizer::AddInferredFiltersFromArray(
	CMemoryPool *mp, const CExpressionArrays *pdrgpdrgpexpr,
	ULONG ulDisjChildrenLength, CExpressionArray *pdrgpexprInferredFilters)
{
	const ULONG ulEntryLength =
		(pdrgpdrgpexpr == NULL) ? 0 : pdrgpdrgpexpr->Size();
	if (ulEntryLength == ulDisjChildrenLength)
	{
		CExpressionArray *pdrgpexprDisjuncts =
			GPOS_NEW(mp) CExpressionArray(mp);
		for (ULONG ul = 0; ul < ulEntryLength; ++ul)
		{
			(*pdrgpdrgpexpr)[ul]->AddRef();
			CExpression *pexprConj =
				CPredicateUtils::PexprConjunction(mp, (*pdrgpdrgpexpr)[ul]);
			pdrgpexprDisjuncts->Append(pexprConj);
		}
		if (0 < pdrgpexprDisjuncts->Size())
		{
			pdrgpexprInferredFilters->Append(
				CPredicateUtils::PexprDisjunction(mp, pdrgpexprDisjuncts));
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PcrsColumnsProducedByChildren
//
//	@doc:
//		Returns the set of columns produced by the scalar trees of the given expression's
//		children
//
//---------------------------------------------------------------------------
CColRefSet *
CExpressionFactorizer::PcrsColumnsProducedByChildren(CMemoryPool *mp,
													 CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	const ULONG arity = pexpr->Arity();
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	for (ULONG ulTop = 0; ulTop < arity; ulTop++)
	{
		CExpression *pexprChild = (*pexpr)[ulTop];
		const ULONG ulChildArity = pexprChild->Arity();
		for (ULONG ulBelowChild = 0; ulBelowChild < ulChildArity;
			 ulBelowChild++)
		{
			CExpression *pexprGrandChild = (*pexprChild)[ulBelowChild];
			if (pexprGrandChild->Pop()->FScalar())
			{
				CColRefSet *pcrsChildDefined =
					pexprGrandChild->DeriveDefinedColumns();
				pcrs->Include(pcrsChildDefined);
			}
		}
	}

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprExtractInferredFiltersFromDisj
//
//	@doc:
//		Compute disjunctive inferred filters that can be pushed to the column creators
//
//---------------------------------------------------------------------------
CExpression *
CExpressionFactorizer::PexprExtractInferredFiltersFromDisj(
	CMemoryPool *mp, CExpression *pexpr,
	CExpression *pexprLowestLogicalAncestor)
{
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CPredicateUtils::FOr(pexpr) &&
				"input must be an OR expression");
	GPOS_ASSERT(NULL != pexprLowestLogicalAncestor);

	const ULONG arity = pexpr->Arity();
	GPOS_ASSERT(2 <= arity);

	// for each source operator, create a map entry which, for every disjunct,
	// has the array of comparisons using that operator
	// we initialize the entries with operators appearing in the first disjunct
	SourceToArrayPosMap *psrc2array = GPOS_NEW(mp) SourceToArrayPosMap(mp);

	// create a similar map for computed columns
	ColumnToArrayPosMap *pcol2array = GPOS_NEW(mp) ColumnToArrayPosMap(mp);

	CColRefSet *pcrsProducedByChildren = NULL;
	if (COperator::EopLogicalSelect ==
		pexprLowestLogicalAncestor->Pop()->Eopid())
	{
		pcrsProducedByChildren =
			PcrsColumnsProducedByChildren(mp, pexprLowestLogicalAncestor);
	}

	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprCurrent = (*pexpr)[ul];
		BOOL fFirst = 0 == ul;
		if (CPredicateUtils::FAnd(pexprCurrent))
		{
			const ULONG ulAndArity = pexprCurrent->Arity();
			for (ULONG ulAnd = 0; ulAnd < ulAndArity; ++ulAnd)
			{
				StoreBaseOpToColumnExpr(mp, (*pexprCurrent)[ulAnd], psrc2array,
										pcol2array, pcrsProducedByChildren,
										fFirst,	 // fAllowNewSources
										ul);
			}
		}
		else
		{
			StoreBaseOpToColumnExpr(mp, pexprCurrent, psrc2array, pcol2array,
									pcrsProducedByChildren,
									fFirst /*fAllowNewSources*/, ul);
		}

		if (fFirst && 0 == psrc2array->Size() && 0 == pcol2array->Size())
		{
			psrc2array->Release();
			pcol2array->Release();
			CRefCount::SafeRelease(pcrsProducedByChildren);
			pexpr->AddRef();
			return pexpr;
		}
		GPOS_CHECK_ABORT;
	}
	CExpression *pexprWithPrefilters =
		PexprAddInferredFilters(mp, pexpr, psrc2array, pcol2array);
	psrc2array->Release();
	pcol2array->Release();
	CRefCount::SafeRelease(pcrsProducedByChildren);
	CExpression *pexprDeduped =
		CExpressionUtils::PexprDedupChildren(mp, pexprWithPrefilters);
	pexprWithPrefilters->Release();

	return pexprDeduped;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprExtractInferredFilters
//
//	@doc:
//		Compute disjunctive pre-filters that can be pushed to the column creators.
//		These inferred filters need to "cover" all the disjuncts with expressions
//		coming from the same source.
//
//		For instance, out of the predicate
//		((sale_type = 's'::text AND dyear = 2001 AND year_total > 0::numeric) OR
//		(sale_type = 's'::text AND dyear = 2002) OR
//		(sale_type = 'w'::text AND dmoy = 7 AND year_total > 0::numeric) OR
//		(sale_type = 'w'::text AND dyear = 2002 AND dmoy = 7))
//
//		we can infer the filter
//		dyear=2001 OR dyear=2002 OR dmoy=7 OR (dyear=2002 AND dmoy=7)
//
//		which can later be pushed down by the normalizer
//
//---------------------------------------------------------------------------
CExpression* CExpressionFactorizer::PexprExtractInferredFilters(CMemoryPool *mp, CExpression *pexpr)
{
	return PexprProcessDisjDescendents(mp, pexpr, NULL, PexprExtractInferredFiltersFromDisj);
}
