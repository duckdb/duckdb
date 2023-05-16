//---------------------------------------------------------------------------
//	@filename:
//		CLogicalNAryJoin.cpp
//
//	@doc:
//		Implementation of n-ary inner join operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CLogicalNAryJoin.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColumnFactory.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/operators/CPredicateUtils.h"
#include "duckdb/optimizer/cascade/statistics/CStatisticsUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalNAryJoin::CLogicalNAryJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalNAryJoin::CLogicalNAryJoin(CMemoryPool *mp)
	: CLogicalJoin(mp), m_lojChildPredIndexes(NULL)
{
	GPOS_ASSERT(NULL != mp);
}

CLogicalNAryJoin::CLogicalNAryJoin(CMemoryPool *mp,
								   ULongPtrArray *lojChildIndexes)
	: CLogicalJoin(mp), m_lojChildPredIndexes(lojChildIndexes)
{
	GPOS_ASSERT(NULL != mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalNAryJoin::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalNAryJoin::DeriveMaxCard(CMemoryPool *mp,
								CExpressionHandle &exprhdl) const
{
	CMaxCard maxCard(1);
	const ULONG arity = exprhdl.Arity();

	// multiply the max cards of the children (use at least 1 for LOJ children)
	for (ULONG ul = 0; ul < arity - 1; ul++)
	{
		CMaxCard childMaxCard = exprhdl.DeriveMaxCard(ul);

		if (IsInnerJoinChild(ul) || 1 <= childMaxCard.Ull())
		{
			maxCard *= childMaxCard;
		}
	}

	if (exprhdl.DerivePropertyConstraint()->FContradiction())
	{
		return CMaxCard(0 /*ull*/);
	}

	CExpression *pexprScalar = exprhdl.PexprScalarExactChild(arity - 1);

	if (NULL != pexprScalar)
	{
		if (COperator::EopScalarNAryJoinPredList == pexprScalar->Pop()->Eopid())
		{
			CExpression *pexprScalarChild = GetTrueInnerJoinPreds(mp, exprhdl);

			// in case of a false condition (when the operator is non Inner Join)
			// maxcard should be zero
			if (NULL != pexprScalarChild &&
				CUtils::FScalarConstFalse(pexprScalarChild))
			{
				pexprScalarChild->Release();
				return CMaxCard(0 /*ull*/);
			}
			CRefCount::SafeRelease(pexprScalarChild);
		}
		else
		{
			return CLogical::Maxcard(exprhdl, exprhdl.Arity() - 1, maxCard);
		}
	}

	return maxCard;
}

CColRefSet *
CLogicalNAryJoin::DeriveNotNullColumns(CMemoryPool *mp,
									   CExpressionHandle &exprhdl) const
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);

	// union not nullable columns from the first N-1 children that are not right children of LOJs
	ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity - 1; ul++)
	{
		if (IsInnerJoinChild(ul))
		{
			CColRefSet *pcrsChild = exprhdl.DeriveNotNullColumns(ul);
			GPOS_ASSERT(pcrs->IsDisjoint(pcrsChild) &&
						"Input columns are not disjoint");

			pcrs->Union(pcrsChild);
		}
	}

	return pcrs;
}

CPropConstraint *
CLogicalNAryJoin::DerivePropertyConstraint(CMemoryPool *mp,
										   CExpressionHandle &exprhdl) const
{
	if (!HasOuterJoinChildren())
	{
		// shortcut for inner joins
		return PpcDeriveConstraintFromPredicates(mp, exprhdl);
	}

	// the following logic is similar to PpcDeriveConstraintFromPredicates, except that
	// it excludes right children of LOJs and their ON predicates
	CColRefSetArray *equivalenceClasses = GPOS_NEW(mp) CColRefSetArray(mp);
	CConstraintArray *constraints = GPOS_NEW(mp) CConstraintArray(mp);

	// collect constraint properties from inner join children
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity - 1; ul++)
	{
		if (IsInnerJoinChild(ul))
		{
			CPropConstraint *ppc = exprhdl.DerivePropertyConstraint(ul);

			// equivalence classes coming from child
			CColRefSetArray *pdrgpcrsChild = ppc->PdrgpcrsEquivClasses();

			// merge with the equivalence classes we have so far
			CColRefSetArray *pdrgpcrsMerged = CUtils::PdrgpcrsMergeEquivClasses(
				mp, equivalenceClasses, pdrgpcrsChild);
			equivalenceClasses->Release();
			equivalenceClasses = pdrgpcrsMerged;

			// constraint coming from child
			CConstraint *pcnstr = ppc->Pcnstr();
			if (NULL != pcnstr)
			{
				pcnstr->AddRef();
				constraints->Append(pcnstr);
			}
		}
	}

	// process inner join predicates
	CExpression *trueInnerJoinPreds = GetTrueInnerJoinPreds(mp, exprhdl);
	if (NULL != trueInnerJoinPreds)
	{
		CColRefSetArray *equivClassesFromInnerJoinPreds = NULL;
		CConstraint *pcnstr = CConstraint::PcnstrFromScalarExpr(
			mp, trueInnerJoinPreds, &equivClassesFromInnerJoinPreds);

		if (NULL != pcnstr)
		{
			constraints->Append(pcnstr);

			// merge with the equivalence classes we have so far
			CColRefSetArray *pdrgpcrsMerged = CUtils::PdrgpcrsMergeEquivClasses(
				mp, equivalenceClasses, equivClassesFromInnerJoinPreds);
			equivalenceClasses->Release();
			equivalenceClasses = pdrgpcrsMerged;
		}

		trueInnerJoinPreds->Release();
		CRefCount::SafeRelease(equivClassesFromInnerJoinPreds);
	}

	CConstraint *pcnstrNew = CConstraint::PcnstrConjunction(mp, constraints);

	return GPOS_NEW(mp) CPropConstraint(mp, equivalenceClasses, pcnstrNew);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalNAryJoin::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalNAryJoin::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);

	(void) xform_set->ExchangeSet(CXform::ExfSubqNAryJoin2Apply);
	(void) xform_set->ExchangeSet(CXform::ExfExpandNAryJoin);
	(void) xform_set->ExchangeSet(CXform::ExfExpandNAryJoinMinCard);
	(void) xform_set->ExchangeSet(CXform::ExfExpandNAryJoinDP);
	(void) xform_set->ExchangeSet(CXform::ExfExpandNAryJoinGreedy);
	(void) xform_set->ExchangeSet(CXform::ExfExpandNAryJoinDPv2);

	return xform_set;
}

CLogicalNAryJoin *
CLogicalNAryJoin::PopConvertNAryLOJ(COperator *pop)
{
	CLogicalNAryJoin *naryJoin = PopConvert(pop);

	if (NULL != naryJoin && naryJoin->HasOuterJoinChildren())
	{
		return naryJoin;
	}

	return NULL;
}

CExpression *
CLogicalNAryJoin::GetTrueInnerJoinPreds(CMemoryPool *mp,
										CExpressionHandle &exprhdl) const
{
	// "true" inner join predicates are those that don't rely on any tables that are
	// right children of non-inner joins. Example:
	//
	// select ... from foo left outer join bar on foo.a=bar.a join jazz on foo.b=jazz.b and coalesce(bar.c,0) = jazz.c;
	//
	// coalesce(bar.c,0) = jazz.c is not a "true" inner join predicate, since it relies
	// on column bar.c, which comes from an LOJ and therefore may be NULL, even though
	// bar.c might have been created with a NOT NULL constraint. We don't want to use
	// such predicates in constraint derivation.
	ULONG arity = exprhdl.Arity();
	CExpression *pexprScalar = exprhdl.PexprScalarExactChild(arity - 1);

	if (NULL == pexprScalar)
	{
		// can't determine the true inner join preds, as there is no exact scalar
		// expression available and this method is expected to return an exact expression
		return NULL;
	}

	if (!HasOuterJoinChildren())
	{
		// all inner joins, all the predicates are true inner join preds
		pexprScalar->AddRef();
		return pexprScalar;
	}

	CExpressionArray *predArray = NULL;
	CExpressionArray *trueInnerJoinPredArray =
		GPOS_NEW(mp) CExpressionArray(mp);
	CExpression *innerJoinPreds = (*pexprScalar)[0];
	BOOL isAConjunction = CPredicateUtils::FAnd(innerJoinPreds);

	GPOS_ASSERT(COperator::EopScalarNAryJoinPredList ==
				pexprScalar->Pop()->Eopid());

	// split the predicate into conjuncts and inspect those individually
	predArray = CPredicateUtils::PdrgpexprConjuncts(mp, innerJoinPreds);

	for (ULONG ul = 0; ul < predArray->Size(); ul++)
	{
		CExpression *pred = (*predArray)[ul];
		CColRefSet *predCols = pred->DeriveUsedColumns();
		BOOL addToPredArray = true;

		// check whether the predicate uses any ColRefs that come from a non-inner join child
		for (ULONG c = 0; c < exprhdl.Arity() - 1; c++)
		{
			if (0 < *(*m_lojChildPredIndexes)[c])
			{
				// this is a right child of a non-inner join
				CColRefSet *nijOutputCols = exprhdl.DeriveOutputColumns(c);

				if (predCols->FIntersects(nijOutputCols))
				{
					// this predicate refers to some columns from non-inner joins,
					// which may become NULL, even when the type of the column is NOT NULL,
					// so the predicate may not actually be FALSE constants in some cases
					addToPredArray = false;
					break;
				}
			}
		}

		if (addToPredArray)
		{
			pred->AddRef();
			trueInnerJoinPredArray->Append(pred);
		}
	}

	predArray->Release();
	if (0 == trueInnerJoinPredArray->Size())
	{
		trueInnerJoinPredArray->Release();
		return CUtils::PexprScalarConstBool(mp, true);
	}
	return CPredicateUtils::PexprConjDisj(mp, trueInnerJoinPredArray,
										  isAConjunction);
}


//---------------------------------------------------------------------------
// CLogicalNAryJoin::ReplaceInnerJoinPredicates
//
// given an existing scalar child of an NAry join, make a new copy, replacing
// only the inner join predicates and leaving the LOJ ON predicates the same
//---------------------------------------------------------------------------
CExpression *
CLogicalNAryJoin::ReplaceInnerJoinPredicates(
	CMemoryPool *mp, CExpression *old_nary_join_scalar_expr,
	CExpression *new_inner_join_preds)
{
	COperator *pop = old_nary_join_scalar_expr->Pop();

	if (EopScalarNAryJoinPredList == pop->Eopid())
	{
		GPOS_ASSERT(NULL != m_lojChildPredIndexes);
		// this requires a bit of surgery, make a new copy of the
		// CScalarNAryJoinPredList with the first child replaced
		CExpressionArray *new_children = GPOS_NEW(mp) CExpressionArray(mp);

		new_children->Append(new_inner_join_preds);

		for (ULONG ul = 1; ul < old_nary_join_scalar_expr->Arity(); ul++)
		{
			CExpression *existing_child = (*old_nary_join_scalar_expr)[ul];

			existing_child->AddRef();
			new_children->Append(existing_child);
		}

		pop->AddRef();

		return GPOS_NEW(mp) CExpression(mp, pop, new_children);
	}

	// with all inner joins it's a total replacement, just return the inner join preds
	// (caller should have passed us a ref count which they now get back from us)
	GPOS_ASSERT(NULL == m_lojChildPredIndexes);

	return new_inner_join_preds;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalNAryJoin::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalNAryJoin::OsPrint(IOstream &os) const
{
	os << SzId();

	if (NULL != m_lojChildPredIndexes)
	{
		// print out the indexes of the logical children that correspond to
		// the scalar child entries below the CScalarNAryJoinPredList
		os << " [";
		ULONG size = m_lojChildPredIndexes->Size();
		for (ULONG ul = 0; ul < size; ul++)
		{
			if (0 < ul)
			{
				os << ", ";
			}
			os << *((*m_lojChildPredIndexes)[ul]);
		}
		os << "]";
	}

	return os;
}