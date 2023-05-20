//---------------------------------------------------------------------------
//	@filename:
//		CPredicateUtils.cpp
//
//	@doc:
//		Implementation of predicate normalization
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CPredicateUtils.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CCastUtils.h"
#include "duckdb/optimizer/cascade/base/CColRefSetIter.h"
#include "duckdb/optimizer/cascade/base/CColRefTable.h"
#include "duckdb/optimizer/cascade/base/CConstraintDisjunction.h"
#include "duckdb/optimizer/cascade/base/CConstraintInterval.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/exception.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessor.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessorUtils.h"
#include "duckdb/optimizer/cascade/operators/CNormalizer.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalJoin.h"
#include "duckdb/optimizer/cascade/operators/ops.h"
#include "duckdb/optimizer/cascade/md/CMDIdGPDB.h"
#include "duckdb/optimizer/cascade/md/IMDCast.h"
#include "duckdb/optimizer/cascade/md/IMDScalarOp.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"
#include "duckdb/optimizer/cascade/statistics/CStatistics.h"

using namespace gpopt;
using namespace gpmd;

// check if the expression is a negated boolean scalar identifier
BOOL CPredicateUtils::FNegatedBooleanScalarIdent(CExpression *pexprPred)
{
	GPOS_ASSERT(NULL != pexprPred);

	if (CPredicateUtils::FNot(pexprPred))
	{
		return (FNot(pexprPred) && FBooleanScalarIdent((*pexprPred)[0]));
	}

	return false;
}

// check if the expression is a boolean scalar identifier
BOOL CPredicateUtils::FBooleanScalarIdent(CExpression *pexprPred)
{
	GPOS_ASSERT(NULL != pexprPred);

	if (COperator::EopScalarIdent == pexprPred->Pop()->Eopid())
	{
		CScalarIdent *popScIdent = CScalarIdent::PopConvert(pexprPred->Pop());
		if (IMDType::EtiBool ==
			popScIdent->Pcr()->RetrieveType()->GetDatumType())
		{
			return true;
		}
	}

	return false;
}

// is the given expression an equality comparison
BOOL
CPredicateUtils::IsEqualityOp(CExpression *pexpr)
{
	return FComparison(pexpr, IMDType::EcmptEq);
}

// is the given expression a comparison
BOOL
CPredicateUtils::FComparison(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	return COperator::EopScalarCmp == pexpr->Pop()->Eopid();
}

// is the given expression a comparison of the given type
BOOL
CPredicateUtils::FComparison(CExpression *pexpr, IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();

	if (COperator::EopScalarCmp != pop->Eopid())
	{
		return false;
	}

	CScalarCmp *popScCmp = CScalarCmp::PopConvert(pop);
	GPOS_ASSERT(NULL != popScCmp);

	return cmp_type == popScCmp->ParseCmpType();
}

// Is the given expression a comparison over the given column. A comparison
// can only be between the given column and an expression involving only
// the allowed columns. If the allowed columns set is NULL, then we only want
// constant comparisons.
BOOL CPredicateUtils::FComparison(CExpression* pexpr, CColRef *colref, CColRefSet* pcrsAllowedRefs)
{
	GPOS_ASSERT(NULL != pexpr);

	if (COperator::EopScalarCmp != pexpr->Pop()->Eopid())
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	if (CUtils::FScalarIdent(pexprLeft, colref) ||
		CScalarIdent::FCastedScId(pexprLeft, colref))
	{
		return FValidRefsOnly(pexprRight, pcrsAllowedRefs);
	}

	if (CUtils::FScalarIdent(pexprRight, colref) ||
		CScalarIdent::FCastedScId(pexprRight, colref))
	{
		return FValidRefsOnly(pexprLeft, pcrsAllowedRefs);
	}

	return false;
}

// Is the given expression a range comparison only between the given column and
// an expression involving only the allowed columns. If the allowed columns set
// is NULL, then we only want constant comparisons.
// Also, the comparison type must be one of: LT, GT, LEq, GEq, Eq
BOOL
CPredicateUtils::FRangeComparison(
	CExpression *pexpr, CColRef *colref,
	CColRefSet
		*pcrsAllowedRefs  // other column references allowed in the comparison
)
{
	if (!FComparison(pexpr, colref, pcrsAllowedRefs))
	{
		return false;
	}
	IMDType::ECmpType cmp_type =
		CScalarCmp::PopConvert(pexpr->Pop())->ParseCmpType();
	return (IMDType::EcmptOther != cmp_type && IMDType::EcmptNEq != cmp_type);
}

BOOL
CPredicateUtils::FIdentCompareOuterRefExprIgnoreCast(
	CExpression *pexpr,
	CColRefSet
		*pcrsOuterRefs,	 // other column references allowed in the comparison
	CColRef **localColRef)
{
	GPOS_ASSERT(NULL != pexpr);

	if (COperator::EopScalarCmp != pexpr->Pop()->Eopid() ||
		NULL == pcrsOuterRefs)
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	BOOL leftIsACol = CUtils::FScalarIdentIgnoreCast(pexprLeft);
	BOOL rightIsACol = CUtils::FScalarIdentIgnoreCast(pexprRight);
	CColRefSet *pcrsUsedLeft = pexprLeft->DeriveUsedColumns();
	CColRefSet *pcrsUsedRight = pexprRight->DeriveUsedColumns();

	// allow any expressions of the form
	//  col = expr(outer refs)
	//  expr(outer refs) = col
	BOOL colOpOuterrefExpr =
		(leftIsACol && !pcrsOuterRefs->FIntersects(pcrsUsedLeft) &&
		 pcrsOuterRefs->ContainsAll(pcrsUsedRight));
	BOOL outerRefExprOpCol =
		(rightIsACol && !pcrsOuterRefs->FIntersects(pcrsUsedRight) &&
		 pcrsOuterRefs->ContainsAll(pcrsUsedLeft));

	if (NULL != localColRef)
	{
		if (colOpOuterrefExpr)
		{
			GPOS_ASSERT(pcrsUsedLeft->Size() == 1);
			*localColRef = pcrsUsedLeft->PcrFirst();
		}
		else if (outerRefExprOpCol)
		{
			GPOS_ASSERT(pcrsUsedRight->Size() == 1);
			*localColRef = pcrsUsedRight->PcrFirst();
		}
		else
		{
			// return value with be false, initialize the variable to be nice
			*localColRef = NULL;
		}
	}

	return colOpOuterrefExpr || outerRefExprOpCol;
}

// Check whether the given expression contains references to only the given
// columns. If pcrsAllowedRefs is NULL, then check whether the expression has
// no column references and no volatile functions
BOOL CPredicateUtils::FValidRefsOnly(CExpression *pexprScalar, CColRefSet *pcrsAllowedRefs)
{
	if (NULL != pcrsAllowedRefs)
	{
		return pcrsAllowedRefs->ContainsAll(pexprScalar->DeriveUsedColumns());
	}

	return CUtils::FVarFreeExpr(pexprScalar) && IMDFunction::EfsVolatile != pexprScalar->DeriveScalarFunctionProperties()->Efs();
}



// is the given expression a conjunction of equality comparisons
BOOL CPredicateUtils::FConjunctionOfEqComparisons(CMemoryPool *mp, CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	if (IsEqualityOp(pexpr))
	{
		return true;
	}

	CExpressionArray *pdrgpexpr = PdrgpexprConjuncts(mp, pexpr);
	const ULONG ulConjuncts = pdrgpexpr->Size();

	for (ULONG ul = 0; ul < ulConjuncts; ul++)
	{
		if (!IsEqualityOp((*pexpr)[ul]))
		{
			pdrgpexpr->Release();
			return false;
		}
	}

	pdrgpexpr->Release();
	return true;
}

// does the given expression have any NOT children?
BOOL
CPredicateUtils::FHasNegatedChild(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (FNot((*pexpr)[ul]))
		{
			return true;
		}
	}

	return false;
}

// recursively collect conjuncts
void
CPredicateUtils::CollectConjuncts(CExpression *pexpr,
								  CExpressionArray *pdrgpexpr)
{
	GPOS_CHECK_STACK_SIZE;

	if (FAnd(pexpr))
	{
		const ULONG arity = pexpr->Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			CollectConjuncts((*pexpr)[ul], pdrgpexpr);
		}
	}
	else
	{
		pexpr->AddRef();
		pdrgpexpr->Append(pexpr);
	}
}

// recursively collect disjuncts
void
CPredicateUtils::CollectDisjuncts(CExpression *pexpr,
								  CExpressionArray *pdrgpexpr)
{
	GPOS_CHECK_STACK_SIZE;

	if (FOr(pexpr))
	{
		const ULONG arity = pexpr->Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			CollectDisjuncts((*pexpr)[ul], pdrgpexpr);
		}
	}
	else
	{
		pexpr->AddRef();
		pdrgpexpr->Append(pexpr);
	}
}

// extract conjuncts from a predicate
CExpressionArray *
CPredicateUtils::PdrgpexprConjuncts(CMemoryPool *mp, CExpression *pexpr)
{
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	CollectConjuncts(pexpr, pdrgpexpr);

	return pdrgpexpr;
}

// extract disjuncts from a predicate
CExpressionArray *
CPredicateUtils::PdrgpexprDisjuncts(CMemoryPool *mp, CExpression *pexpr)
{
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	CollectDisjuncts(pexpr, pdrgpexpr);

	return pdrgpexpr;
}

// This function expects an array of disjuncts (children of OR operator),
// the function expands disjuncts in the given array by converting
// ArrayComparison to AND/OR tree and deduplicating resulting disjuncts
CExpressionArray *
CPredicateUtils::PdrgpexprExpandDisjuncts(CMemoryPool *mp,
										  CExpressionArray *pdrgpexprDisjuncts)
{
	GPOS_ASSERT(NULL != pdrgpexprDisjuncts);

	CExpressionArray *pdrgpexprExpanded = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG size = pdrgpexprDisjuncts->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *pexpr = (*pdrgpexprDisjuncts)[ul];
		if (COperator::EopScalarArrayCmp == pexpr->Pop()->Eopid())
		{
			CExpression *pexprExpanded =
				CScalarArrayCmp::PexprExpand(mp, pexpr);
			if (FOr(pexprExpanded))
			{
				CExpressionArray *pdrgpexprArrayCmpDisjuncts =
					PdrgpexprDisjuncts(mp, pexprExpanded);
				CUtils::AddRefAppend<CExpression, CleanupRelease>(
					pdrgpexprExpanded, pdrgpexprArrayCmpDisjuncts);
				pdrgpexprArrayCmpDisjuncts->Release();
				pexprExpanded->Release();
			}
			else
			{
				pdrgpexprExpanded->Append(pexprExpanded);
			}

			continue;
		}

		if (FAnd(pexpr))
		{
			CExpressionArray *pdrgpexprConjuncts =
				PdrgpexprConjuncts(mp, pexpr);
			CExpressionArray *pdrgpexprExpandedConjuncts =
				PdrgpexprExpandConjuncts(mp, pdrgpexprConjuncts);
			pdrgpexprConjuncts->Release();
			pdrgpexprExpanded->Append(
				PexprConjunction(mp, pdrgpexprExpandedConjuncts));

			continue;
		}

		pexpr->AddRef();
		pdrgpexprExpanded->Append(pexpr);
	}

	CExpressionArray *pdrgpexprResult =
		CUtils::PdrgpexprDedup(mp, pdrgpexprExpanded);
	pdrgpexprExpanded->Release();

	return pdrgpexprResult;
}

// This function expects an array of conjuncts (children of AND operator),
// the function expands conjuncts in the given array by converting
// ArrayComparison to AND/OR tree and deduplicating resulting conjuncts
CExpressionArray *
CPredicateUtils::PdrgpexprExpandConjuncts(CMemoryPool *mp,
										  CExpressionArray *pdrgpexprConjuncts)
{
	GPOS_ASSERT(NULL != pdrgpexprConjuncts);

	CExpressionArray *pdrgpexprExpanded = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG size = pdrgpexprConjuncts->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *pexpr = (*pdrgpexprConjuncts)[ul];
		if (COperator::EopScalarArrayCmp == pexpr->Pop()->Eopid())
		{
			CExpression *pexprExpanded =
				CScalarArrayCmp::PexprExpand(mp, pexpr);
			if (FAnd(pexprExpanded))
			{
				CExpressionArray *pdrgpexprArrayCmpConjuncts =
					PdrgpexprConjuncts(mp, pexprExpanded);
				CUtils::AddRefAppend<CExpression, CleanupRelease>(
					pdrgpexprExpanded, pdrgpexprArrayCmpConjuncts);
				pdrgpexprArrayCmpConjuncts->Release();
				pexprExpanded->Release();
			}
			else
			{
				pdrgpexprExpanded->Append(pexprExpanded);
			}

			continue;
		}

		if (FOr(pexpr))
		{
			CExpressionArray *pdrgpexprDisjuncts =
				PdrgpexprDisjuncts(mp, pexpr);
			CExpressionArray *pdrgpexprExpandedDisjuncts =
				PdrgpexprExpandDisjuncts(mp, pdrgpexprDisjuncts);
			pdrgpexprDisjuncts->Release();
			pdrgpexprExpanded->Append(
				PexprDisjunction(mp, pdrgpexprExpandedDisjuncts));

			continue;
		}

		pexpr->AddRef();
		pdrgpexprExpanded->Append(pexpr);
	}

	CExpressionArray *pdrgpexprResult =
		CUtils::PdrgpexprDedup(mp, pdrgpexprExpanded);
	pdrgpexprExpanded->Release();

	return pdrgpexprResult;
}

// check if a conjunct/disjunct can be skipped
BOOL
CPredicateUtils::FSkippable(CExpression *pexpr, BOOL fConjunction)
{
	return ((fConjunction && CUtils::FScalarConstTrue(pexpr)) ||
			(!fConjunction && CUtils::FScalarConstFalse(pexpr)));
}

// check if a conjunction/disjunction can be reduced to a constant
// True/False based on the given conjunct/disjunct
BOOL
CPredicateUtils::FReducible(CExpression *pexpr, BOOL fConjunction)
{
	return ((fConjunction && CUtils::FScalarConstFalse(pexpr)) ||
			(!fConjunction && CUtils::FScalarConstTrue(pexpr)));
}

// reverse the given operator type, for example > => <, <= => >=
IMDType::ECmpType
CPredicateUtils::EcmptReverse(IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);

	IMDType::ECmpType rgrgecmpt[][2] = {{IMDType::EcmptEq, IMDType::EcmptEq},
										{IMDType::EcmptG, IMDType::EcmptL},
										{IMDType::EcmptGEq, IMDType::EcmptLEq},
										{IMDType::EcmptNEq, IMDType::EcmptNEq}};

	const ULONG size = GPOS_ARRAY_SIZE(rgrgecmpt);

	for (ULONG ul = 0; ul < size; ul++)
	{
		IMDType::ECmpType *pecmpt = rgrgecmpt[ul];

		if (pecmpt[0] == cmp_type)
		{
			return pecmpt[1];
		}

		if (pecmpt[1] == cmp_type)
		{
			return pecmpt[0];
		}
	}

	GPOS_ASSERT(!"Comparison does not have a reverse");

	return IMDType::EcmptOther;
}

// is the condition a LIKE predicate
BOOL
CPredicateUtils::FLikePredicate(IMDId *mdid)
{
	GPOS_ASSERT(NULL != mdid);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDScalarOp *md_scalar_op = md_accessor->RetrieveScOp(mdid);

	const CWStringConst *str_opname = md_scalar_op->Mdname().GetMDName();

	// comparison semantics for statistics purposes is looser
	// than regular comparison
	CWStringConst pstrLike(GPOS_WSZ_LIT("~~"));
	if (!str_opname->Equals(&pstrLike))
	{
		return false;
	}

	return true;
}

// is the condition a LIKE predicate
BOOL
CPredicateUtils::FLikePredicate(CExpression *pexpr)
{
	COperator *pop = pexpr->Pop();
	if (COperator::EopScalarCmp != pop->Eopid())
	{
		return false;
	}

	CScalarCmp *popScCmp = CScalarCmp::PopConvert(pop);
	IMDId *mdid = popScCmp->MdIdOp();

	return FLikePredicate(mdid);
}

// extract the components of a LIKE predicate
void
CPredicateUtils::ExtractLikePredComponents(CExpression *pexprPred,
										   CExpression **ppexprScIdent,
										   CExpression **ppexprConst)
{
	GPOS_ASSERT(NULL != pexprPred);
	GPOS_ASSERT(2 == pexprPred->Arity());
	GPOS_ASSERT(FLikePredicate(pexprPred));

	CExpression *pexprLeft = (*pexprPred)[0];
	CExpression *pexprRight = (*pexprPred)[1];

	*ppexprScIdent = NULL;
	*ppexprConst = NULL;

	CExpression *pexprLeftNoCast = pexprLeft;
	CExpression *pexprRightNoCast = pexprRight;

	if (COperator::EopScalarCast == pexprLeft->Pop()->Eopid())
	{
		pexprLeftNoCast = (*pexprLeft)[0];
	}

	if (COperator::EopScalarCast == pexprRight->Pop()->Eopid())
	{
		pexprRightNoCast = (*pexprRight)[0];
	}

	if (COperator::EopScalarIdent == pexprLeftNoCast->Pop()->Eopid())
	{
		*ppexprScIdent = pexprLeftNoCast;
	}
	else if (COperator::EopScalarIdent == pexprRightNoCast->Pop()->Eopid())
	{
		*ppexprScIdent = pexprRightNoCast;
	}

	if (COperator::EopScalarConst == pexprLeftNoCast->Pop()->Eopid())
	{
		*ppexprConst = pexprLeftNoCast;
	}
	else if (COperator::EopScalarConst == pexprRightNoCast->Pop()->Eopid())
	{
		*ppexprConst = pexprRightNoCast;
	}
}

// extract components in a comparison expression on the given key
void
CPredicateUtils::ExtractComponents(CExpression *pexprScCmp, CColRef *pcrKey,
								   CExpression **ppexprKey,
								   CExpression **ppexprOther,
								   IMDType::ECmpType *pecmpt)
{
	GPOS_ASSERT(NULL != pexprScCmp);
	GPOS_ASSERT(NULL != pcrKey);
	GPOS_ASSERT(FComparison(pexprScCmp));

	*ppexprKey = NULL;
	*ppexprOther = NULL;

	CExpression *pexprLeft = (*pexprScCmp)[0];
	CExpression *pexprRight = (*pexprScCmp)[1];

	IMDType::ECmpType cmp_type =
		CScalarCmp::PopConvert(pexprScCmp->Pop())->ParseCmpType();

	if (CUtils::FScalarIdent(pexprLeft, pcrKey) ||
		CScalarIdent::FCastedScId(pexprLeft, pcrKey))
	{
		*ppexprKey = pexprLeft;
		*ppexprOther = pexprRight;
		*pecmpt = cmp_type;
	}
	else if (CUtils::FScalarIdent(pexprRight, pcrKey) ||
			 CScalarIdent::FCastedScId(pexprRight, pcrKey))
	{
		*ppexprKey = pexprRight;
		*ppexprOther = pexprLeft;
		*pecmpt = EcmptReverse(cmp_type);
	}
	GPOS_ASSERT(NULL != *ppexprKey && NULL != *ppexprOther);
}

// Expression is a comparison with a simple identifer on at least one side
BOOL
CPredicateUtils::FIdentCompare(CExpression *pexpr, IMDType::ECmpType pecmpt,
							   CColRef *colref)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != colref);

	if (!FComparison(pexpr, pecmpt))
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	if (CUtils::FScalarIdent(pexprLeft, colref) ||
		CCastUtils::FBinaryCoercibleCastedScId(pexprLeft, colref))
	{
		return true;
	}
	else if (CUtils::FScalarIdent(pexprRight, colref) ||
			 CCastUtils::FBinaryCoercibleCastedScId(pexprRight, colref))
	{
		return true;
	}

	return false;
}

// create conjunction/disjunction from array of components; Takes ownership over the given array of expressions
CExpression *
CPredicateUtils::PexprConjDisj(CMemoryPool *mp, CExpressionArray *pdrgpexpr,
							   BOOL fConjunction)
{
	CScalarBoolOp::EBoolOperator eboolop = CScalarBoolOp::EboolopAnd;
	if (!fConjunction)
	{
		eboolop = CScalarBoolOp::EboolopOr;
	}

	CExpressionArray *pdrgpexprFinal = GPOS_NEW(mp) CExpressionArray(mp);
	ULONG size = 0;
	if (NULL != pdrgpexpr)
	{
		size = pdrgpexpr->Size();
	}

	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *pexpr = (*pdrgpexpr)[ul];

		if (FSkippable(pexpr, fConjunction))
		{
			// skip current conjunct/disjunct
			continue;
		}

		if (FReducible(pexpr, fConjunction))
		{
			// a False (True) conjunct (disjunct) yields the whole conjunction (disjunction) False (True)
			CRefCount::SafeRelease(pdrgpexpr);
			CRefCount::SafeRelease(pdrgpexprFinal);

			return CUtils::PexprScalarConstBool(mp, !fConjunction /*fValue*/);
		}

		// add conjunct/disjunct to result array
		pexpr->AddRef();
		pdrgpexprFinal->Append(pexpr);
	}
	CRefCount::SafeRelease(pdrgpexpr);

	// assemble result
	CExpression *pexprResult = NULL;
	if (NULL != pdrgpexprFinal && (0 < pdrgpexprFinal->Size()))
	{
		if (1 == pdrgpexprFinal->Size())
		{
			pexprResult = (*pdrgpexprFinal)[0];
			pexprResult->AddRef();
			pdrgpexprFinal->Release();

			return pexprResult;
		}

		return CUtils::PexprScalarBoolOp(mp, eboolop, pdrgpexprFinal);
	}

	pexprResult = CUtils::PexprScalarConstBool(mp, fConjunction /*fValue*/);
	CRefCount::SafeRelease(pdrgpexprFinal);

	return pexprResult;
}

// create conjunction from array of components;
CExpression *
CPredicateUtils::PexprConjunction(CMemoryPool *mp, CExpressionArray *pdrgpexpr)
{
	return PexprConjDisj(mp, pdrgpexpr, true /*fConjunction*/);
}

// create disjunction from array of components;
CExpression *
CPredicateUtils::PexprDisjunction(CMemoryPool *mp, CExpressionArray *pdrgpexpr)
{
	return PexprConjDisj(mp, pdrgpexpr, false /*fConjunction*/);
}

// create a conjunction/disjunction of two components; Does *not* take ownership over given expressions
CExpression *
CPredicateUtils::PexprConjDisj(CMemoryPool *mp, CExpression *pexprOne,
							   CExpression *pexprTwo, BOOL fConjunction)
{
	GPOS_ASSERT(NULL != pexprOne);
	GPOS_ASSERT(NULL != pexprTwo);

	if (pexprOne == pexprTwo)
	{
		pexprOne->AddRef();
		return pexprOne;
	}

	CExpressionArray *pdrgpexprOne = NULL;
	CExpressionArray *pdrgpexprTwo = NULL;

	if (fConjunction)
	{
		pdrgpexprOne = PdrgpexprConjuncts(mp, pexprOne);
		pdrgpexprTwo = PdrgpexprConjuncts(mp, pexprTwo);
	}
	else
	{
		pdrgpexprOne = PdrgpexprDisjuncts(mp, pexprOne);
		pdrgpexprTwo = PdrgpexprDisjuncts(mp, pexprTwo);
	}

	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	CUtils::AddRefAppend<CExpression, CleanupRelease>(pdrgpexpr, pdrgpexprOne);
	CUtils::AddRefAppend<CExpression, CleanupRelease>(pdrgpexpr, pdrgpexprTwo);

	pdrgpexprOne->Release();
	pdrgpexprTwo->Release();

	return PexprConjDisj(mp, pdrgpexpr, fConjunction);
}

// create a conjunction of two components;
CExpression *
CPredicateUtils::PexprConjunction(CMemoryPool *mp, CExpression *pexprOne,
								  CExpression *pexprTwo)
{
	return PexprConjDisj(mp, pexprOne, pexprTwo, true /*fConjunction*/);
}

// create a disjunction of two components;
CExpression *
CPredicateUtils::PexprDisjunction(CMemoryPool *mp, CExpression *pexprOne,
								  CExpression *pexprTwo)
{
	return PexprConjDisj(mp, pexprOne, pexprTwo, false /*fConjunction*/);
}

// extract equality predicates over scalar identifiers
CExpressionArray *
CPredicateUtils::PdrgpexprPlainEqualities(CMemoryPool *mp,
										  CExpressionArray *pdrgpexpr)
{
	CExpressionArray *pdrgpexprEqualities = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG arity = pdrgpexpr->Size();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprCurr = (*pdrgpexpr)[ul];

		if (FPlainEquality(pexprCurr))
		{
			pexprCurr->AddRef();
			pdrgpexprEqualities->Append(pexprCurr);
		}
	}
	return pdrgpexprEqualities;
}

// is an expression an equality over scalar identifiers
BOOL
CPredicateUtils::FPlainEquality(CExpression *pexpr)
{
	if (IsEqualityOp(pexpr))
	{
		CExpression *pexprLeft = (*pexpr)[0];
		CExpression *pexprRight = (*pexpr)[1];

		// check if the scalar condition is over scalar idents
		if (COperator::EopScalarIdent == pexprLeft->Pop()->Eopid() &&
			COperator::EopScalarIdent == pexprRight->Pop()->Eopid())
		{
			return true;
		}
	}
	return false;
}

// is an expression a self comparison on some column
BOOL
CPredicateUtils::FSelfComparison(CExpression *pexpr, IMDType::ECmpType *pecmpt)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pecmpt);

	*pecmpt = IMDType::EcmptOther;
	COperator *pop = pexpr->Pop();
	if (CUtils::FScalarCmp(pexpr))
	{
		*pecmpt = CScalarCmp::PopConvert(pop)->ParseCmpType();
		COperator *popLeft = (*pexpr)[0]->Pop();
		COperator *popRight = (*pexpr)[1]->Pop();

		// return true if comparison is over the same column, and that column
		// is not nullable
		if (COperator::EopScalarIdent != popLeft->Eopid() ||
			COperator::EopScalarIdent != popRight->Eopid() ||
			CScalarIdent::PopConvert(popLeft)->Pcr() !=
				CScalarIdent::PopConvert(popRight)->Pcr())
		{
			return false;
		}

		CColRef *colref =
			const_cast<CColRef *>(CScalarIdent::PopConvert(popLeft)->Pcr());

		return CColRef::EcrtTable == colref->Ecrt() &&
			   !CColRefTable::PcrConvert(colref)->IsNullable();
	}

	return false;
}

// eliminate self comparison and replace it with True or False if possible
CExpression *
CPredicateUtils::PexprEliminateSelfComparison(CMemoryPool *mp,
											  CExpression *pexpr)
{
	GPOS_ASSERT(pexpr->Pop()->FScalar());

	pexpr->AddRef();
	CExpression *pexprNew = pexpr;
	IMDType::ECmpType cmp_type = IMDType::EcmptOther;
	if (FSelfComparison(pexpr, &cmp_type))
	{
		switch (cmp_type)
		{
			case IMDType::EcmptEq:
			case IMDType::EcmptLEq:
			case IMDType::EcmptGEq:
				pexprNew->Release();
				pexprNew = CUtils::PexprScalarConstBool(mp, true /*value*/);
				break;

			case IMDType::EcmptNEq:
			case IMDType::EcmptL:
			case IMDType::EcmptG:
			case IMDType::EcmptIDF:
				pexprNew->Release();
				pexprNew = CUtils::PexprScalarConstBool(mp, false /*value*/);
				break;

			default:
				break;
		}
	}

	return pexprNew;
}

// is the given expression in the form (col1 Is NOT DISTINCT FROM col2)
BOOL
CPredicateUtils::FINDFScalarIdents(CExpression *pexpr)
{
	if (!FNot(pexpr))
	{
		return false;
	}

	CExpression *pexprChild = (*pexpr)[0];
	if (COperator::EopScalarIsDistinctFrom != pexprChild->Pop()->Eopid())
	{
		return false;
	}

	CExpression *pexprOuter = (*pexprChild)[0];
	CExpression *pexprInner = (*pexprChild)[1];

	return (COperator::EopScalarIdent == pexprOuter->Pop()->Eopid() &&
			COperator::EopScalarIdent == pexprInner->Pop()->Eopid());
}

// is the given expression in the form (col1 Is DISTINCT FROM col2)
BOOL
CPredicateUtils::FIDFScalarIdents(CExpression *pexpr)
{
	if (COperator::EopScalarIsDistinctFrom != pexpr->Pop()->Eopid())
	{
		return false;
	}

	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprInner = (*pexpr)[1];

	return (COperator::EopScalarIdent == pexprOuter->Pop()->Eopid() &&
			COperator::EopScalarIdent == pexprInner->Pop()->Eopid());
}

// is the given expression in the form 'expr IS DISTINCT FROM false)'
BOOL
CPredicateUtils::FIDFFalse(CExpression *pexpr)
{
	if (COperator::EopScalarIsDistinctFrom != pexpr->Pop()->Eopid())
	{
		return false;
	}

	return CUtils::FScalarConstFalse((*pexpr)[0]) ||
		   CUtils::FScalarConstFalse((*pexpr)[1]);
}

// is the given expression in the form (expr IS DISTINCT FROM expr)
BOOL
CPredicateUtils::FIDF(CExpression *pexpr)
{
	return (COperator::EopScalarIsDistinctFrom == pexpr->Pop()->Eopid());
}

// is the given expression in the form (expr Is NOT DISTINCT FROM expr)
BOOL
CPredicateUtils::FINDF(CExpression *pexpr)
{
	return (FNot(pexpr) && FIDF((*pexpr)[0]));
}

// generate a conjunction of INDF expressions between corresponding columns in the given arrays
CExpression *
CPredicateUtils::PexprINDFConjunction(CMemoryPool *mp,
									  CColRefArray *pdrgpcrFirst,
									  CColRefArray *pdrgpcrSecond)
{
	GPOS_ASSERT(NULL != pdrgpcrFirst);
	GPOS_ASSERT(NULL != pdrgpcrSecond);
	GPOS_ASSERT(pdrgpcrFirst->Size() == pdrgpcrSecond->Size());
	GPOS_ASSERT(0 < pdrgpcrFirst->Size());

	const ULONG num_cols = pdrgpcrFirst->Size();
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		pdrgpexpr->Append(
			CUtils::PexprINDF(mp, (*pdrgpcrFirst)[ul], (*pdrgpcrSecond)[ul]));
	}

	return PexprConjunction(mp, pdrgpexpr);
}

// is the given expression a comparison between a scalar ident and a constant
BOOL
CPredicateUtils::FCompareIdentToConst(CExpression *pexpr)
{
	COperator *pop = pexpr->Pop();

	if (COperator::EopScalarCmp != pop->Eopid())
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	// left side must be scalar ident

	if (!(CUtils::FScalarIdent(pexprLeft) ||
		  CCastUtils::FBinaryCoercibleCastedScId(pexprLeft)))
	{
		return false;
	}

	// right side must be a constant
	if (!(CUtils::FScalarConst(pexprRight) ||
		  CCastUtils::FBinaryCoercibleCastedConst(pexprRight)))
	{
		return false;
	}

	return true;
}

// is the given expression of the form (col IS DISTINCT FROM const)
BOOL
CPredicateUtils::FIdentIDFConst(CExpression *pexpr)
{
	COperator *pop = pexpr->Pop();

	if (COperator::EopScalarIsDistinctFrom != pop->Eopid())
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	// left side must be scalar ident
	if (COperator::EopScalarIdent != pexprLeft->Pop()->Eopid())
	{
		return false;
	}

	// right side must be a constant
	if (COperator::EopScalarConst != pexprRight->Pop()->Eopid())
	{
		return false;
	}

	return true;
}

// is the given expression of the form (col = col)
BOOL
CPredicateUtils::FEqIdentsOfSameType(CExpression *pexpr)
{
	if (!CPredicateUtils::IsEqualityOp(pexpr))
	{
		return false;
	}
	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	// left side must be scalar ident
	if (COperator::EopScalarIdent != pexprLeft->Pop()->Eopid())
	{
		return false;
	}

	// right side must be a scalar ident
	if (COperator::EopScalarIdent != pexprRight->Pop()->Eopid())
	{
		return false;
	}

	CScalarIdent *left_ident = CScalarIdent::PopConvert(pexprLeft->Pop());
	CScalarIdent *right_ident = CScalarIdent::PopConvert(pexprRight->Pop());
	if (!left_ident->MdidType()->Equals(right_ident->MdidType()))
	{
		return false;
	}

	return true;
}


// is the given expression is of the form (col IS DISTINCT FROM const)
// ignoring cast on either sides
BOOL
CPredicateUtils::FIdentIDFConstIgnoreCast(CExpression *pexpr)
{
	return FIdentCompareConstIgnoreCast(pexpr,
										COperator::EopScalarIsDistinctFrom);
}

// is the given expression of the form (col cmp constant) ignoring casting on either sides
BOOL
CPredicateUtils::FIdentCompareConstIgnoreCast(CExpression *pexpr,
											  COperator::EOperatorId op_id)
{
	COperator *pop = pexpr->Pop();

	if (op_id != pop->Eopid())
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	// col IDF const
	if (COperator::EopScalarIdent == pexprLeft->Pop()->Eopid() &&
		COperator::EopScalarConst == pexprRight->Pop()->Eopid())
	{
		return true;
	}

	// cast(col) IDF const
	if (CScalarIdent::FCastedScId(pexprLeft) &&
		COperator::EopScalarConst == pexprRight->Pop()->Eopid())
	{
		return true;
	}

	// col IDF cast(constant)
	if (COperator::EopScalarIdent == pexprLeft->Pop()->Eopid() &&
		CScalarConst::FCastedConst(pexprRight))
	{
		return true;
	}

	// cast(col) IDF cast(constant)
	if (CScalarIdent::FCastedScId(pexprLeft) &&
		CScalarConst::FCastedConst(pexprRight))
	{
		return true;
	}

	return false;
}

// is the given expression a comparison between a const and a const
BOOL
CPredicateUtils::FCompareConstToConstIgnoreCast(CExpression *pexpr)
{
	COperator *pop = pexpr->Pop();

	if (COperator::EopScalarCmp != pop->Eopid())
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	// left side must be scalar const

	if (!(CUtils::FScalarConst(pexprLeft) ||
		  CCastUtils::FBinaryCoercibleCastedConst(pexprLeft)))
	{
		return false;
	}

	// right side must be a constant
	if (!(CUtils::FScalarConst(pexprRight) ||
		  CCastUtils::FBinaryCoercibleCastedConst(pexprRight)))
	{
		return false;
	}

	return true;
}

// is the given expression an array comparison between scalar ident
// and a const array or a constant
BOOL
CPredicateUtils::FArrayCompareIdentToConstIgnoreCast(CExpression *pexpr)
{
	if (!CUtils::FScalarArrayCmp(pexpr))
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	if (CUtils::FScalarIdentIgnoreCast(pexprLeft))
	{
		if ((CUtils::FScalarArray(pexprRight) ||
			 CUtils::FScalarArrayCoerce(pexprRight)))
		{
			CExpression *pexprArray = CUtils::PexprScalarArrayChild(pexpr);
			return CUtils::FScalarConstArray(pexprArray);
		}
		return CUtils::FScalarConst(pexprRight) ||
			   CScalarConst::FCastedConst(pexprRight);
	}

	return false;
}

// is the given expression of the form NOT (col IS DISTINCT FROM const) ignoring cast on either sides
BOOL
CPredicateUtils::FIdentINDFConstIgnoreCast(CExpression *pexpr)
{
	if (!FNot(pexpr))
	{
		return false;
	}

	return FIdentCompareConstIgnoreCast((*pexpr)[0],
										COperator::EopScalarIsDistinctFrom);
}

// is the given expression a comparison between a scalar ident under a scalar cast and a constant array
// +--CScalarArrayCmp Any (=)
// |--CScalarCast
// |  +--CScalarIdent
// +--CScalarConstArray:
BOOL
CPredicateUtils::FCompareCastIdentToConstArray(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	if (CUtils::FScalarArrayCmp(pexpr) &&
		(CCastUtils::FBinaryCoercibleCast((*pexpr)[0]) &&
		 CUtils::FScalarIdent((*(*pexpr)[0])[0])))
	{
		CExpression *pexprArray = CUtils::PexprScalarArrayChild(pexpr);
		return CUtils::FScalarConstArray(pexprArray);
	}

	return false;
}

// is the given expression a comparison between a scalar ident and an array with constants or ScalarIdents
BOOL
CPredicateUtils::FCompareScalarIdentToConstAndScalarIdentArray(
	CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	if (!CUtils::FScalarArrayCmp(pexpr) || !CUtils::FScalarIdent((*pexpr)[0]) ||
		!CUtils::FScalarArray((*pexpr)[1]))
	{
		return false;
	}

	CExpression *pexprArray = CUtils::PexprScalarArrayChild(pexpr);
	return CUtils::FScalarConstAndScalarIdentArray(pexprArray);
}

// is the given expression a comparison between a scalar ident and a constant array
BOOL
CPredicateUtils::FCompareIdentToConstArray(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	if (!CUtils::FScalarArrayCmp(pexpr) || !CUtils::FScalarIdent((*pexpr)[0]) ||
		!CUtils::FScalarArray((*pexpr)[1]))
	{
		return false;
	}

	CExpression *pexprArray = CUtils::PexprScalarArrayChild(pexpr);
	return CUtils::FScalarConstArray(pexprArray);
}

// Find a predicate that can be used for partition pruning with the given
// part key in the array of expressions if one exists. Relevant predicates
// are those that compare the partition key to expressions involving only
// the allowed columns. If the allowed columns set is NULL, then we only want
// constant comparisons.
CExpression *
CPredicateUtils::PexprPartPruningPredicate(
	CMemoryPool *mp, const CExpressionArray *pdrgpexpr, CColRef *pcrPartKey,
	CExpression *pexprCol,		 // predicate on pcrPartKey obtained from pcnstr
	CColRefSet *pcrsAllowedRefs	 // allowed colrefs in exprs (except pcrPartKey)
)
{
	CExpressionArray *pdrgpexprResult = GPOS_NEW(mp) CExpressionArray(mp);

	// Assert that pexprCol is an expr on pcrPartKey only and no other colref
	GPOS_ASSERT(pexprCol == NULL || CUtils::FScalarConstTrue(pexprCol) ||
				(pexprCol->DeriveUsedColumns()->Size() == 1 &&
				 pexprCol->DeriveUsedColumns()->PcrFirst() == pcrPartKey));

	for (ULONG ul = 0; ul < pdrgpexpr->Size(); ul++)
	{
		CExpression *pexpr = (*pdrgpexpr)[ul];

		if (NULL != pcrsAllowedRefs &&
			!GPOS_FTRACE(EopttraceAllowGeneralPredicatesforDPE))
		{
			// Only allow equal comparison exprs for dynamic partition selection.

			// This will reduce long execution times for partition selection using
			// non-equality predicates. These are expensive to execute since (at the
			// moment) to determine the selected partitions, the executor must, for
			// each row from its subtree, iterate over all the partition rules, and
			// for each such rule, execute the non-equality predicate. So, in case of
			// a large number of rows and/or large number of partitions the execution
			// time of partition selection may outweigh any potential savings earned
			// from skipping the scans of eliminated partitions.
			if (FComparison(pexpr, pcrPartKey, pcrsAllowedRefs) &&
				!pexpr->DeriveScalarFunctionProperties()
					 ->NeedsSingletonExecution())
			{
				CScalarCmp *popCmp = CScalarCmp::PopConvert(pexpr->Pop());

				if (popCmp->ParseCmpType() == IMDType::EcmptEq)
				{
					pexpr->AddRef();
					pdrgpexprResult->Append(pexpr);
				}
			}

			// pexprCol contains a predicate only on partKey, which is useless for
			// dynamic partition selection, so ignore it here
			pexprCol = NULL;
		}
		else
		{
			// (NULL == pcrsAllowedRefs) implies static partition elimination, since
			// the expressions we select can only contain the partition key
			// If EopttraceAllowGeneralPredicatesforDPE is set, allow a larger set
			// of partition predicates for DPE as well (see note above).

			if (FBoolPredicateOnColumn(pexpr, pcrPartKey) ||
				FNullCheckOnColumn(pexpr, pcrPartKey) ||
				IsDisjunctionOfRangeComparison(mp, pexpr, pcrPartKey,
											   pcrsAllowedRefs) ||
				(FRangeComparison(pexpr, pcrPartKey, pcrsAllowedRefs) &&
				 !pexpr->DeriveScalarFunctionProperties()
					  ->NeedsSingletonExecution()))
			{
				pexpr->AddRef();
				pdrgpexprResult->Append(pexpr);
			}
		}
	}

	// Remove any redundant "IS NOT NULL" filter on the partition key that was derived
	// from contraints
	if (pexprCol != NULL &&
		CPredicateUtils::FNotNullCheckOnColumn(pexprCol, pcrPartKey) &&
		(pdrgpexprResult->Size() > 0 &&
		 ExprsContainsOnlyStrictComparisons(pdrgpexprResult)))
	{
#ifdef GPOS_DEBUG
		CColRefSet *pcrsUsed = CUtils::PcrsExtractColumns(mp, pdrgpexprResult);
		GPOS_ASSERT_IMP(pdrgpexprResult->Size() > 0,
						pcrsUsed->FMember(pcrPartKey));
		CRefCount::SafeRelease(pcrsUsed);
#endif
		// pexprCol is a redundent "IS NOT NULL" expr. Ignore it
		pexprCol = NULL;
	}

	// Finally, remove duplicate expressions
	CExpressionArray *pdrgpexprResultNew =
		PdrgpexprAppendConjunctsDedup(mp, pdrgpexprResult, pexprCol);
	pdrgpexprResult->Release();
	pdrgpexprResult = pdrgpexprResultNew;

	if (0 == pdrgpexprResult->Size())
	{
		pdrgpexprResult->Release();
		return NULL;
	}

	return PexprConjunction(mp, pdrgpexprResult);
}

// append the conjuncts from the given expression to the given array, removing
// any duplicates, and return the resulting array
CExpressionArray *
CPredicateUtils::PdrgpexprAppendConjunctsDedup(CMemoryPool *mp,
											   CExpressionArray *pdrgpexpr,
											   CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pdrgpexpr);

	if (NULL == pexpr)
	{
		pdrgpexpr->AddRef();
		return pdrgpexpr;
	}

	CExpressionArray *pdrgpexprConjuncts = PdrgpexprConjuncts(mp, pexpr);
	CUtils::AddRefAppend(pdrgpexprConjuncts, pdrgpexpr);

	CExpressionArray *pdrgpexprNew =
		CUtils::PdrgpexprDedup(mp, pdrgpexprConjuncts);
	pdrgpexprConjuncts->Release();
	return pdrgpexprNew;
}

// check if the given expression is a boolean expression on the
// given column, e.g. if its of the form "ScalarIdent(colref)" or "Not(ScalarIdent(colref))"
BOOL
CPredicateUtils::FBoolPredicateOnColumn(CExpression *pexpr, CColRef *colref)
{
	BOOL fBoolean =
		(IMDType::EtiBool == colref->RetrieveType()->GetDatumType());

	if (fBoolean &&
		(CUtils::FScalarIdent(pexpr, colref) ||
		 (FNot(pexpr) && CUtils::FScalarIdent((*pexpr)[0], colref))))
	{
		return true;
	}

	return false;
}

// check if the given expression is a null check on the given column
// i.e. "is null" or "is not null"
BOOL
CPredicateUtils::FNullCheckOnColumn(CExpression *pexpr, CColRef *colref)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != colref);

	CExpression *pexprIsNull = pexpr;
	if (FNot(pexpr))
	{
		pexprIsNull = (*pexpr)[0];
	}

	if (CUtils::FScalarNullTest(pexprIsNull))
	{
		CExpression *pexprChild = (*pexprIsNull)[0];
		return (CUtils::FScalarIdent(pexprChild, colref) ||
				CCastUtils::FBinaryCoercibleCastedScId(pexprChild, colref));
	}

	return false;
}

// check if the given expression of the form "col is not null"
BOOL
CPredicateUtils::FNotNullCheckOnColumn(CExpression *pexpr, CColRef *colref)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != colref);

	if (0 == pexpr->Arity())
		return false;

	return (FNullCheckOnColumn(pexpr, colref) && FNot(pexpr));
}


// check if the given expression is a scalar array cmp expression on the
// given column
BOOL
CPredicateUtils::FScArrayCmpOnColumn(CExpression *pexpr, CColRef *colref,
									 BOOL fConstOnly)
{
	GPOS_ASSERT(NULL != pexpr);

	if (COperator::EopScalarArrayCmp != pexpr->Pop()->Eopid())
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	if (!CUtils::FScalarIdent(pexprLeft, colref) ||
		!CUtils::FScalarArray(pexprRight))
	{
		return false;
	}

	const ULONG ulArrayElems = pexprRight->Arity();

	BOOL fSupported = true;
	for (ULONG ul = 0; ul < ulArrayElems && fSupported; ul++)
	{
		CExpression *pexprArrayElem = (*pexprRight)[ul];
		if (fConstOnly && !CUtils::FScalarConst(pexprArrayElem))
		{
			fSupported = false;
		}
	}

	return fSupported;
}

// check if the given expression is a disjunction of scalar cmp expression
// on the given column
BOOL
CPredicateUtils::IsDisjunctionOfRangeComparison(CMemoryPool *mp,
												CExpression *pexpr,
												CColRef *colref,
												CColRefSet *pcrsAllowedRefs)
{
	if (!FOr(pexpr))
	{
		return false;
	}

	CExpressionArray *pdrgpexprDisjuncts = PdrgpexprDisjuncts(mp, pexpr);
	const ULONG ulDisjuncts = pdrgpexprDisjuncts->Size();
	for (ULONG ulDisj = 0; ulDisj < ulDisjuncts; ulDisj++)
	{
		CExpression *pexprDisj = (*pdrgpexprDisjuncts)[ulDisj];
		if (!FRangeComparison(pexprDisj, colref, pcrsAllowedRefs))
		{
			pdrgpexprDisjuncts->Release();
			return false;
		}
	}

	pdrgpexprDisjuncts->Release();
	return true;
}

// extract interesting expressions involving the partitioning keys;
// the function Add-Refs the returned copy if not null. Relevant predicates
// are those that compare the partition keys to expressions involving only
// the allowed columns. If the allowed columns set is NULL, then we only want
// constant filters.
CExpression *
CPredicateUtils::PexprExtractPredicatesOnPartKeys(
	CMemoryPool *mp, CExpression *pexprScalar,
	CColRef2dArray *pdrgpdrgpcrPartKeys, CColRefSet *pcrsAllowedRefs,
	BOOL fUseConstraints)
{
	GPOS_ASSERT(NULL != pdrgpdrgpcrPartKeys);
	if (GPOS_FTRACE(EopttraceDisablePartSelection))
	{
		return NULL;
	}

	CExpressionArray *pdrgpexprConjuncts = PdrgpexprConjuncts(mp, pexprScalar);
	CColRefSetArray *pdrgpcrsChild = NULL;
	CConstraint *pcnstr = NULL;
	if (pexprScalar->DeriveHasScalarArrayCmp() &&
		!GPOS_FTRACE(EopttraceArrayConstraints))
	{
		// if we have any Array Comparisons, we expand them into conjunctions/disjunctions
		// of comparison predicates and then reconstruct scalar expression. This is because the
		// DXL translator for partitions would not previously handle array statements
		CExpressionArray *pdrgpexprExpandedConjuncts =
			PdrgpexprExpandConjuncts(mp, pdrgpexprConjuncts);
		pdrgpexprConjuncts->Release();
		CExpression *pexprExpandedScalar =
			PexprConjunction(mp, pdrgpexprExpandedConjuncts);

		// this will no longer contain array statements
		pdrgpexprConjuncts = PdrgpexprConjuncts(mp, pexprExpandedScalar);

		pcnstr = CConstraint::PcnstrFromScalarExpr(mp, pexprExpandedScalar,
												   &pdrgpcrsChild);
		pexprExpandedScalar->Release();
	}
	else
	{
		// skip this step when
		// 1. there are any Array comparisons
		// 2. previously, we expanded array expressions. However, there is now code to handle array
		// constraints in the DXL translator and therefore, it is unnecessary work to expand arrays
		// into disjunctions
		pcnstr =
			CConstraint::PcnstrFromScalarExpr(mp, pexprScalar, &pdrgpcrsChild);
	}
	CRefCount::SafeRelease(pdrgpcrsChild);


	// check if expanded scalar leads to a contradiction in computed constraint
	BOOL fContradiction = (NULL != pcnstr && pcnstr->FContradiction());
	if (fContradiction)
	{
		pdrgpexprConjuncts->Release();
		pcnstr->Release();

		return NULL;
	}

	const ULONG ulLevels = pdrgpdrgpcrPartKeys->Size();
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < ulLevels; ul++)
	{
		CColRef *colref = CUtils::PcrExtractPartKey(pdrgpdrgpcrPartKeys, ul);
		CExpression *pexprCol =
			PexprPredicateCol(mp, pcnstr, colref, fUseConstraints);

		// look for a filter on the part key
		CExpression *pexprCmp = PexprPartPruningPredicate(
			mp, pdrgpexprConjuncts, colref, pexprCol, pcrsAllowedRefs);
		CRefCount::SafeRelease(pexprCol);
		GPOS_ASSERT_IMP(
			NULL != pexprCmp &&
				COperator::EopScalarCmp == pexprCmp->Pop()->Eopid(),
			IMDType::EcmptOther !=
				CScalarCmp::PopConvert(pexprCmp->Pop())->ParseCmpType());

		if (NULL != pexprCmp && !CUtils::FScalarConstTrue(pexprCmp))
		{
			// include comparison predicate if it is non-trivial
			pexprCmp->AddRef();
			pdrgpexpr->Append(pexprCmp);
		}
		CRefCount::SafeRelease(pexprCmp);
	}

	pdrgpexprConjuncts->Release();
	CRefCount::SafeRelease(pcnstr);

	if (0 == pdrgpexpr->Size())
	{
		pdrgpexpr->Release();
		return NULL;
	}

	return PexprConjunction(mp, pdrgpexpr);
}

// extract the constraint on the given column and return the corresponding scalar expression
CExpression *
CPredicateUtils::PexprPredicateCol(CMemoryPool *mp, CConstraint *pcnstr,
								   CColRef *colref, BOOL fUseConstraints)
{
	if (NULL == pcnstr || !fUseConstraints)
	{
		return NULL;
	}

	CExpression *pexprCol = NULL;
	CConstraint *pcnstrCol = pcnstr->Pcnstr(mp, colref);
	if (NULL != pcnstrCol && !pcnstrCol->IsConstraintUnbounded())
	{
		pexprCol = pcnstrCol->PexprScalar(mp);
		pexprCol->AddRef();
	}

	CRefCount::SafeRelease(pcnstrCol);

	return pexprCol;
}

// checks if comparison is between two columns, or a column and a const
BOOL
CPredicateUtils::FCompareColToConstOrCol(CExpression *pexprScalar)
{
	if (!CUtils::FScalarCmp(pexprScalar))
	{
		return false;
	}

	CExpression *pexprLeft = (*pexprScalar)[0];
	CExpression *pexprRight = (*pexprScalar)[1];

	BOOL fColLeft = CUtils::FScalarIdent(pexprLeft);
	BOOL fColRight = CUtils::FScalarIdent(pexprRight);
	BOOL fConstLeft = CUtils::FScalarConst(pexprLeft);
	BOOL fConstRight = CUtils::FScalarConst(pexprRight);

	return (fColLeft && fColRight) || (fColLeft && fConstRight) ||
		   (fConstLeft && fColRight);
}

// checks if the given constraint specifies a constant column
BOOL
CPredicateUtils::FConstColumn(CConstraint *pcnstr, const CColRef *
#ifdef GPOS_DEBUG
													   colref
#endif	// GPOS_DEBUG
)
{
	if (NULL == pcnstr || CConstraint::EctInterval != pcnstr->Ect())
	{
		// no constraint on column or constraint is not an interval
		return false;
	}

	GPOS_ASSERT(pcnstr->FConstraint(colref));

	CConstraintInterval *pcnstrInterval =
		dynamic_cast<CConstraintInterval *>(pcnstr);
	CRangeArray *pdrgprng = pcnstrInterval->Pdrgprng();
	if (1 < pdrgprng->Size())
	{
		return false;
	}

	if (0 == pdrgprng->Size())
	{
		return pcnstrInterval->FIncludesNull();
	}

	GPOS_ASSERT(1 == pdrgprng->Size());

	const CRange *prng = (*pdrgprng)[0];

	return prng->FPoint() && !pcnstrInterval->FIncludesNull();
}

// checks if the given constraint specifies a set of constants for a column
BOOL
CPredicateUtils::FColumnDisjunctionOfConst(CConstraint *pcnstr,
										   const CColRef *colref)
{
	if (FConstColumn(pcnstr, colref))
	{
		return true;
	}

	if (NULL == pcnstr || CConstraint::EctInterval != pcnstr->Ect())
	{
		// no constraint on column or constraint is not an interval
		return false;
	}

	GPOS_ASSERT(pcnstr->FConstraint(colref));

	GPOS_ASSERT(CConstraint::EctInterval == pcnstr->Ect());

	CConstraintInterval *pcnstrInterval =
		dynamic_cast<CConstraintInterval *>(pcnstr);
	return FColumnDisjunctionOfConst(pcnstrInterval, colref);
}

// checks if the given constraint specifies a set of constants for a column
BOOL
CPredicateUtils::FColumnDisjunctionOfConst(CConstraintInterval *pcnstrInterval,
										   const CColRef *
#ifdef GPOS_DEBUG
											   colref
#endif
)
{
	GPOS_ASSERT(pcnstrInterval->FConstraint(colref));

	CRangeArray *pdrgprng = pcnstrInterval->Pdrgprng();

	if (0 == pdrgprng->Size())
	{
		return pcnstrInterval->FIncludesNull();
	}

	GPOS_ASSERT(0 < pdrgprng->Size());

	const ULONG ulRanges = pdrgprng->Size();

	for (ULONG ul = 0; ul < ulRanges; ul++)
	{
		CRange *prng = (*pdrgprng)[ul];
		if (!prng->FPoint())
		{
			return false;
		}
	}

	return true;
}

// helper to create index lookup comparison predicate with index key on left side
CExpression *
CPredicateUtils::PexprIndexLookupKeyOnLeft(CMemoryPool *mp,
										   CMDAccessor *md_accessor,
										   CExpression *pexprScalar,
										   const IMDIndex *pmdindex,
										   CColRefArray *pdrgpcrIndex,
										   CColRefSet *outer_refs)
{
	GPOS_ASSERT(NULL != pexprScalar);

	CExpression *pexprLeft = (*pexprScalar)[0];
	CExpression *pexprRight = (*pexprScalar)[1];

	CColRefSet *pcrsIndex = GPOS_NEW(mp) CColRefSet(mp, pdrgpcrIndex);

	if ((CUtils::FScalarIdent(pexprLeft) &&
		 pcrsIndex->FMember(
			 CScalarIdent::PopConvert(pexprLeft->Pop())->Pcr())) ||
		(CCastUtils::FBinaryCoercibleCastedScId(pexprLeft) &&
		 pcrsIndex->FMember(
			 CScalarIdent::PopConvert((*pexprLeft)[0]->Pop())->Pcr())))
	{
		// left expression is a scalar identifier or casted scalar identifier on an index key
		CColRefSet *pcrsUsedRight = pexprRight->DeriveUsedColumns();
		BOOL fSuccess = true;

		if (0 < pcrsUsedRight->Size())
		{
			if (!pcrsUsedRight->IsDisjoint(pcrsIndex))
			{
				// right argument uses index key, cannot use predicate for index lookup
				fSuccess = false;
			}
			else if (NULL != outer_refs)
			{
				CColRefSet *pcrsOuterRefsRight =
					GPOS_NEW(mp) CColRefSet(mp, *pcrsUsedRight);
				pcrsOuterRefsRight->Difference(pcrsIndex);
				fSuccess = outer_refs->ContainsAll(pcrsOuterRefsRight);
				pcrsOuterRefsRight->Release();
			}
		}

		fSuccess =
			(fSuccess && FCompatibleIndexPredicate(pexprScalar, pmdindex,
												   pdrgpcrIndex, md_accessor));

		if (fSuccess)
		{
			pcrsIndex->Release();
			pexprScalar->AddRef();
			return pexprScalar;
		}
	}

	pcrsIndex->Release();
	return NULL;
}

// helper to create index lookup comparison predicate with index key on right side
CExpression *
CPredicateUtils::PexprIndexLookupKeyOnRight(CMemoryPool *mp,
											CMDAccessor *md_accessor,
											CExpression *pexprScalar,
											const IMDIndex *pmdindex,
											CColRefArray *pdrgpcrIndex,
											CColRefSet *outer_refs)
{
	GPOS_ASSERT(NULL != pexprScalar);

	CExpression *pexprLeft = (*pexprScalar)[0];
	CExpression *pexprRight = (*pexprScalar)[1];
	if (CUtils::FScalarCmp(pexprScalar))
	{
		CScalarCmp *popScCmp = CScalarCmp::PopConvert(pexprScalar->Pop());
		CScalarCmp *popScCmpCommute =
			popScCmp->PopCommutedOp(mp, pexprScalar->Pop());

		if (popScCmpCommute)
		{
			// build new comparison after switching arguments and using commutative comparison operator
			pexprRight->AddRef();
			pexprLeft->AddRef();
			CExpression *pexprCommuted = GPOS_NEW(mp)
				CExpression(mp, popScCmpCommute, pexprRight, pexprLeft);
			CExpression *pexprIndexCond =
				PexprIndexLookupKeyOnLeft(mp, md_accessor, pexprCommuted,
										  pmdindex, pdrgpcrIndex, outer_refs);
			pexprCommuted->Release();

			return pexprIndexCond;
		}
	}

	return NULL;
}

// Check if given expression is a valid index lookup predicate, and
// return modified (as needed) expression to be used for index lookup,
// a scalar expression is a valid index lookup predicate if it is in one
// the two forms:
//	[index-key CMP expr]
//	[expr CMP index-key]
// where expr is a scalar expression that is free of index keys and
// may have outer references (in the case of index nested loops)
CExpression *
CPredicateUtils::PexprIndexLookup(CMemoryPool *mp, CMDAccessor *md_accessor,
								  CExpression *pexprScalar,
								  const IMDIndex *pmdindex,
								  CColRefArray *pdrgpcrIndex,
								  CColRefSet *outer_refs,
								  BOOL allowArrayCmpForBTreeIndexes)
{
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(NULL != pdrgpcrIndex);

	IMDType::ECmpType cmptype = IMDType::EcmptOther;

	if (CUtils::FScalarCmp(pexprScalar))
	{
		cmptype = CScalarCmp::PopConvert(pexprScalar->Pop())->ParseCmpType();
	}
	else if (CUtils::FScalarArrayCmp(pexprScalar) &&
			 (IMDIndex::EmdindBitmap == pmdindex->IndexType() ||
			  (allowArrayCmpForBTreeIndexes &&
			   IMDIndex::EmdindBtree == pmdindex->IndexType())))
	{
		// array cmps are always allowed on bitmap indexes and when requested on btree indexes
		cmptype = CUtils::ParseCmpType(
			CScalarArrayCmp::PopConvert(pexprScalar->Pop())->MdIdOp());
	}

	BOOL gin_or_gist_index = (pmdindex->IndexType() == IMDIndex::EmdindGist ||
							  pmdindex->IndexType() == IMDIndex::EmdindGin);

	if (cmptype == IMDType::EcmptNEq || cmptype == IMDType::EcmptIDF ||
		(cmptype == IMDType::EcmptOther &&
		 !gin_or_gist_index) ||	 // only GiST indexes with a comparison type other are ok
		(gin_or_gist_index &&
		 pexprScalar->Arity() <
			 2))  // we do not support index expressions for GiST indexes
	{
		return NULL;
	}

	CExpression *pexprIndexLookupKeyOnLeft = PexprIndexLookupKeyOnLeft(
		mp, md_accessor, pexprScalar, pmdindex, pdrgpcrIndex, outer_refs);
	if (NULL != pexprIndexLookupKeyOnLeft)
	{
		return pexprIndexLookupKeyOnLeft;
	}

	CExpression *pexprIndexLookupKeyOnRight = PexprIndexLookupKeyOnRight(
		mp, md_accessor, pexprScalar, pmdindex, pdrgpcrIndex, outer_refs);
	if (NULL != pexprIndexLookupKeyOnRight)
	{
		return pexprIndexLookupKeyOnRight;
	}

	return NULL;
}

// split predicates into those that refer to an index key, and those that don't
void
CPredicateUtils::ExtractIndexPredicates(
	CMemoryPool *mp, CMDAccessor *md_accessor,
	CExpressionArray *pdrgpexprPredicate, const IMDIndex *pmdindex,
	CColRefArray *pdrgpcrIndex, CExpressionArray *pdrgpexprIndex,
	CExpressionArray *pdrgpexprResidual,
	CColRefSet *
		pcrsAcceptedOuterRefs,	// outer refs that are acceptable in an index predicate
	BOOL allowArrayCmpForBTreeIndexes)
{
	const ULONG length = pdrgpexprPredicate->Size();

	CColRefSet *pcrsIndex = GPOS_NEW(mp) CColRefSet(mp, pdrgpcrIndex);

	for (ULONG ul = 0; ul < length; ul++)
	{
		CExpression *pexprCond = (*pdrgpexprPredicate)[ul];

		pexprCond->AddRef();

		CColRefSet *pcrsUsed =
			GPOS_NEW(mp) CColRefSet(mp, *pexprCond->DeriveUsedColumns());
		if (NULL != pcrsAcceptedOuterRefs)
		{
			// filter out all accepted outer references
			pcrsUsed->Difference(pcrsAcceptedOuterRefs);
		}

		BOOL fSubset =
			(0 < pcrsUsed->Size()) && (pcrsIndex->ContainsAll(pcrsUsed));
		pcrsUsed->Release();

		if (!fSubset)
		{
			pdrgpexprResidual->Append(pexprCond);
			continue;
		}

		CExpressionArray *pdrgpexprTarget = pdrgpexprIndex;

		if (CUtils::FScalarIdentBoolType(pexprCond))
		{
			// expression is a column identifier of boolean type: convert to "col = true"
			pexprCond = CUtils::PexprScalarEqCmp(
				mp, pexprCond,
				CUtils::PexprScalarConstBool(mp, true /*value*/,
											 false /*is_null*/));
		}
		else if (FNot(pexprCond) &&
				 CUtils::FScalarIdentBoolType((*pexprCond)[0]))
		{
			// expression is of the form "not(col) for a column identifier of boolean type: convert to "col = false"
			CExpression *pexprScId = (*pexprCond)[0];
			pexprCond->Release();
			pexprScId->AddRef();
			pexprCond = CUtils::PexprScalarEqCmp(
				mp, pexprScId,
				CUtils::PexprScalarConstBool(mp, false /*value*/,
											 false /*is_null*/));
		}
		else
		{
			// attempt building index lookup predicate
			CExpression *pexprLookupPred = PexprIndexLookup(
				mp, md_accessor, pexprCond, pmdindex, pdrgpcrIndex,
				pcrsAcceptedOuterRefs, allowArrayCmpForBTreeIndexes);
			if (NULL != pexprLookupPred)
			{
				pexprCond->Release();
				pexprCond = pexprLookupPred;
			}
			else
			{
				// not a supported predicate
				pdrgpexprTarget = pdrgpexprResidual;
			}
		}

		pdrgpexprTarget->Append(pexprCond);
	}

	pcrsIndex->Release();
}

// split given scalar expression into two conjunctions; without outer
// references and with outer references
void
CPredicateUtils::SeparateOuterRefs(CMemoryPool *mp, CExpression *pexprScalar,
								   CColRefSet *outer_refs,
								   CExpression **ppexprLocal,
								   CExpression **ppexprOuterRef)
{
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(NULL != outer_refs);
	GPOS_ASSERT(NULL != ppexprLocal);
	GPOS_ASSERT(NULL != ppexprOuterRef);

	CColRefSet *pcrsUsed = pexprScalar->DeriveUsedColumns();
	if (pcrsUsed->IsDisjoint(outer_refs))
	{
		// if used columns are disjoint from outer references, return input expression
		pexprScalar->AddRef();
		*ppexprLocal = pexprScalar;
		*ppexprOuterRef = CUtils::PexprScalarConstBool(mp, true /*fval*/);
		return;
	}

	if (COperator::EopScalarNAryJoinPredList == pexprScalar->Pop()->Eopid())
	{
		// for a ScalarNAryJoinPredList we have to preserve that operator and
		// separate the outer refs from each of its children
		CExpressionArray *localChildren = GPOS_NEW(mp) CExpressionArray(mp);
		CExpressionArray *outerRefChildren = GPOS_NEW(mp) CExpressionArray(mp);

		for (ULONG c = 0; c < pexprScalar->Arity(); c++)
		{
			CExpression *childLocalExpr = NULL;
			CExpression *childOuterRefExpr = NULL;

			SeparateOuterRefs(mp, (*pexprScalar)[c], outer_refs,
							  &childLocalExpr, &childOuterRefExpr);
			localChildren->Append(childLocalExpr);
			outerRefChildren->Append(childOuterRefExpr);
		}

		// reassemble the CScalarNAryJoinPredList with its new children without outer refs
		pexprScalar->Pop()->AddRef();
		*ppexprLocal =
			GPOS_NEW(mp) CExpression(mp, pexprScalar->Pop(), localChildren);

		// do the same with the outer refs
		pexprScalar->Pop()->AddRef();
		*ppexprOuterRef =
			GPOS_NEW(mp) CExpression(mp, pexprScalar->Pop(), outerRefChildren);

		return;
	}

	CExpressionArray *pdrgpexpr = PdrgpexprConjuncts(mp, pexprScalar);
	CExpressionArray *pdrgpexprLocal = GPOS_NEW(mp) CExpressionArray(mp);
	CExpressionArray *pdrgpexprOuterRefs = GPOS_NEW(mp) CExpressionArray(mp);

	const ULONG size = pdrgpexpr->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *pexprPred = (*pdrgpexpr)[ul];
		CColRefSet *pcrsPredUsed = pexprPred->DeriveUsedColumns();
		pexprPred->AddRef();
		if (0 == pcrsPredUsed->Size() || outer_refs->IsDisjoint(pcrsPredUsed))
		{
			pdrgpexprLocal->Append(pexprPred);
		}
		else
		{
			pdrgpexprOuterRefs->Append(pexprPred);
		}
	}
	pdrgpexpr->Release();

	*ppexprLocal = PexprConjunction(mp, pdrgpexprLocal);
	*ppexprOuterRef = PexprConjunction(mp, pdrgpexprOuterRefs);
}

// convert predicates of the form (a Cmp b) into (a InvCmp b);
// where InvCmp is the inverse comparison (e.g., '=' --> '<>')
CExpression *
CPredicateUtils::PexprInverseComparison(CMemoryPool *mp, CExpression *pexprCmp)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	IMDId *mdid_op = CScalarCmp::PopConvert(pexprCmp->Pop())->MdIdOp();
	IMDId *pmdidInverseOp =
		md_accessor->RetrieveScOp(mdid_op)->GetInverseOpMdid();
	const CWStringConst *pstrFirst =
		md_accessor->RetrieveScOp(pmdidInverseOp)->Mdname().GetMDName();

	// generate a predicate for the inversion of the comparison involved in the subquery
	(*pexprCmp)[0]->AddRef();
	(*pexprCmp)[1]->AddRef();
	pmdidInverseOp->AddRef();

	return CUtils::PexprScalarCmp(mp, (*pexprCmp)[0], (*pexprCmp)[1],
								  *pstrFirst, pmdidInverseOp);
}

// convert predicates of the form (true = (a Cmp b)) into (a Cmp b);
// do this operation recursively on deep expression tree
CExpression *
CPredicateUtils::PexprPruneSuperfluosEquality(CMemoryPool *mp,
											  CExpression *pexpr)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(pexpr->Pop()->FScalar());

	if (CUtils::FSubquery(pexpr->Pop()))
	{
		// cannot recurse below subquery
		pexpr->AddRef();
		return pexpr;
	}

	if (IsEqualityOp(pexpr))
	{
		BOOL fConstTrueLeftChild = CUtils::FScalarConstTrue((*pexpr)[0]);
		BOOL fConstTrueRightChild = CUtils::FScalarConstTrue((*pexpr)[1]);
		BOOL fConstFalseLeftChild = CUtils::FScalarConstFalse((*pexpr)[0]);
		BOOL fConstFalseRightChild = CUtils::FScalarConstFalse((*pexpr)[1]);

		BOOL fCmpLeftChild = CUtils::FScalarCmp((*pexpr)[0]);
		BOOL fCmpRightChild = CUtils::FScalarCmp((*pexpr)[1]);

		if (fCmpRightChild)
		{
			if (fConstTrueLeftChild)
			{
				return PexprPruneSuperfluosEquality(mp, (*pexpr)[1]);
			}

			if (fConstFalseLeftChild)
			{
				CExpression *pexprInverse =
					PexprInverseComparison(mp, (*pexpr)[1]);
				CExpression *pexprPruned =
					PexprPruneSuperfluosEquality(mp, pexprInverse);
				pexprInverse->Release();
				return pexprPruned;
			}
		}

		if (fCmpLeftChild)
		{
			if (fConstTrueRightChild)
			{
				return PexprPruneSuperfluosEquality(mp, (*pexpr)[0]);
			}

			if (fConstFalseRightChild)
			{
				CExpression *pexprInverse =
					PexprInverseComparison(mp, (*pexpr)[0]);
				CExpression *pexprPruned =
					PexprPruneSuperfluosEquality(mp, pexprInverse);
				pexprInverse->Release();

				return pexprPruned;
			}
		}
	}

	// process children
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG ulChildren = pexpr->Arity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild =
			PexprPruneSuperfluosEquality(mp, (*pexpr)[ul]);
		pdrgpexpr->Append(pexprChild);
	}

	COperator *pop = pexpr->Pop();
	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);
}

// determine if we should test predicate implication for statistics computation
BOOL
CPredicateUtils::FCheckPredicateImplication(CExpression *pexprPred)
{
	// currently restrict testing implication to only equality of column references on scalar
	// ident and binary coercible casted idents
	if (COperator::EopScalarCmp == pexprPred->Pop()->Eopid() &&
		IMDType::EcmptEq ==
			CScalarCmp::PopConvert(pexprPred->Pop())->ParseCmpType())
	{
		CExpression *pexprLeft = (*pexprPred)[0];
		CExpression *pexprRight = (*pexprPred)[1];
		return ((COperator::EopScalarIdent == pexprLeft->Pop()->Eopid() ||
				 CCastUtils::FBinaryCoercibleCastedScId(pexprLeft)) &&
				(COperator::EopScalarIdent == pexprRight->Pop()->Eopid() ||
				 CCastUtils::FBinaryCoercibleCastedScId(pexprRight)));
	}
	return false;
}

// Given a predicate and a list of equivalence classes, return true if that predicate is
// implied by given equivalence classes
BOOL
CPredicateUtils::FImpliedPredicate(CExpression *pexprPred,
								   CColRefSetArray *pdrgpcrsEquivClasses)
{
	GPOS_ASSERT(pexprPred->Pop()->FScalar());
	GPOS_ASSERT(FCheckPredicateImplication(pexprPred));

	CColRefSet *pcrsUsed = pexprPred->DeriveUsedColumns();
	const ULONG size = pdrgpcrsEquivClasses->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CColRefSet *pcrs = (*pdrgpcrsEquivClasses)[ul];
		if (pcrs->ContainsAll(pcrsUsed))
		{
			// predicate is implied by given equivalence classes
			return true;
		}
	}

	return false;
}

// Remove conjuncts that are implied based on child equivalence classes,
// the main use case is minimizing join/selection predicates to avoid
// cardinality under-estimation,
//
// for example, in the expression ((a=b) AND (a=c)), if a child
// equivalence class is {b,c}, then we remove the conjunct (a=c)
// since it can be implied from {b,c}, {a,b}
CExpression *
CPredicateUtils::PexprRemoveImpliedConjuncts(CMemoryPool *mp,
											 CExpression *pexprScalar,
											 CExpressionHandle &exprhdl)
{
	if (COperator::EopScalarNAryJoinPredList == pexprScalar->Pop()->Eopid())
	{
		// for a ScalarNAryJoinPredList we have to preserve that operator and
		// remove implied preds from each child individually
		CExpressionArray *newChildren = GPOS_NEW(mp) CExpressionArray(mp);

		for (ULONG c = 0; c < pexprScalar->Arity(); c++)
		{
			newChildren->Append(
				PexprRemoveImpliedConjuncts(mp, (*pexprScalar)[c], exprhdl));
		}

		// reassemble the CScalarNAryJoinPredList with its new children without implied conjuncts
		pexprScalar->Pop()->AddRef();

		return GPOS_NEW(mp) CExpression(mp, pexprScalar->Pop(), newChildren);
	}

	// extract equivalence classes from logical children
	CColRefSetArray *pdrgpcrs =
		CUtils::PdrgpcrsCopyChildEquivClasses(mp, exprhdl);

	// extract all the conjuncts
	CExpressionArray *pdrgpexprConjuncts = PdrgpexprConjuncts(mp, pexprScalar);
	const ULONG size = pdrgpexprConjuncts->Size();
	CExpressionArray *pdrgpexprNewConjuncts = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *pexprConj = (*pdrgpexprConjuncts)[ul];
		if (FCheckPredicateImplication(pexprConj) &&
			FImpliedPredicate(pexprConj, pdrgpcrs))
		{
			// skip implied conjunct
			continue;
		}

		// add predicate to current equivalence classes
		CColRefSetArray *pdrgpcrsConj = NULL;
		CConstraint *pcnstr =
			CConstraint::PcnstrFromScalarExpr(mp, pexprConj, &pdrgpcrsConj);
		CRefCount::SafeRelease(pcnstr);
		if (NULL != pdrgpcrsConj)
		{
			CColRefSetArray *pdrgpcrsMerged =
				CUtils::PdrgpcrsMergeEquivClasses(mp, pdrgpcrs, pdrgpcrsConj);
			pdrgpcrs->Release();
			pdrgpcrsConj->Release();
			pdrgpcrs = pdrgpcrsMerged;
		}

		// add conjunct to new conjuncts array
		pexprConj->AddRef();
		pdrgpexprNewConjuncts->Append(pexprConj);
	}

	pdrgpexprConjuncts->Release();
	pdrgpcrs->Release();

	return PexprConjunction(mp, pdrgpexprNewConjuncts);
}

// check if given correlations are valid for (anti)semi-joins;
// we disallow correlations referring to inner child, since inner
// child columns are not visible above (anti)semi-join
BOOL
CPredicateUtils::FValidSemiJoinCorrelations(
	CMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprInner,
	CExpressionArray *pdrgpexprCorrelations)
{
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);

	CColRefSet *pcrsOuterOuput = pexprOuter->DeriveOutputColumns();
	CColRefSet *pcrsInnerOuput = pexprInner->DeriveOutputColumns();

	// collect output columns of both children
	CColRefSet *pcrsChildren = GPOS_NEW(mp) CColRefSet(mp, *pcrsOuterOuput);
	pcrsChildren->Union(pcrsInnerOuput);

	const ULONG ulCorrs = pdrgpexprCorrelations->Size();
	BOOL fValid = true;
	for (ULONG ul = 0; fValid && ul < ulCorrs; ul++)
	{
		CExpression *pexprPred = (*pdrgpexprCorrelations)[ul];
		CColRefSet *pcrsUsed = pexprPred->DeriveUsedColumns();
		if (0 < pcrsUsed->Size() && !pcrsChildren->ContainsAll(pcrsUsed) &&
			!pcrsUsed->IsDisjoint(pcrsInnerOuput))
		{
			// disallow correlations referring to inner child
			fValid = false;
		}
	}
	pcrsChildren->Release();

	return fValid;
}

// check if given expression is (a conjunction of) simple column
// equality that use columns from the given column set
BOOL
CPredicateUtils::FSimpleEqualityUsingCols(CMemoryPool *mp,
										  CExpression *pexprScalar,
										  CColRefSet *pcrs)
{
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(pexprScalar->Pop()->FScalar());
	GPOS_ASSERT(NULL != pcrs);
	GPOS_ASSERT(0 < pcrs->Size());

	// break expression into conjuncts
	CExpressionArray *pdrgpexpr = PdrgpexprConjuncts(mp, pexprScalar);
	const ULONG size = pdrgpexpr->Size();
	BOOL fSuccess = true;
	for (ULONG ul = 0; fSuccess && ul < size; ul++)
	{
		// join predicate must be an equality of scalar idents and uses columns from given set
		CExpression *pexprConj = (*pdrgpexpr)[ul];
		CColRefSet *pcrsUsed = pexprConj->DeriveUsedColumns();
		fSuccess = IsEqualityOp(pexprConj) &&
				   CUtils::FScalarIdent((*pexprConj)[0]) &&
				   CUtils::FScalarIdent((*pexprConj)[1]) &&
				   !pcrs->IsDisjoint(pcrsUsed);
	}
	pdrgpexpr->Release();

	return fSuccess;
}

// for all columns in the given expression and are members of the given column set, replace columns with NULLs
CExpression *
CPredicateUtils::PexprReplaceColsWithNulls(CMemoryPool *mp,
										   CExpression *pexprScalar,
										   CColRefSet *pcrs)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexprScalar);

	COperator *pop = pexprScalar->Pop();
	GPOS_ASSERT(pop->FScalar());

	if (CUtils::FSubquery(pop))
	{
		// do not recurse into subqueries
		pexprScalar->AddRef();
		return pexprScalar;
	}

	if (COperator::EopScalarIdent == pop->Eopid() &&
		pcrs->FMember(CScalarIdent::PopConvert(pop)->Pcr()))
	{
		// replace column with NULL constant
		return CUtils::PexprScalarConstBool(mp, false /*value*/,
											true /*is_null*/);
	}

	// process children recursively
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG ulChildren = pexprScalar->Arity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild =
			PexprReplaceColsWithNulls(mp, (*pexprScalar)[ul], pcrs);
		pdrgpexpr->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);
}

// check if scalar expression evaluates to (NOT TRUE) when
// all columns in the given set that are included in the expression
// are set to NULL
BOOL
CPredicateUtils::FNullRejecting(CMemoryPool *mp, CExpression *pexprScalar,
								CColRefSet *pcrs)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(pexprScalar->Pop()->FScalar());

	BOOL fHasVolatileFunctions =
		(IMDFunction::EfsVolatile ==
		 pexprScalar->DeriveScalarFunctionProperties()->Efs());
	BOOL fHasSQL = (IMDFunction::EfdaNoSQL !=
					pexprScalar->DeriveScalarFunctionProperties()->Efda());

	if (fHasVolatileFunctions || fHasSQL ||
		pexprScalar->DeriveHasNonScalarFunction())
	{
		// scalar expression must not have volatile functions, functions with SQL, subquery or non-scalar functions
		return false;
	}

	// create another expression copy where we replace columns included in the set with NULL values
	CExpression *pexprColsReplacedWithNulls =
		PexprReplaceColsWithNulls(mp, pexprScalar, pcrs);

	// evaluate the resulting expression
	CScalar::EBoolEvalResult eber =
		CScalar::EberEvaluate(mp, pexprColsReplacedWithNulls);
	pexprColsReplacedWithNulls->Release();

	// return TRUE if expression evaluation  result is (NOT TRUE), which means we need to
	// check if result is NULL or result is False,
	// in these two cases, a predicate will filter out incoming tuple, which means it is Null-Rejecting
	return (CScalar::EberNull == eber || CScalar::EberFalse == eber ||
			CScalar::EberNotTrue == eber);
}

// returns true iff the given expression is a Not operator whose child is an identifier
BOOL
CPredicateUtils::FNotIdent(CExpression *pexpr)
{
	return FNot(pexpr) &&
		   COperator::EopScalarIdent == (*pexpr)[0]->Pop()->Eopid();
}

// returns true iff all predicates in the given array are compatible with the given index.
BOOL
CPredicateUtils::FCompatiblePredicates(CExpressionArray *pdrgpexprPred,
									   const IMDIndex *pmdindex,
									   CColRefArray *pdrgpcrIndex,
									   CMDAccessor *md_accessor)
{
	GPOS_ASSERT(NULL != pdrgpexprPred);
	GPOS_ASSERT(NULL != pmdindex);

	const ULONG ulNumPreds = pdrgpexprPred->Size();
	for (ULONG ul = 0; ul < ulNumPreds; ul++)
	{
		if (!FCompatibleIndexPredicate((*pdrgpexprPred)[ul], pmdindex,
									   pdrgpcrIndex, md_accessor))
		{
			return false;
		}
	}

	return true;
}

// returns true iff the given predicate 'pexprPred' is compatible with the given index 'pmdindex'.
BOOL
CPredicateUtils::FCompatibleIndexPredicate(CExpression *pexprPred,
										   const IMDIndex *pmdindex,
										   CColRefArray *pdrgpcrIndex,
										   CMDAccessor *md_accessor)
{
	GPOS_ASSERT(NULL != pexprPred);
	GPOS_ASSERT(NULL != pmdindex);

	const IMDScalarOp *pmdobjScCmp = NULL;
	if (COperator::EopScalarCmp == pexprPred->Pop()->Eopid())
	{
		CScalarCmp *popScCmp = CScalarCmp::PopConvert(pexprPred->Pop());
		pmdobjScCmp = md_accessor->RetrieveScOp(popScCmp->MdIdOp());
	}
	else if (COperator::EopScalarArrayCmp == pexprPred->Pop()->Eopid())
	{
		CScalarArrayCmp *popScArrCmp =
			CScalarArrayCmp::PopConvert(pexprPred->Pop());
		pmdobjScCmp = md_accessor->RetrieveScOp(popScArrCmp->MdIdOp());
	}
	else
	{
		return false;
	}

	CExpression *pexprLeft = (*pexprPred)[0];
	CColRefSet *pcrsUsed = pexprLeft->DeriveUsedColumns();
	GPOS_ASSERT(1 == pcrsUsed->Size());

	CColRef *pcrIndexKey = pcrsUsed->PcrFirst();
	ULONG ulKeyPos = pdrgpcrIndex->IndexOf(pcrIndexKey);
	GPOS_ASSERT(gpos::ulong_max != ulKeyPos);

	return (pmdindex->IsCompatible(pmdobjScCmp, ulKeyPos));
}

// check if given array of expressions contain a volatile function like random().
BOOL
CPredicateUtils::FContainsVolatileFunction(CExpressionArray *pdrgpexprPred)
{
	GPOS_ASSERT(NULL != pdrgpexprPred);

	const ULONG ulNumPreds = pdrgpexprPred->Size();
	for (ULONG ul = 0; ul < ulNumPreds; ul++)
	{
		CExpression *pexpr = (CExpression *) (*pdrgpexprPred)[ul];

		if (FContainsVolatileFunction(pexpr))
		{
			return true;
		}
	}

	return false;
}

// check if the expression contains a volatile function like random().
BOOL
CPredicateUtils::FContainsVolatileFunction(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);


	COperator *popCurrent = pexpr->Pop();
	GPOS_ASSERT(NULL != popCurrent);

	if (COperator::EopScalarFunc == popCurrent->Eopid())
	{
		CScalarFunc *pCurrentFunction = CScalarFunc::PopConvert(popCurrent);
		return IMDFunction::EfsVolatile ==
			   pCurrentFunction->EfsGetFunctionStability();
	}

	// recursively check children
	const ULONG ulChildren = pexpr->Arity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		BOOL isVolatile = FContainsVolatileFunction((*pexpr)[ul]);
		if (isVolatile)
		{
			return true;
		}
	}

	// cannot find any
	return false;
}

// check if the expensive CNF conversion is beneficial in finding predicate for hash join
BOOL
CPredicateUtils::FConvertToCNF(CExpression *pexprOuter, CExpression *pexprInner,
							   CExpression *pexprScalar)
{
	GPOS_ASSERT(NULL != pexprScalar);

	if (FComparison(pexprScalar))
	{
		return CPhysicalJoin::FHashJoinCompatible(pexprScalar, pexprOuter,
												  pexprInner);
	}

	BOOL fOr = FOr(pexprScalar);
	BOOL fAllChidrenDoCNF = true;
	BOOL fExistsChildDoCNF = false;

	// recursively check children
	const ULONG arity = pexprScalar->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		BOOL fCNFConversion =
			FConvertToCNF(pexprOuter, pexprInner, (*pexprScalar)[ul]);

		if (fCNFConversion)
		{
			fExistsChildDoCNF = true;
		}
		else
		{
			fAllChidrenDoCNF = false;
		}
	}

	if (fOr)
	{
		// an OR predicate can only be beneficial is all of it
		// children state that there is a benefit for CNF conversion
		// eg: ((a1 > b1) AND (a2 > b2)) OR ((a3 == b3) AND (a4 == b4))
		// one of the OR children has no equality condition and thus when
		// we convert the expression into CNF none of then will be useful to
		// for hash join

		return fAllChidrenDoCNF;
	}
	else
	{
		// at least one one child states that CNF conversion is beneficial

		return fExistsChildDoCNF;
	}
}

// if the nth child of the given union/union all expression is also a
// union / union all expression, then collect the latter's children and
// set the input columns of the new n-ary union/unionall operator
void
CPredicateUtils::CollectGrandChildrenUnionUnionAll(
	CMemoryPool *mp, CExpression *pexpr, ULONG child_index,
	CExpressionArray *pdrgpexprResult, CColRef2dArray *pdrgdrgpcrResult)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(child_index < pexpr->Arity());
	GPOS_ASSERT(NULL != pdrgpexprResult);
	GPOS_ASSERT(NULL != pdrgdrgpcrResult);
	GPOS_ASSERT(
		CPredicateUtils::FCollapsibleChildUnionUnionAll(pexpr, child_index));


	CExpression *pexprChild = (*pexpr)[child_index];
	GPOS_ASSERT(NULL != pexprChild);

	CLogicalSetOp *pop = CLogicalSetOp::PopConvert(pexpr->Pop());
	CLogicalSetOp *popChild = CLogicalSetOp::PopConvert(pexprChild->Pop());

	// the parent setop's expected input columns and the child setop's output columns
	// may have different size or order or both. We need to ensure that the new
	// n-ary setop has the right order of the input columns from its grand children
	CColRef2dArray *pdrgpdrgpcrInput = pop->PdrgpdrgpcrInput();
	CColRefArray *pdrgpcrInputExpected = (*pdrgpdrgpcrInput)[child_index];

	const ULONG num_cols = pdrgpcrInputExpected->Size();

	CColRefArray *pdrgpcrOuputChild = popChild->PdrgpcrOutput();
	GPOS_ASSERT(num_cols <= pdrgpcrOuputChild->Size());

	ULongPtrArray *pdrgpul = GPOS_NEW(mp) ULongPtrArray(mp);
	for (ULONG ulColIdx = 0; ulColIdx < num_cols; ulColIdx++)
	{
		const CColRef *colref = (*pdrgpcrInputExpected)[ulColIdx];
		ULONG ulPos = pdrgpcrOuputChild->IndexOf(colref);
		GPOS_ASSERT(gpos::ulong_max != ulPos);
		pdrgpul->Append(GPOS_NEW(mp) ULONG(ulPos));
	}

	CColRef2dArray *pdrgdrgpcrChild = popChild->PdrgpdrgpcrInput();
	const ULONG ulArityChild = pexprChild->Arity();
	GPOS_ASSERT(pdrgdrgpcrChild->Size() == ulArityChild);

	for (ULONG ul = 0; ul < ulArityChild; ul++)
	{
		// collect the grand child expression
		CExpression *pexprGrandchild = (*pexprChild)[ul];
		GPOS_ASSERT(pexprGrandchild->Pop()->FLogical());

		pexprGrandchild->AddRef();
		pdrgpexprResult->Append(pexprGrandchild);

		// collect the correct input columns
		CColRefArray *pdrgpcrOld = (*pdrgdrgpcrChild)[ul];
		CColRefArray *pdrgpcrNew = GPOS_NEW(mp) CColRefArray(mp);
		for (ULONG ulColIdx = 0; ulColIdx < num_cols; ulColIdx++)
		{
			ULONG ulPos = *(*pdrgpul)[ulColIdx];
			CColRef *colref = (*pdrgpcrOld)[ulPos];
			pdrgpcrNew->Append(colref);
		}

		pdrgdrgpcrResult->Append(pdrgpcrNew);
	}

	pdrgpul->Release();
}

// check if we can collapse the nth child of the given union / union all operator
BOOL
CPredicateUtils::FCollapsibleChildUnionUnionAll(CExpression *pexpr,
												ULONG child_index)
{
	GPOS_ASSERT(NULL != pexpr);

	if (!CPredicateUtils::FUnionOrUnionAll(pexpr))
	{
		return false;
	}

	CExpression *pexprChild = (*pexpr)[child_index];
	GPOS_ASSERT(NULL != pexprChild);

	// we can only collapse when the parent and child operator are of the same kind
	return (pexprChild->Pop()->Eopid() == pexpr->Pop()->Eopid());
}

// check if the input predicate is a simple predicate that can be used for bitmap
// index lookup directly without separating it's children, such as
// * A simple scalar (array) cmp between an ident and a constant (array)
// * A conjunct with children of the form:
//		+--CScalarCmp (op)
//		|--CScalarIdent
//		+--CScalarConst
// OR
//		+--CScalarArrayCmp (op)
//		|--CScalarIdent
//		+--CScalarConstArray
// OR
//		+--CScalarBoolOp (EboolopNot)
//			+--CScalarIdent (boolean type)
// OR
//		+--CScalarIdent   (boolean type)

// Note: Casted idents, constants or const arrays are allowed, such as
//		+--CScalarArrayCmp (op)
//		|--CScalarCast
//		|  +--CScalarIdent
//		+--CScalarConstArray
BOOL
CPredicateUtils::FBitmapLookupSupportedPredicateOrConjunct(
	CExpression *pexpr, CColRefSet *outer_refs)
{
	if (CPredicateUtils::FAnd(pexpr))
	{
		const ULONG ulArity = pexpr->Arity();
		BOOL result = true;
		for (ULONG ul = 0; ul < ulArity && result; ul++)
		{
			result = result &&
					 CPredicateUtils::FBitmapLookupSupportedPredicateOrConjunct(
						 (*pexpr)[ul], outer_refs);
		}

		return result;
	}

	// indexes allow ident cmp const and ident cmp outerref
	if (CPredicateUtils::FIdentCompareConstIgnoreCast(
			pexpr, COperator::EopScalarCmp) ||
		CPredicateUtils::FIdentCompareOuterRefExprIgnoreCast(pexpr,
															 outer_refs) ||
		CPredicateUtils::FArrayCompareIdentToConstIgnoreCast(pexpr) ||
		CUtils::FScalarIdentBoolType(pexpr) ||
		(!CUtils::FScalarIdentBoolType(pexpr) &&
		 CPredicateUtils::FNotIdent(pexpr)))
	{
		return true;
	}

	return false;
}

BOOL CPredicateUtils::FBuiltInComparisonIsVeryStrict(IMDId *mdid)
{
	const CMDIdGPDB *const pmdidGPDB = CMDIdGPDB::CastMdid(mdid);
	GPOS_ASSERT(NULL != pmdidGPDB);
	return true;
}

BOOL
CPredicateUtils::ExprContainsOnlyStrictComparisons(CMemoryPool *mp, CExpression *expr)
{
	CExpressionArray *conjuncts = PdrgpexprConjuncts(mp, expr);
	BOOL result = ExprsContainsOnlyStrictComparisons(conjuncts);
	conjuncts->Release();
	return result;
}

BOOL
CPredicateUtils::ExprsContainsOnlyStrictComparisons(CExpressionArray *conjuncts)
{
	CMDAccessor *mda = COptCtxt::PoctxtFromTLS()->Pmda();

	BOOL result = true;
	for (ULONG ul = 0; ul < conjuncts->Size(); ++ul)
	{
		CExpression *conjunct = (*conjuncts)[ul];
		if (FComparison(conjunct))
		{
			CScalarCmp *pscalarCmp = CScalarCmp::PopConvert(conjunct->Pop());
			if (!CMDAccessorUtils::FScalarOpReturnsNullOnNullInput(
					mda, pscalarCmp->MdIdOp()))
			{
				result = false;
				break;
			}
		}
		else
		{
			result = false;
			break;
		}
	}

	return result;
}