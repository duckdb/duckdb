//---------------------------------------------------------------------------
//	@filename:
//		CCastUtils.cpp
//
//	@doc:
//		Implementation of cast utility functions
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CCastUtils.h"

#include "duckdb/optimizer/cascade/memory/CAutoMemoryPool.h"

#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessorUtils.h"
#include "duckdb/optimizer/cascade/operators/CPredicateUtils.h"
#include "duckdb/optimizer/cascade/operators/CScalarArrayCoerceExpr.h"
#include "duckdb/optimizer/cascade/operators/CScalarCast.h"
#include "duckdb/optimizer/cascade/operators/CScalarIdent.h"
#include "duckdb/optimizer/cascade/md/IMDCast.h"

using namespace gpopt;
using namespace gpmd;

// is the given expression a binary coercible cast of a scalar identifier
BOOL
CCastUtils::FBinaryCoercibleCastedScId(CExpression *pexpr, CColRef *colref)
{
	GPOS_ASSERT(NULL != pexpr);

	if (!FBinaryCoercibleCast(pexpr))
	{
		return false;
	}

	CExpression *pexprChild = (*pexpr)[0];

	// cast(col1)
	return COperator::EopScalarIdent == pexprChild->Pop()->Eopid() &&
		   colref == CScalarIdent::PopConvert(pexprChild->Pop())->Pcr();
}

// is the given expression a binary coercible cast of a scalar identifier
BOOL
CCastUtils::FBinaryCoercibleCastedScId(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	if (!FBinaryCoercibleCast(pexpr))
	{
		return false;
	}

	CExpression *pexprChild = (*pexpr)[0];

	// cast(col1)
	return COperator::EopScalarIdent == pexprChild->Pop()->Eopid();
}

// is the given expression a binary coercible cast of a const
BOOL
CCastUtils::FBinaryCoercibleCastedConst(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	if (!FBinaryCoercibleCast(pexpr))
	{
		return false;
	}

	CExpression *pexprChild = (*pexpr)[0];

	// cast(const)
	return COperator::EopScalarConst == pexprChild->Pop()->Eopid();
}

// extract the column reference if the given expression a scalar identifier
// or a cast of a scalar identifier or a function that casts a scalar identifier.
// Else return NULL.
const CColRef *
CCastUtils::PcrExtractFromScIdOrCastScId(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	BOOL fScIdent = COperator::EopScalarIdent == pexpr->Pop()->Eopid();
	BOOL fCastedScIdent = CScalarIdent::FCastedScId(pexpr);

	// col or cast(col)
	if (!fScIdent && !fCastedScIdent)
	{
		return NULL;
	}

	CScalarIdent *popScIdent = NULL;
	if (fScIdent)
	{
		popScIdent = CScalarIdent::PopConvert(pexpr->Pop());
	}
	else
	{
		GPOS_ASSERT(fCastedScIdent);
		popScIdent = CScalarIdent::PopConvert((*pexpr)[0]->Pop());
	}

	return popScIdent->Pcr();
}

// cast the input column reference to the destination mdid
CExpression *
CCastUtils::PexprCast(CMemoryPool *mp, CMDAccessor *md_accessor,
					  const CColRef *colref, IMDId *mdid_dest)
{
	GPOS_ASSERT(NULL != mdid_dest);

	IMDId *mdid_src = colref->RetrieveType()->MDId();
	GPOS_ASSERT(
		CMDAccessorUtils::FCastExists(md_accessor, mdid_src, mdid_dest));

	const IMDCast *pmdcast = md_accessor->Pmdcast(mdid_src, mdid_dest);

	mdid_dest->AddRef();
	pmdcast->GetCastFuncMdId()->AddRef();
	CExpression *pexpr;
	CScalarCast *popCast = GPOS_NEW(mp) CScalarCast(mp, mdid_dest, pmdcast->GetCastFuncMdId(), pmdcast->IsBinaryCoercible());
	pexpr = GPOS_NEW(mp) CExpression(mp, popCast, CUtils::PexprScalarIdent(mp, colref));
	return pexpr;
}

// check whether the given expression is a binary coercible cast of something
BOOL
CCastUtils::FBinaryCoercibleCast(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	COperator *pop = pexpr->Pop();

	return FScalarCast(pexpr) &&
		   CScalarCast::PopConvert(pop)->IsBinaryCoercible();
}

BOOL
CCastUtils::FScalarCast(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	COperator *pop = pexpr->Pop();

	return COperator::EopScalarCast == pop->Eopid();
}

// return the given expression without any binary coercible casts
// that exist on the top
CExpression *
CCastUtils::PexprWithoutBinaryCoercibleCasts(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(pexpr->Pop()->FScalar());

	CExpression *pexprOutput = pexpr;

	while (FBinaryCoercibleCast(pexprOutput))
	{
		GPOS_ASSERT(1 == pexprOutput->Arity());
		pexprOutput = (*pexprOutput)[0];
	}

	return pexprOutput;
}

// add explicit casting to equality operations between compatible types
CExpressionArray* CCastUtils::PdrgpexprCastEquality(CMemoryPool *mp, CExpression *pexpr)
{
	GPOS_ASSERT(pexpr->Pop()->FScalar());

	CExpressionArray *pdrgpexpr = CPredicateUtils::PdrgpexprConjuncts(mp, pexpr);
	CExpressionArray *pdrgpexprNew = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG ulPreds = pdrgpexpr->Size();
	for (ULONG ul = 0; ul < ulPreds; ul++)
	{
		CExpression *pexprPred = (*pdrgpexpr)[ul];
		pexprPred->AddRef();
		CExpression *pexprNewPred = pexprPred;

		if (CPredicateUtils::IsEqualityOp(pexprPred) || CPredicateUtils::FINDF(pexprPred))
		{
			CExpression *pexprCasted = PexprAddCast(mp, pexprPred);
			if (NULL != pexprCasted)
			{
				// release predicate since we will construct a new one
				pexprNewPred->Release();
				pexprNewPred = pexprCasted;
			}
		}
		pdrgpexprNew->Append(pexprNewPred);
	}

	pdrgpexpr->Release();

	return pdrgpexprNew;
}

// add explicit casting to left child of given equality or INDF predicate
// and return resulting casted expression;
// the function returns NULL if operation failed
CExpression* CCastUtils::PexprAddCast(CMemoryPool *mp, CExpression *pexprPred)
{
	GPOS_ASSERT(NULL != pexprPred);
	GPOS_ASSERT(CUtils::FScalarCmp(pexprPred) || CPredicateUtils::FINDF(pexprPred));

	CExpression *pexprChild = pexprPred;

	if (!CUtils::FScalarCmp(pexprPred))
	{
		pexprChild = (*pexprPred)[0];
	}

	CExpression *pexprLeft = (*pexprChild)[0];
	CExpression *pexprRight = (*pexprChild)[1];

	IMDId *mdid_type_left = CScalar::PopConvert(pexprLeft->Pop())->MdidType();
	IMDId *mdid_type_right = CScalar::PopConvert(pexprRight->Pop())->MdidType();

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	CExpression *pexprNewPred = NULL;

	BOOL fTypesEqual = mdid_type_left->Equals(mdid_type_right);
	BOOL fCastLtoR = CMDAccessorUtils::FCastExists(md_accessor, mdid_type_left, mdid_type_right);
	BOOL fCastRtoL = CMDAccessorUtils::FCastExists(md_accessor, mdid_type_right, mdid_type_left);

	if (fTypesEqual || !(fCastLtoR || fCastRtoL))
	{
		return pexprNewPred;
	}

	pexprLeft->AddRef();
	pexprRight->AddRef();

	CExpression *pexprNewLeft = pexprLeft;
	CExpression *pexprNewRight = pexprRight;

	if (fCastLtoR)
	{
		pexprNewLeft = PexprCast(mp, md_accessor, pexprLeft, mdid_type_right);
	}
	else
	{
		GPOS_ASSERT(fCastRtoL);
		pexprNewRight = PexprCast(mp, md_accessor, pexprRight, mdid_type_left);
	}

	GPOS_ASSERT(NULL != pexprNewLeft && NULL != pexprNewRight);

	if (CUtils::FScalarCmp(pexprPred))
	{
		pexprNewPred = CUtils::PexprScalarCmp(mp, pexprNewLeft, pexprNewRight, IMDType::EcmptEq);
	}
	else
	{
		pexprNewPred = CUtils::PexprINDF(mp, pexprNewLeft, pexprNewRight);
	}

	return pexprNewPred;
}

// add explicit casting on the input expression to the destination type
CExpression* CCastUtils::PexprCast(CMemoryPool *mp, CMDAccessor *md_accessor, CExpression *pexpr, IMDId *mdid_dest)
{
	IMDId *mdid_src = CScalar::PopConvert(pexpr->Pop())->MdidType();
	const IMDCast *pmdcast = md_accessor->Pmdcast(mdid_src, mdid_dest);

	mdid_dest->AddRef();
	pmdcast->GetCastFuncMdId()->AddRef();
	CExpression *pexprCast;

	CScalarCast *popCast = GPOS_NEW(mp) CScalarCast(mp, mdid_dest, pmdcast->GetCastFuncMdId(), pmdcast->IsBinaryCoercible());
	pexprCast = GPOS_NEW(mp) CExpression(mp, popCast, pexpr);
	return pexprCast;
}