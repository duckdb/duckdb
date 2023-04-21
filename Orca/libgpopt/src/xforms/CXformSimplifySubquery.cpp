//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSimplifySubquery.cpp
//
//	@doc:
//		Simplify existential/quantified subqueries by transforming
//		into count(*) subqueries
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformSimplifySubquery.h"

#include "gpos/base.h"

#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDScalarOp.h"

using namespace gpmd;
using namespace gpopt;


// initialization of simplify function mappings
const CXformSimplifySubquery::SSimplifySubqueryMapping
	CXformSimplifySubquery::m_rgssm[] = {
		{FSimplifyExistential, CUtils::FExistentialSubquery},
		{FSimplifyQuantified, CUtils::FQuantifiedSubquery},

		// the last entry is used to replace existential subqueries with count(*)
		// after quantified subqueries have been replaced in the input expression
		{FSimplifyExistential, CUtils::FExistentialSubquery},
};

//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifySubquery::CXformSimplifySubquery
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSimplifySubquery::CXformSimplifySubquery(CExpression *pexprPattern)
	: CXformExploration(pexprPattern)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifySubquery::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		subqueries must exist in scalar tree
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSimplifySubquery::Exfp(CExpressionHandle &exprhdl) const
{
	// consider this transformation only if subqueries exist
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
	;
}



//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifySubquery::FSimplifyQuantified
//
//	@doc:
//		Transform quantified subqueries to count(*) subqueries;
//		the function returns true if transformation succeeded
//
//---------------------------------------------------------------------------
BOOL
CXformSimplifySubquery::FSimplifyQuantified(CMemoryPool *mp,
											CExpression *pexprScalar,
											CExpression **ppexprNewScalar)
{
	GPOS_ASSERT(CUtils::FQuantifiedSubquery(pexprScalar->Pop()));

	CExpression *pexprNewSubquery = NULL;
	CExpression *pexprCmp = NULL;
	CXformUtils::QuantifiedToAgg(mp, pexprScalar, &pexprNewSubquery, &pexprCmp);

	// create a comparison predicate involving subquery expression
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	(*pexprCmp)[1]->AddRef();
	pdrgpexpr->Append(pexprNewSubquery);
	pdrgpexpr->Append((*pexprCmp)[1]);
	pexprCmp->Pop()->AddRef();

	*ppexprNewScalar = GPOS_NEW(mp) CExpression(mp, pexprCmp->Pop(), pdrgpexpr);
	pexprCmp->Release();

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifySubquery::FSimplifyExistential
//
//	@doc:
//		Transform existential subqueries to count(*) subqueries;
//		the function returns true if transformation succeeded
//
//---------------------------------------------------------------------------
BOOL
CXformSimplifySubquery::FSimplifyExistential(CMemoryPool *mp,
											 CExpression *pexprScalar,
											 CExpression **ppexprNewScalar)
{
	GPOS_ASSERT(CUtils::FExistentialSubquery(pexprScalar->Pop()));

	CExpression *pexprNewSubquery = NULL;
	CExpression *pexprCmp = NULL;
	CXformUtils::ExistentialToAgg(mp, pexprScalar, &pexprNewSubquery,
								  &pexprCmp);

	// create a comparison predicate involving subquery expression
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	(*pexprCmp)[1]->AddRef();
	pdrgpexpr->Append(pexprNewSubquery);
	pdrgpexpr->Append((*pexprCmp)[1]);
	pexprCmp->Pop()->AddRef();

	*ppexprNewScalar = GPOS_NEW(mp) CExpression(mp, pexprCmp->Pop(), pdrgpexpr);
	pexprCmp->Release();

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifySubquery::FSimplify
//
//	@doc:
//		Transform existential/quantified subqueries to count(*) subqueries;
//		the function returns true if transformation succeeded
//
//---------------------------------------------------------------------------
BOOL
CXformSimplifySubquery::FSimplify(CMemoryPool *mp, CExpression *pexprScalar,
								  CExpression **ppexprNewScalar,
								  FnSimplify *pfnsimplify, FnMatch *pfnmatch)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexprScalar);

	if (pfnmatch(pexprScalar->Pop()))
	{
		return pfnsimplify(mp, pexprScalar, ppexprNewScalar);
	}

	// for all other types of subqueries, or if no other subqueries are
	// below this point, we add-ref root node and return immediately
	if (CUtils::FSubquery(pexprScalar->Pop()) ||
		!pexprScalar->DeriveHasSubquery())
	{
		pexprScalar->AddRef();
		*ppexprNewScalar = pexprScalar;

		return true;
	}

	// otherwise, recursively process children
	const ULONG arity = pexprScalar->Arity();
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	BOOL fSuccess = true;
	for (ULONG ul = 0; fSuccess && ul < arity; ul++)
	{
		CExpression *pexprChild = NULL;
		fSuccess = FSimplify(mp, (*pexprScalar)[ul], &pexprChild, pfnsimplify,
							 pfnmatch);
		if (fSuccess)
		{
			pdrgpexprChildren->Append(pexprChild);
		}
		else
		{
			CRefCount::SafeRelease(pexprChild);
		}
	}

	if (fSuccess)
	{
		COperator *pop = pexprScalar->Pop();
		pop->AddRef();
		*ppexprNewScalar = GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
	}
	else
	{
		pdrgpexprChildren->Release();
	}

	return fSuccess;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifySubquery::Transform
//
//	@doc:
//		Actual transformation to simplify subquery expression
//
//---------------------------------------------------------------------------
void
CXformSimplifySubquery::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
								  CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();
	CExpression *pexprInput = pexpr;
	const ULONG size = GPOS_ARRAY_SIZE(m_rgssm);
	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *pexprOuter = (*pexprInput)[0];
		CExpression *pexprScalar = (*pexprInput)[1];
		CExpression *pexprNewScalar = NULL;

		if (!FSimplify(mp, pexprScalar, &pexprNewScalar,
					   m_rgssm[ul].m_pfnsimplify, m_rgssm[ul].m_pfnmatch))
		{
			CRefCount::SafeRelease(pexprNewScalar);
			continue;
		}

		pexprOuter->AddRef();
		CExpression *pexprResult = NULL;
		if (COperator::EopLogicalSelect == pexprInput->Pop()->Eopid())
		{
			pexprResult =
				CUtils::PexprLogicalSelect(mp, pexprOuter, pexprNewScalar);
		}
		else
		{
			GPOS_ASSERT(COperator::EopLogicalProject ==
						pexprInput->Pop()->Eopid());

			pexprResult = CUtils::PexprLogicalProject(
				mp, pexprOuter, pexprNewScalar, false /*fNewComputedCol*/);
		}

		// normalize resulting expression
		CExpression *pexprNormalized =
			CNormalizer::PexprNormalize(mp, pexprResult);
		pexprResult->Release();

		pxfres->Add(pexprNormalized);

		if (1 == ul)
		{
			pexprInput = pexprNormalized;
		}
	}
}


// EOF
