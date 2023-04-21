//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSimplifyGbAgg.cpp
//
//	@doc:
//		Implementation of simplifying an aggregate expression by finding
//		the minimal grouping columns based on functional dependencies
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformSimplifyGbAgg.h"

#include "gpos/base.h"

#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/ops.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyGbAgg::CXformSimplifyGbAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSimplifyGbAgg::CXformSimplifyGbAgg(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp))  // scalar project list
			  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyGbAgg::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		aggregate must have grouping columns
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSimplifyGbAgg::Exfp(CExpressionHandle &exprhdl) const
{
	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());

	GPOS_ASSERT(COperator::EgbaggtypeGlobal == popAgg->Egbaggtype());

	if (0 == popAgg->Pdrgpcr()->Size() || NULL != popAgg->PdrgpcrMinimal())
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyGbAgg::FDropGbAgg
//
//	@doc:
//		Return true if GbAgg operator can be dropped because grouping
//		columns include a key
//
//---------------------------------------------------------------------------
BOOL
CXformSimplifyGbAgg::FDropGbAgg(CMemoryPool *mp, CExpression *pexpr,
								CXformResult *pxfres)
{
	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprProjectList = (*pexpr)[1];

	if (0 < pexprProjectList->Arity())
	{
		// GbAgg cannot be dropped if Agg functions are computed
		return false;
	}

	CKeyCollection *pkc = pexprRelational->DeriveKeyCollection();
	if (NULL == pkc)
	{
		// relational child does not have key
		return false;
	}

	const ULONG ulKeys = pkc->Keys();
	BOOL fDrop = false;
	for (ULONG ul = 0; !fDrop && ul < ulKeys; ul++)
	{
		CColRefArray *pdrgpcrKey = pkc->PdrgpcrKey(mp, ul);
		CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, pdrgpcrKey);
		pdrgpcrKey->Release();

		CColRefSet *pcrsGrpCols = GPOS_NEW(mp) CColRefSet(mp);
		pcrsGrpCols->Include(popAgg->Pdrgpcr());
		BOOL fGrpColsHasKey = pcrsGrpCols->ContainsAll(pcrs);

		pcrs->Release();
		pcrsGrpCols->Release();
		if (fGrpColsHasKey)
		{
			// Gb operator can be dropped
			pexprRelational->AddRef();
			CExpression *pexprResult = CUtils::PexprLogicalSelect(
				mp, pexprRelational,
				CPredicateUtils::PexprConjunction(mp, NULL));
			pxfres->Add(pexprResult);
			fDrop = true;
		}
	}

	return fDrop;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyGbAgg::Transform
//
//	@doc:
//		Actual transformation to simplify a aggregate expression
//
//---------------------------------------------------------------------------
void
CXformSimplifyGbAgg::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
							   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	if (FDropGbAgg(mp, pexpr, pxfres))
	{
		// grouping columns could be dropped, GbAgg is transformed to a Select
		return;
	}

	// extract components
	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprProjectList = (*pexpr)[1];

	CColRefArray *colref_array = popAgg->Pdrgpcr();
	CColRefSet *pcrsGrpCols = GPOS_NEW(mp) CColRefSet(mp);
	pcrsGrpCols->Include(colref_array);

	CColRefSet *pcrsCovered =
		GPOS_NEW(mp) CColRefSet(mp);  // set of grouping columns covered by FD's
	CColRefSet *pcrsMinimal = GPOS_NEW(mp)
		CColRefSet(mp);	 // a set of minimal grouping columns based on FD's
	CFunctionalDependencyArray *pdrgpfd = pexpr->DeriveFunctionalDependencies();

	// collect grouping columns FD's
	const ULONG size = (pdrgpfd == NULL) ? 0 : pdrgpfd->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CFunctionalDependency *pfd = (*pdrgpfd)[ul];
		if (pfd->FIncluded(pcrsGrpCols))
		{
			pcrsCovered->Include(pfd->PcrsDetermined());
			pcrsCovered->Include(pfd->PcrsKey());
			pcrsMinimal->Include(pfd->PcrsKey());
		}
	}
	BOOL fCovered = pcrsCovered->Equals(pcrsGrpCols);
	pcrsGrpCols->Release();
	pcrsCovered->Release();

	if (!fCovered)
	{
		// the union of RHS of collected FD's does not cover all grouping columns
		pcrsMinimal->Release();
		return;
	}

	// create a new Agg with minimal grouping columns
	colref_array->AddRef();

	CLogicalGbAgg *popAggNew = GPOS_NEW(mp) CLogicalGbAgg(
		mp, colref_array, pcrsMinimal->Pdrgpcr(mp), popAgg->Egbaggtype());
	pcrsMinimal->Release();
	GPOS_ASSERT(!popAgg->Matches(popAggNew) &&
				"Simplified aggregate matches original aggregate");

	pexprRelational->AddRef();
	pexprProjectList->AddRef();
	CExpression *pexprResult = GPOS_NEW(mp)
		CExpression(mp, popAggNew, pexprRelational, pexprProjectList);
	pxfres->Add(pexprResult);
}


// EOF
