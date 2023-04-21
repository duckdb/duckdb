//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformCollapseGbAgg.cpp
//
//	@doc:
//		Implementation of collapsing two cascaded group-by operators
//		into a single group-by
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformCollapseGbAgg.h"

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/ops.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformCollapseGbAgg::CXformCollapseGbAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformCollapseGbAgg::CXformCollapseGbAgg(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp) CPatternTree(mp))  // scalar project list
				  ),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp))  // scalar project list
			  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformCollapseGbAgg::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		GbAgg must be global and have non empty grouping columns
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformCollapseGbAgg::Exfp(CExpressionHandle &exprhdl) const
{
	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());
	if (!popAgg->FGlobal() || 0 == popAgg->Pdrgpcr()->Size())
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformCollapseGbAgg::Transform
//
//	@doc:
//		Actual transformation to collapse two cascaded group by operators;
//		if the top Gb grouping columns are subset of bottom Gb grouping
//		columns AND both Gb operators do not define agg functions, we can
//		remove the bottom group by operator
//
//
//---------------------------------------------------------------------------
void
CXformCollapseGbAgg::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
							   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CLogicalGbAgg *popTopGbAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());
	GPOS_ASSERT(0 < popTopGbAgg->Pdrgpcr()->Size());
	GPOS_ASSERT(popTopGbAgg->FGlobal());

	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprTopProjectList = (*pexpr)[1];

	CLogicalGbAgg *popBottomGbAgg =
		CLogicalGbAgg::PopConvert(pexprRelational->Pop());
	CExpression *pexprChild = (*pexprRelational)[0];
	CExpression *pexprBottomProjectList = (*pexprRelational)[1];

	if (!popBottomGbAgg->FGlobal())
	{
		// bottom GbAgg must be global to prevent xform from getting applied to splitted GbAggs
		return;
	}

	if (0 < pexprTopProjectList->Arity() || 0 < pexprBottomProjectList->Arity())
	{
		// exit if any of the Gb operators has an aggregate function
		return;
	}

#ifdef GPOS_DEBUG
	// for two cascaded GbAgg ops with no agg functions, top grouping
	// columns must be a subset of bottom grouping columns
	CColRefSet *pcrsTopGrpCols =
		GPOS_NEW(mp) CColRefSet(mp, popTopGbAgg->Pdrgpcr());
	CColRefSet *pcrsBottomGrpCols =
		GPOS_NEW(mp) CColRefSet(mp, popBottomGbAgg->Pdrgpcr());
	GPOS_ASSERT(pcrsBottomGrpCols->ContainsAll(pcrsTopGrpCols));

	pcrsTopGrpCols->Release();
	pcrsBottomGrpCols->Release();
#endif	// GPOS_DEBUG

	pexprChild->AddRef();
	CExpression *pexprSelect = CUtils::PexprLogicalSelect(
		mp, pexprChild,
		CPredicateUtils::PexprConjunction(mp, NULL /*pdrgpexpr*/));

	popTopGbAgg->AddRef();
	pexprTopProjectList->AddRef();
	CExpression *pexprGbAggNew = GPOS_NEW(mp)
		CExpression(mp, popTopGbAgg, pexprSelect, pexprTopProjectList);

	pxfres->Add(pexprGbAggNew);
}


// EOF
