//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSplitGbAgg.cpp
//
//	@doc:
//		Implementation of the splitting of an aggregate into a pair of
//		local and global aggregate
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformSplitGbAgg.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefComputed.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/ops.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDAggregate.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitGbAgg::CXformSplitGbAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSplitGbAgg::CXformSplitGbAgg(CMemoryPool *mp)
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
//		CXformSplitGbAgg::CXformSplitGbAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSplitGbAgg::CXformSplitGbAgg(CExpression *pexprPattern)
	: CXformExploration(pexprPattern)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitGbAgg::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSplitGbAgg::Exfp(CExpressionHandle &exprhdl) const
{
	// do not split aggregate if it is a local aggregate, has distinct aggs, has outer references,
	// or return types of Agg functions are ambiguous
	if (!CLogicalGbAgg::PopConvert(exprhdl.Pop())->FGlobal() ||
		0 < exprhdl.DeriveTotalDistinctAggs(1) ||
		0 < exprhdl.DeriveOuterReferences()->Size() ||
		NULL == exprhdl.PexprScalarExactChild(1) ||
		CXformUtils::FHasAmbiguousType(
			exprhdl.PexprScalarExactChild(1 /*child_index*/),
			COptCtxt::PoctxtFromTLS()->Pmda()))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitGbAgg::Transform
//
//	@doc:
//		Actual transformation to expand a global aggregate into a pair of
//		local and global aggregate
//
//---------------------------------------------------------------------------
void
CXformSplitGbAgg::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
							CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();
	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());

	// extract components
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprProjectList = (*pexpr)[1];

	// check if the transformation is applicable
	if (!FApplicable(pexprProjectList))
	{
		return;
	}

	pexprRelational->AddRef();

	CExpression *pexprProjectListLocal = NULL;
	CExpression *pexprProjectListGlobal = NULL;

	(void) PopulateLocalGlobalProjectList(
		mp, pexprProjectList, &pexprProjectListLocal, &pexprProjectListGlobal);

	GPOS_ASSERT(NULL != pexprProjectListLocal && NULL != pexprProjectListLocal);

	CColRefArray *colref_array = popAgg->Pdrgpcr();

	colref_array->AddRef();
	CColRefArray *pdrgpcrLocal = colref_array;

	colref_array->AddRef();
	CColRefArray *pdrgpcrGlobal = colref_array;

	CColRefArray *pdrgpcrMinimal = popAgg->PdrgpcrMinimal();
	if (NULL != pdrgpcrMinimal)
	{
		// addref minimal grouping columns twice to be used in local and global aggregate
		pdrgpcrMinimal->AddRef();
		pdrgpcrMinimal->AddRef();
	}

	CExpression *local_expr = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CLogicalGbAgg(mp, pdrgpcrLocal, pdrgpcrMinimal,
								   COperator::EgbaggtypeLocal /*egbaggtype*/
								   ),
		pexprRelational, pexprProjectListLocal);

	CExpression *pexprGlobal = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CLogicalGbAgg(mp, pdrgpcrGlobal, pdrgpcrMinimal,
								   COperator::EgbaggtypeGlobal /*egbaggtype*/
								   ),
		local_expr, pexprProjectListGlobal);

	pxfres->Add(pexprGlobal);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitGbAgg::PopulateLocalGlobalProjectList
//
//	@doc:
//		Populate the local or global project list from the input project list
//
//---------------------------------------------------------------------------
void
CXformSplitGbAgg::PopulateLocalGlobalProjectList(
	CMemoryPool *mp, CExpression *pexprProjList,
	CExpression **ppexprProjListLocal, CExpression **ppexprProjListGlobal)
{
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// list of project elements for the new local and global aggregates
	CExpressionArray *pdrgpexprProjElemLocal =
		GPOS_NEW(mp) CExpressionArray(mp);
	CExpressionArray *pdrgpexprProjElemGlobal =
		GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG arity = pexprProjList->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprProgElem = (*pexprProjList)[ul];
		CScalarProjectElement *popScPrEl =
			CScalarProjectElement::PopConvert(pexprProgElem->Pop());

		// get the scalar agg func
		CExpression *pexprAggFunc = (*pexprProgElem)[0];
		CScalarAggFunc *popScAggFunc =
			CScalarAggFunc::PopConvert(pexprAggFunc->Pop());

		popScAggFunc->MDId()->AddRef();
		CScalarAggFunc *popScAggFuncLocal = CUtils::PopAggFunc(
			mp, popScAggFunc->MDId(),
			GPOS_NEW(mp)
				CWStringConst(mp, popScAggFunc->PstrAggFunc()->GetBuffer()),
			popScAggFunc->IsDistinct(), EaggfuncstageLocal, /* fGlobal */
			true											/* fSplit */
		);

		popScAggFunc->MDId()->AddRef();
		CScalarAggFunc *popScAggFuncGlobal = CUtils::PopAggFunc(
			mp, popScAggFunc->MDId(),
			GPOS_NEW(mp)
				CWStringConst(mp, popScAggFunc->PstrAggFunc()->GetBuffer()),
			false /* is_distinct */, EaggfuncstageGlobal, /* fGlobal */
			true										  /* fSplit */
		);

		// determine column reference for the new project element
		const IMDAggregate *pmdagg =
			md_accessor->RetrieveAgg(popScAggFunc->MDId());
		const IMDType *pmdtype =
			md_accessor->RetrieveType(pmdagg->GetIntermediateResultTypeMdid());
		CColRef *pcrLocal =
			col_factory->PcrCreate(pmdtype, default_type_modifier);
		CColRef *pcrGlobal = popScPrEl->Pcr();

		// create a new local aggregate function
		// create array of arguments for the aggregate function
		CExpressionArray *pdrgpexprAgg = pexprAggFunc->PdrgPexpr();

		pdrgpexprAgg->AddRef();
		CExpressionArray *pdrgpexprLocal = pdrgpexprAgg;

		CExpression *pexprAggFuncLocal =
			GPOS_NEW(mp) CExpression(mp, popScAggFuncLocal, pdrgpexprLocal);

		// create a new global aggregate function adding the column reference of the
		// intermediate result to the arguments of the global aggregate function
		CExpressionArray *pdrgpexprGlobal = GPOS_NEW(mp) CExpressionArray(mp);
		CExpression *pexprArg = CUtils::PexprScalarIdent(mp, pcrLocal);
		pdrgpexprGlobal->Append(pexprArg);

		CExpression *pexprAggFuncGlobal =
			GPOS_NEW(mp) CExpression(mp, popScAggFuncGlobal, pdrgpexprGlobal);

		// create new project elements for the aggregate functions
		CExpression *pexprProjElemLocal =
			CUtils::PexprScalarProjectElement(mp, pcrLocal, pexprAggFuncLocal);

		CExpression *pexprProjElemGlobal = CUtils::PexprScalarProjectElement(
			mp, pcrGlobal, pexprAggFuncGlobal);

		pdrgpexprProjElemLocal->Append(pexprProjElemLocal);
		pdrgpexprProjElemGlobal->Append(pexprProjElemGlobal);
	}

	// create new project lists
	*ppexprProjListLocal = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarProjectList(mp), pdrgpexprProjElemLocal);

	*ppexprProjListGlobal = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarProjectList(mp), pdrgpexprProjElemGlobal);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitGbAgg::FApplicable
//
//	@doc:
//		Check if we the transformation is applicable (no distinct qualified
//		aggregate (DQA)) present
//
//---------------------------------------------------------------------------
BOOL
CXformSplitGbAgg::FApplicable(CExpression *pexpr)
{
	const ULONG arity = pexpr->Arity();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprPrEl = (*pexpr)[ul];

		// get the scalar child of the project element
		CExpression *pexprAggFunc = (*pexprPrEl)[0];
		CScalarAggFunc *popScAggFunc =
			CScalarAggFunc::PopConvert(pexprAggFunc->Pop());

		if (popScAggFunc->IsDistinct() ||
			!md_accessor->RetrieveAgg(popScAggFunc->MDId())->IsSplittable())
		{
			return false;
		}
	}

	return true;
}

// EOF
