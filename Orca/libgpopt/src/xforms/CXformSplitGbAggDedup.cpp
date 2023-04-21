//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformSplitGbAggDedup.cpp
//
//	@doc:
//		Implementation of the splitting of a dedup aggregate into a pair of
//		local and global aggregates
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformSplitGbAggDedup.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefComputed.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/ops.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "naucrates/md/IMDAggregate.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitGbAggDedup::CXformSplitGbAggDedup
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSplitGbAggDedup::CXformSplitGbAggDedup(CMemoryPool *mp)
	: CXformSplitGbAgg(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalGbAggDeduplicate(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp))  // scalar project list
			  ))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitGbAggDedup::Transform
//
//	@doc:
//		Actual transformation to expand a global aggregate into a pair of
//		local and global aggregate
//
//---------------------------------------------------------------------------
void
CXformSplitGbAggDedup::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
								 CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();
	CLogicalGbAggDeduplicate *popAggDedup =
		CLogicalGbAggDeduplicate::PopConvert(pexpr->Pop());

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

	CColRefArray *colref_array = popAggDedup->Pdrgpcr();
	colref_array->AddRef();
	colref_array->AddRef();

	CColRefArray *pdrgpcrMinimal = popAggDedup->PdrgpcrMinimal();
	if (NULL != pdrgpcrMinimal)
	{
		pdrgpcrMinimal->AddRef();
		pdrgpcrMinimal->AddRef();
	}

	CColRefArray *pdrgpcrKeys = popAggDedup->PdrgpcrKeys();
	pdrgpcrKeys->AddRef();
	pdrgpcrKeys->AddRef();

	CExpression *local_expr = GPOS_NEW(mp)
		CExpression(mp,
					GPOS_NEW(mp) CLogicalGbAggDeduplicate(
						mp, colref_array, pdrgpcrMinimal,
						COperator::EgbaggtypeLocal /*egbaggtype*/, pdrgpcrKeys),
					pexprRelational, pexprProjectListLocal);

	CExpression *pexprGlobal = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CLogicalGbAggDeduplicate(
			mp, colref_array, pdrgpcrMinimal,
			COperator::EgbaggtypeGlobal /*egbaggtype*/, pdrgpcrKeys),
		local_expr, pexprProjectListGlobal);

	pxfres->Add(pexprGlobal);
}

// EOF
