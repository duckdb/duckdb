//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformGbAggDedup2HashAggDedup.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformGbAggDedup2HashAggDedup.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggDedup2HashAggDedup::CXformGbAggDedup2HashAggDedup
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGbAggDedup2HashAggDedup::CXformGbAggDedup2HashAggDedup(CMemoryPool *mp)
	: CXformGbAgg2HashAgg(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalGbAggDeduplicate(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggDedup2HashAggDedup::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformGbAggDedup2HashAggDedup::Transform(CXformContext *pxfctxt,
										 CXformResult *pxfres,
										 CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();
	CLogicalGbAggDeduplicate *popAggDedup =
		CLogicalGbAggDeduplicate::PopConvert(pexpr->Pop());
	CColRefArray *colref_array = popAggDedup->Pdrgpcr();
	colref_array->AddRef();

	CColRefArray *pdrgpcrKeys = popAggDedup->PdrgpcrKeys();
	pdrgpcrKeys->AddRef();

	// extract components
	CExpression *pexprRel = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];
	GPOS_ASSERT(0 == pexprScalar->Arity());

	// addref children
	pexprRel->AddRef();
	pexprScalar->AddRef();

	// create alternative expression
	CExpression *pexprAlt = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CPhysicalHashAggDeduplicate(
			mp, colref_array, popAggDedup->PdrgpcrMinimal(),
			popAggDedup->Egbaggtype(), pdrgpcrKeys,
			popAggDedup->FGeneratesDuplicates(),
			CXformUtils::FMultiStageAgg(pexpr),
			CXformUtils::FAggGenBySplitDQAXform(pexpr), popAggDedup->AggStage(),
			!CXformUtils::FLocalAggCreatedByEagerAggXform(pexpr)),
		pexprRel, pexprScalar);

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF
