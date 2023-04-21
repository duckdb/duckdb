//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformGbAgg2ScalarAgg.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformGbAgg2ScalarAgg.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformGbAgg2HashAgg.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2ScalarAgg::CXformGbAgg2ScalarAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGbAgg2ScalarAgg::CXformGbAgg2ScalarAgg(CMemoryPool *mp)
	: CXformImplementation(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2ScalarAgg::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		grouping columns must be empty
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformGbAgg2ScalarAgg::Exfp(CExpressionHandle &exprhdl) const
{
	if (0 < CLogicalGbAgg::PopConvert(exprhdl.Pop())->Pdrgpcr()->Size() ||
		exprhdl.DeriveHasSubquery(1))
	{
		// GbAgg has grouping columns, or agg functions use subquery arguments
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2ScalarAgg::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformGbAgg2ScalarAgg::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
								 CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();
	CColRefArray *colref_array = popAgg->Pdrgpcr();
	colref_array->AddRef();

	// extract components
	CExpression *pexprRel = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	// addref children
	pexprRel->AddRef();
	pexprScalar->AddRef();

	CColRefArray *pdrgpcrArgDQA = popAgg->PdrgpcrArgDQA();
	if (pdrgpcrArgDQA != NULL && 0 != pdrgpcrArgDQA->Size())
	{
		pdrgpcrArgDQA->AddRef();
	}

	// create alternative expression
	CExpression *pexprAlt = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CPhysicalScalarAgg(
			mp, colref_array, popAgg->PdrgpcrMinimal(), popAgg->Egbaggtype(),
			popAgg->FGeneratesDuplicates(), pdrgpcrArgDQA,
			CXformUtils::FMultiStageAgg(pexpr),
			CXformUtils::FAggGenBySplitDQAXform(pexpr), popAgg->AggStage(),
			!CXformUtils::FLocalAggCreatedByEagerAggXform(pexpr)),
		pexprRel, pexprScalar);

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}


// EOF
