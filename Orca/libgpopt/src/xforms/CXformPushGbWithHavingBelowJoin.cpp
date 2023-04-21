//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformPushGbWithHavingBelowJoin.cpp
//
//	@doc:
//		Implementation of pushing group by below join transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformPushGbWithHavingBelowJoin.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbWithHavingBelowJoin::CXformPushGbWithHavingBelowJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformPushGbWithHavingBelowJoin::CXformPushGbWithHavingBelowJoin(
	CMemoryPool *mp)
	:  // pattern
	  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalSelect(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // join outer child
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // join inner child
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp) CPatternTree(mp))  // join predicate
				  ),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp))  // scalar project list
			  ),
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // Having clause
		  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbWithHavingBelowJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		we only push down global aggregates
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformPushGbWithHavingBelowJoin::Exfp(CExpressionHandle &  // exprhdl
) const
{
	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbWithHavingBelowJoin::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformPushGbWithHavingBelowJoin::Transform(CXformContext *pxfctxt,
										   CXformResult *pxfres,
										   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	CExpression *pexprGb = (*pexpr)[0];
	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(pexprGb->Pop());
	if (!popGbAgg->FGlobal())
	{
		// xform only applies to global aggs
		return;
	}

	CExpression *pexprResult = CXformUtils::PexprPushGbBelowJoin(mp, pexpr);

	if (NULL != pexprResult)
	{
		// add alternative to results
		pxfres->Add(pexprResult);
	}
}


// EOF
