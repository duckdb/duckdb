//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformPushGbBelowJoin.cpp
//
//	@doc:
//		Implementation of pushing group by below join transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformPushGbBelowJoin.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbBelowJoin::CXformPushGbBelowJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformPushGbBelowJoin::CXformPushGbBelowJoin(CMemoryPool *mp)
	:  // pattern
	  CXformExploration(GPOS_NEW(mp) CExpression(
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
		  ))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbBelowJoin::CXformPushGbBelowJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformPushGbBelowJoin::CXformPushGbBelowJoin(CExpression *pexprPattern)
	: CXformExploration(pexprPattern)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbBelowJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		we only push down global aggregates
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformPushGbBelowJoin::Exfp(CExpressionHandle &exprhdl) const
{
	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());
	if (!popGbAgg->FGlobal())
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbBelowJoin::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformPushGbBelowJoin::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
								 CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	CExpression *pexprResult = CXformUtils::PexprPushGbBelowJoin(mp, pexpr);

	if (NULL != pexprResult)
	{
		// add alternative to results
		pxfres->Add(pexprResult);
	}
}


// EOF
