//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformSelect2Filter.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformSelect2Filter.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2Filter::CXformSelect2Filter
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSelect2Filter::CXformSelect2Filter(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalSelect(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // predicate
		  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2Filter::Exfp
//
//	@doc:
//		Compute xform promise level for a given expression handle;
// 		if scalar predicate has a subquery, then we must have an
// 		equivalent logical Apply expression created during exploration;
// 		no need for generating a Filter expression here
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSelect2Filter::Exfp(CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2Filter::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformSelect2Filter::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
							   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	// addref all children
	pexprRelational->AddRef();
	pexprScalar->AddRef();

	// assemble physical operator
	CExpression *pexprFilter = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CPhysicalFilter(mp), pexprRelational, pexprScalar);

	// add alternative to results
	pxfres->Add(pexprFilter);
}


// EOF
