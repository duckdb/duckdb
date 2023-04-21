//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CXformCollapseProject.cpp
//
//	@doc:
//		Implementation of the transform that collapses two cascaded project nodes
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformCollapseProject.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformCollapseProject::CXformCollapseProject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformCollapseProject::CXformCollapseProject(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalProject(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CLogicalProject(mp),
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
//		CXformSplitDQA::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformCollapseProject::Exfp(CExpressionHandle &	 //exprhdl
) const
{
	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformCollapseProject::Transform
//
//	@doc:
//		Actual transformation to collapse projects
//
//---------------------------------------------------------------------------
void
CXformCollapseProject::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
								 CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	CExpression *pexprCollapsed = CUtils::PexprCollapseProjects(mp, pexpr);

	if (NULL != pexprCollapsed)
	{
		pxfres->Add(pexprCollapsed);
	}
}

// EOF
