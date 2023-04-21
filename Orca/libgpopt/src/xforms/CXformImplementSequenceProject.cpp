//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementSequenceProject.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementSequenceProject.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementSequenceProject::CXformImplementSequenceProject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementSequenceProject::CXformImplementSequenceProject(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalSequenceProject(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // scalar child
		  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementSequenceProject::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementSequenceProject::Transform(CXformContext *pxfctxt,
										  CXformResult *pxfres,
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

	// extract members of logical sequence project operator
	CLogicalSequenceProject *popLogicalSequenceProject =
		CLogicalSequenceProject::PopConvert(pexpr->Pop());
	CDistributionSpec *pds = popLogicalSequenceProject->Pds();
	COrderSpecArray *pdrgpos = popLogicalSequenceProject->Pdrgpos();
	CWindowFrameArray *pdrgpwf = popLogicalSequenceProject->Pdrgpwf();
	pds->AddRef();
	pdrgpos->AddRef();
	pdrgpwf->AddRef();

	// assemble physical operator
	CExpression *pexprSequenceProject = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CPhysicalSequenceProject(mp, pds, pdrgpos, pdrgpwf),
		pexprRelational, pexprScalar);

	// add alternative to results
	pxfres->Add(pexprSequenceProject);
}


// EOF
