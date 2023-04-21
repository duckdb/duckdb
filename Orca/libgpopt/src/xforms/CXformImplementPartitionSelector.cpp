//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CXformImplementPartitionSelector.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementPartitionSelector.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementPartitionSelector::CXformImplementPartitionSelector
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementPartitionSelector::CXformImplementPartitionSelector(
	CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalPartitionSelector(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp))  // relational child
		  ))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementPartitionSelector::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementPartitionSelector::Transform(CXformContext *pxfctxt,
											CXformResult *pxfres,
											CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CLogicalPartitionSelector *popSelector =
		CLogicalPartitionSelector::PopConvert(pexpr->Pop());
	CExpression *pexprRelational = (*pexpr)[0];

	IMDId *mdid = popSelector->MDId();

	// addref all components
	pexprRelational->AddRef();
	mdid->AddRef();

	UlongToExprMap *phmulexprFilter = GPOS_NEW(mp) UlongToExprMap(mp);

	const ULONG ulLevels = popSelector->UlPartLevels();
	for (ULONG ul = 0; ul < ulLevels; ul++)
	{
		CExpression *pexprFilter = popSelector->PexprPartFilter(ul);
		GPOS_ASSERT(NULL != pexprFilter);
		pexprFilter->AddRef();
#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif
			phmulexprFilter->Insert(GPOS_NEW(mp) ULONG(ul), pexprFilter);
		GPOS_ASSERT(fInserted);
	}

	// assemble physical operator
	CPhysicalPartitionSelectorDML *popPhysicalPartitionSelector =
		GPOS_NEW(mp) CPhysicalPartitionSelectorDML(mp, mdid, phmulexprFilter,
												   popSelector->PcrOid());

	CExpression *pexprPartitionSelector = GPOS_NEW(mp)
		CExpression(mp, popPhysicalPartitionSelector, pexprRelational);

	// add alternative to results
	pxfres->Add(pexprPartitionSelector);
}

// EOF
