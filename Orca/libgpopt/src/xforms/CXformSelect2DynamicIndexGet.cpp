//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSelect2DynamicIndexGet.cpp
//
//	@doc:
//		Implementation of select over a partitioned table to a dynamic index get
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformSelect2DynamicIndexGet.h"

#include "gpos/base.h"

#include "gpopt/base/CConstraint.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CPartConstraint.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/CMDIndexGPDB.h"
#include "naucrates/md/CMDRelationGPDB.h"
#include "naucrates/md/IMDPartConstraint.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2DynamicIndexGet::CXformSelect2DynamicIndexGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSelect2DynamicIndexGet::CXformSelect2DynamicIndexGet(CMemoryPool *mp)
	:  // pattern
	  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalSelect(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalDynamicGet(mp)),	 // relational child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate tree
		  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2DynamicIndexGet::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSelect2DynamicIndexGet::Exfp(CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2DynamicIndexGet::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformSelect2DynamicIndexGet::Transform(CXformContext *pxfctxt,
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

	// get the indexes on this relation
	CLogicalDynamicGet *popDynamicGet =
		CLogicalDynamicGet::PopConvert(pexprRelational->Pop());
	const ULONG ulIndices = popDynamicGet->Ptabdesc()->IndexCount();
	if (0 == ulIndices)
	{
		return;
	}

	// array of expressions in the scalar expression
	CExpressionArray *pdrgpexpr =
		CPredicateUtils::PdrgpexprConjuncts(mp, pexprScalar);
	GPOS_ASSERT(0 < pdrgpexpr->Size());

	// derive the scalar and relational properties to build set of required columns
	CColRefSet *pcrsOutput = pexpr->DeriveOutputColumns();
	CColRefSet *pcrsScalarExpr = pexprScalar->DeriveUsedColumns();

	CColRefSet *pcrsReqd = GPOS_NEW(mp) CColRefSet(mp);
	pcrsReqd->Include(pcrsOutput);
	pcrsReqd->Include(pcrsScalarExpr);

	// find the indexes whose included columns meet the required columns
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel =
		md_accessor->RetrieveRel(popDynamicGet->Ptabdesc()->MDId());

	for (ULONG ul = 0; ul < ulIndices; ul++)
	{
		IMDId *pmdidIndex = pmdrel->IndexMDidAt(ul);
		const IMDIndex *pmdindex = md_accessor->RetrieveIndex(pmdidIndex);
		CPartConstraint *ppartcnstrIndex = CUtils::PpartcnstrFromMDPartCnstr(
			mp, COptCtxt::PoctxtFromTLS()->Pmda(),
			popDynamicGet->PdrgpdrgpcrPart(), pmdindex->MDPartConstraint(),
			popDynamicGet->PdrgpcrOutput());
		CExpression *pexprDynamicIndexGet = CXformUtils::PexprLogicalIndexGet(
			mp, md_accessor, pexprRelational, pexpr->Pop()->UlOpId(), pdrgpexpr,
			pcrsReqd, pcrsScalarExpr, NULL /*outer_refs*/, pmdindex, pmdrel,
			false /*fAllowPartialIndex*/, ppartcnstrIndex);
		if (NULL != pexprDynamicIndexGet)
		{
			// create a redundant SELECT on top of DynamicIndexGet to be able to use predicate in partition elimination

			CExpression *pexprRedundantSelect =
				CXformUtils::PexprRedundantSelectForDynamicIndex(
					mp, pexprDynamicIndexGet);
			pexprDynamicIndexGet->Release();
			pxfres->Add(pexprRedundantSelect);
		}
	}

	pcrsReqd->Release();
	pdrgpexpr->Release();
}

// EOF
