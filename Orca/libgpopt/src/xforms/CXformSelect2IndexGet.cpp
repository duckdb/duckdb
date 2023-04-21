//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSelect2IndexGet.cpp
//
//	@doc:
//		Implementation of select over a table to an index get transformation
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformSelect2IndexGet.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/CMDIndexGPDB.h"
#include "naucrates/md/CMDRelationGPDB.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2IndexGet::CXformSelect2IndexGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSelect2IndexGet::CXformSelect2IndexGet(CMemoryPool *mp)
	:  // pattern
	  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalSelect(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalGet(mp)),  // relational child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate tree
		  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2IndexGet::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSelect2IndexGet::Exfp(CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2IndexGet::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformSelect2IndexGet::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
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
	CLogicalGet *popGet = CLogicalGet::PopConvert(pexprRelational->Pop());
	const ULONG ulIndices = popGet->Ptabdesc()->IndexCount();
	if (0 == ulIndices)
	{
		return;
	}

	// array of expressions in the scalar expression
	CExpressionArray *pdrgpexpr =
		CPredicateUtils::PdrgpexprConjuncts(mp, pexprScalar);
	GPOS_ASSERT(pdrgpexpr->Size() > 0);

	// derive the scalar and relational properties to build set of required columns
	CColRefSet *pcrsOutput = pexpr->DeriveOutputColumns();
	CColRefSet *pcrsScalarExpr = pexprScalar->DeriveUsedColumns();

	CColRefSet *pcrsReqd = GPOS_NEW(mp) CColRefSet(mp);
	pcrsReqd->Include(pcrsOutput);
	pcrsReqd->Include(pcrsScalarExpr);

	// find the indexes whose included columns meet the required columns
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel =
		md_accessor->RetrieveRel(popGet->Ptabdesc()->MDId());

	for (ULONG ul = 0; ul < ulIndices; ul++)
	{
		IMDId *pmdidIndex = pmdrel->IndexMDidAt(ul);
		const IMDIndex *pmdindex = md_accessor->RetrieveIndex(pmdidIndex);
		CExpression *pexprIndexGet = CXformUtils::PexprLogicalIndexGet(
			mp, md_accessor, pexprRelational, pexpr->Pop()->UlOpId(), pdrgpexpr,
			pcrsReqd, pcrsScalarExpr, NULL /*outer_refs*/, pmdindex, pmdrel,
			false /*fAllowPartialIndex*/, NULL /*ppartcnstrIndex*/
		);
		if (NULL != pexprIndexGet)
		{
			pxfres->Add(pexprIndexGet);
		}
	}

	pcrsReqd->Release();
	pdrgpexpr->Release();
}

// EOF
