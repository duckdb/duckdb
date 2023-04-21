//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformSelect2PartialDynamicIndexGet.cpp
//
//	@doc:
//		Implementation of select over a partitioned table to a dynamic index get
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformSelect2PartialDynamicIndexGet.h"

#include "gpos/base.h"

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
//		CXformSelect2PartialDynamicIndexGet::CXformSelect2PartialDynamicIndexGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSelect2PartialDynamicIndexGet::CXformSelect2PartialDynamicIndexGet(
	CMemoryPool *mp)
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
//		CXformSelect2PartialDynamicIndexGet::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSelect2PartialDynamicIndexGet::Exfp(CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2PartialDynamicIndexGet::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformSelect2PartialDynamicIndexGet::Transform(CXformContext *pxfctxt,
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
	CLogicalDynamicGet *popGet =
		CLogicalDynamicGet::PopConvert(pexprRelational->Pop());

	if (popGet->IsPartial())
	{
		// already a partial dynamic get; do not try to split further
		return;
	}

	CTableDescriptor *ptabdesc = popGet->Ptabdesc();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = md_accessor->RetrieveRel(ptabdesc->MDId());
	const ULONG ulIndices = pmdrel->IndexCount();

	if (0 == ulIndices)
	{
		// no indexes on the table
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

	CPartConstraint *ppartcnstr = popGet->Ppartcnstr();
	ppartcnstr->AddRef();

	// find a candidate set of partial index combinations
	SPartDynamicIndexGetInfoArrays *pdrgpdrgppartdig =
		CXformUtils::PdrgpdrgppartdigCandidates(
			mp, md_accessor, pdrgpexpr, popGet->PdrgpdrgpcrPart(), pmdrel,
			ppartcnstr, popGet->PdrgpcrOutput(), pcrsReqd, pcrsScalarExpr,
			NULL  // pcrsAcceptedOuterRefs
		);

	// construct alternative partial index scan plans
	const ULONG ulCandidates = pdrgpdrgppartdig->Size();
	for (ULONG ul = 0; ul < ulCandidates; ul++)
	{
		SPartDynamicIndexGetInfoArray *pdrgppartdig = (*pdrgpdrgppartdig)[ul];
		CreatePartialIndexGetPlan(mp, pexpr, pdrgppartdig, pmdrel, pxfres);
	}

	ppartcnstr->Release();
	pcrsReqd->Release();
	pdrgpexpr->Release();
	pdrgpdrgppartdig->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2PartialDynamicIndexGet::CreatePartialIndexGetPlan
//
//	@doc:
//		Create a plan as a union of the given partial index get candidates and
//		possibly a dynamic table scan
//
//---------------------------------------------------------------------------
void
CXformSelect2PartialDynamicIndexGet::CreatePartialIndexGetPlan(
	CMemoryPool *mp, CExpression *pexpr,
	SPartDynamicIndexGetInfoArray *pdrgppartdig, const IMDRelation *pmdrel,
	CXformResult *pxfres) const
{
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	CLogicalDynamicGet *popGet =
		CLogicalDynamicGet::PopConvert(pexprRelational->Pop());
	CColRefArray *pdrgpcrGet = popGet->PdrgpcrOutput();

	const ULONG ulPartialIndexes = pdrgppartdig->Size();

	CColRef2dArray *pdrgpdrgpcrInput = GPOS_NEW(mp) CColRef2dArray(mp);
	CExpressionArray *pdrgpexprInput = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < ulPartialIndexes; ul++)
	{
		SPartDynamicIndexGetInfo *ppartdig = (*pdrgppartdig)[ul];

		const IMDIndex *pmdindex = ppartdig->m_pmdindex;
		CPartConstraint *ppartcnstr = ppartdig->m_part_constraint;
		CExpressionArray *pdrgpexprIndex = ppartdig->m_pdrgpexprIndex;
		CExpressionArray *pdrgpexprResidual = ppartdig->m_pdrgpexprResidual;

		CColRefArray *pdrgpcrNew = pdrgpcrGet;

		if (0 < ul)
		{
			pdrgpcrNew = CUtils::PdrgpcrCopy(mp, pdrgpcrGet);
		}
		else
		{
			pdrgpcrNew->AddRef();
		}

		CExpression *pexprDynamicScan = NULL;
		if (NULL != pmdindex)
		{
			pexprDynamicScan = CXformUtils::PexprPartialDynamicIndexGet(
				mp, popGet, pexpr->Pop()->UlOpId(), pdrgpexprIndex,
				pdrgpexprResidual, pdrgpcrNew, pmdindex, pmdrel, ppartcnstr,
				NULL,  // pcrsAcceptedOuterRefs
				NULL,  // pdrgpcrOuter
				NULL   // pdrgpcrNewOuter
			);
		}
		else
		{
			pexprDynamicScan = PexprSelectOverDynamicGet(
				mp, popGet, pexprScalar, pdrgpcrNew, ppartcnstr);
		}
		GPOS_ASSERT(NULL != pexprDynamicScan);

		pdrgpdrgpcrInput->Append(pdrgpcrNew);
		pdrgpexprInput->Append(pexprDynamicScan);
	}

	ULONG ulInput = pdrgpexprInput->Size();
	if (0 < ulInput)
	{
		CExpression *pexprResult = NULL;
		if (1 < ulInput)
		{
			pdrgpcrGet->AddRef();
			CColRefArray *pdrgpcrOuter = pdrgpcrGet;

			// construct a new union all operator
			pexprResult = GPOS_NEW(mp) CExpression(
				mp,
				GPOS_NEW(mp) CLogicalUnionAll(
					mp, pdrgpcrOuter, pdrgpdrgpcrInput, popGet->ScanId()),
				pdrgpexprInput);
		}
		else
		{
			pexprResult = (*pdrgpexprInput)[0];
			pexprResult->AddRef();

			// clean up
			pdrgpexprInput->Release();
			pdrgpdrgpcrInput->Release();
		}

		// if scalar expression involves the partitioning key, keep a SELECT node
		// on top for the purposes of partition selection
		CColRef2dArray *pdrgpdrgpcrPartKeys = popGet->PdrgpdrgpcrPart();
		CExpression *pexprPredOnPartKey =
			CPredicateUtils::PexprExtractPredicatesOnPartKeys(
				mp, pexprScalar, pdrgpdrgpcrPartKeys, NULL, /*pcrsAllowedRefs*/
				true										/*fUseConstraints*/
			);

		if (NULL != pexprPredOnPartKey)
		{
			pexprResult =
				GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalSelect(mp),
										 pexprResult, pexprPredOnPartKey);
		}

		pxfres->Add(pexprResult);

		return;
	}

	// clean up
	pdrgpdrgpcrInput->Release();
	pdrgpexprInput->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2PartialDynamicIndexGet::PexprSelectOverDynamicGet
//
//	@doc:
//		Create a partial dynamic get expression with a select on top
//
//---------------------------------------------------------------------------
CExpression *
CXformSelect2PartialDynamicIndexGet::PexprSelectOverDynamicGet(
	CMemoryPool *mp, CLogicalDynamicGet *popGet, CExpression *pexprScalar,
	CColRefArray *pdrgpcrDGet, CPartConstraint *ppartcnstr)
{
	UlongToColRefMap *colref_mapping =
		CUtils::PhmulcrMapping(mp, popGet->PdrgpcrOutput(), pdrgpcrDGet);

	// construct a partial dynamic get with the negated constraint
	CPartConstraint *ppartcnstrPartialDynamicGet =
		ppartcnstr->PpartcnstrCopyWithRemappedColumns(mp, colref_mapping,
													  true /*must_exist*/);

	CLogicalDynamicGet *popPartialDynamicGet =
		(CLogicalDynamicGet *) popGet->PopCopyWithRemappedColumns(
			mp, colref_mapping, true /*must_exist*/);
	popPartialDynamicGet->SetPartConstraint(ppartcnstrPartialDynamicGet);
	popPartialDynamicGet->SetSecondaryScanId(
		COptCtxt::PoctxtFromTLS()->UlPartIndexNextVal());
	popPartialDynamicGet->SetPartial();

	CExpression *pexprSelect = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CLogicalSelect(mp),
					GPOS_NEW(mp) CExpression(mp, popPartialDynamicGet),
					pexprScalar->PexprCopyWithRemappedColumns(
						mp, colref_mapping, true /*must_exist*/));

	colref_mapping->Release();

	return pexprSelect;
}

// EOF
