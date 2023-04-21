//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformJoin2IndexApply.cpp
//
//	@doc:
//		Implementation of Inner/Outer Join to Apply transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformJoin2IndexApply.h"

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CSubqueryHandler.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDIndex.h"

using namespace gpmd;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformJoin2IndexApply::Exfp(CExpressionHandle &exprhdl) const
{
	if (0 == exprhdl.DeriveUsedColumns(2)->Size() ||
		exprhdl.DeriveHasSubquery(2) || exprhdl.HasOuterRefs())
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::ComputeColumnSets
//
//	@doc:
//		Based on the inner and the scalar expression, it computes scalar expression
//		columns, outer references and required columns.
//		Caller does not take ownership of ppcrsScalarExpr.
//		Caller takes ownership of ppcrsOuterRefs and ppcrsReqd.
//
//---------------------------------------------------------------------------
void
CXformJoin2IndexApply::ComputeColumnSets(CMemoryPool *mp,
										 CExpression *pexprInner,
										 CExpression *pexprScalar,
										 CColRefSet **ppcrsScalarExpr,
										 CColRefSet **ppcrsOuterRefs,
										 CColRefSet **ppcrsReqd) const
{
	CColRefSet *pcrsInnerOutput = pexprInner->DeriveOutputColumns();
	*ppcrsScalarExpr = pexprScalar->DeriveUsedColumns();
	*ppcrsOuterRefs = GPOS_NEW(mp) CColRefSet(mp, **ppcrsScalarExpr);
	(*ppcrsOuterRefs)->Difference(pcrsInnerOutput);

	*ppcrsReqd = GPOS_NEW(mp) CColRefSet(mp);
	(*ppcrsReqd)->Include(pcrsInnerOutput);
	(*ppcrsReqd)->Include(*ppcrsScalarExpr);
	(*ppcrsReqd)->Difference(*ppcrsOuterRefs);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::CreateFullIndexApplyAlternatives
//
//	@doc:
//		Helper to add IndexApply expression to given xform results container
//
//---------------------------------------------------------------------------
void
CXformJoin2IndexApply::CreateHomogeneousIndexApplyAlternatives(
	CMemoryPool *mp, ULONG ulOriginOpId, CExpression *pexprOuter,
	CExpression *pexprInner, CExpression *pexprScalar,
	CTableDescriptor *ptabdescInner, CLogicalDynamicGet *popDynamicGet,
	CXformResult *pxfres, IMDIndex::EmdindexType emdtype) const
{
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(NULL != ptabdescInner);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(IMDIndex::EmdindBtree == emdtype ||
				IMDIndex::EmdindBitmap == emdtype);

	const ULONG ulIndices = ptabdescInner->IndexCount();
	if (0 == ulIndices)
	{
		return;
	}

	// derive the scalar and relational properties to build set of required columns
	CColRefSet *pcrsScalarExpr = NULL;
	CColRefSet *outer_refs = NULL;
	CColRefSet *pcrsReqd = NULL;
	ComputeColumnSets(mp, pexprInner, pexprScalar, &pcrsScalarExpr, &outer_refs,
					  &pcrsReqd);

	if (IMDIndex::EmdindBtree == emdtype)
	{
		CreateHomogeneousBtreeIndexApplyAlternatives(
			mp, ulOriginOpId, pexprOuter, pexprInner, pexprScalar,
			ptabdescInner, popDynamicGet, pcrsScalarExpr, outer_refs, pcrsReqd,
			ulIndices, pxfres);
	}
	else
	{
		CreateHomogeneousBitmapIndexApplyAlternatives(
			mp, ulOriginOpId, pexprOuter, pexprInner, pexprScalar,
			ptabdescInner, outer_refs, pcrsReqd, pxfres);
	}

	//clean-up
	pcrsReqd->Release();
	outer_refs->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::CreateHomogeneousBtreeIndexApplyAlternatives
//
//	@doc:
//		Helper to add IndexApply expression to given xform results container
//		for homogeneous b-tree indexes
//
//---------------------------------------------------------------------------
void
CXformJoin2IndexApply::CreateHomogeneousBtreeIndexApplyAlternatives(
	CMemoryPool *mp, ULONG ulOriginOpId, CExpression *pexprOuter,
	CExpression *pexprInner, CExpression *pexprScalar,
	CTableDescriptor *ptabdescInner, CLogicalDynamicGet *popDynamicGet,
	CColRefSet *pcrsScalarExpr, CColRefSet *outer_refs, CColRefSet *pcrsReqd,
	ULONG ulIndices, CXformResult *pxfres) const
{
	// array of expressions in the scalar expression
	CExpressionArray *pdrgpexpr =
		CPredicateUtils::PdrgpexprConjuncts(mp, pexprScalar);
	GPOS_ASSERT(pdrgpexpr->Size() > 0);

	// find the indexes whose included columns meet the required columns
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = md_accessor->RetrieveRel(ptabdescInner->MDId());

	for (ULONG ul = 0; ul < ulIndices; ul++)
	{
		IMDId *pmdidIndex = pmdrel->IndexMDidAt(ul);
		const IMDIndex *pmdindex = md_accessor->RetrieveIndex(pmdidIndex);

		CPartConstraint *ppartcnstrIndex = NULL;
		if (NULL != popDynamicGet)
		{
			ppartcnstrIndex = CUtils::PpartcnstrFromMDPartCnstr(
				mp, COptCtxt::PoctxtFromTLS()->Pmda(),
				popDynamicGet->PdrgpdrgpcrPart(), pmdindex->MDPartConstraint(),
				popDynamicGet->PdrgpcrOutput());
		}
		CreateAlternativesForBtreeIndex(
			mp, ulOriginOpId, pexprOuter, pexprInner, md_accessor, pdrgpexpr,
			pcrsScalarExpr, outer_refs, pcrsReqd, pmdrel, pmdindex,
			ppartcnstrIndex, pxfres);
	}

	//clean-up
	pdrgpexpr->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::CreateAlternativesForBtreeIndex
//
//	@doc:
//		Helper to add IndexApply expression to given xform results container
//		for homogeneous b-tree indexes.
//
//---------------------------------------------------------------------------
void
CXformJoin2IndexApply::CreateAlternativesForBtreeIndex(
	CMemoryPool *mp, ULONG ulOriginOpId, CExpression *pexprOuter,
	CExpression *pexprInner, CMDAccessor *md_accessor,
	CExpressionArray *pdrgpexprConjuncts, CColRefSet *pcrsScalarExpr,
	CColRefSet *outer_refs, CColRefSet *pcrsReqd, const IMDRelation *pmdrel,
	const IMDIndex *pmdindex, CPartConstraint *ppartcnstrIndex,
	CXformResult *pxfres) const
{
	CExpression *pexprLogicalIndexGet = CXformUtils::PexprLogicalIndexGet(
		mp, md_accessor, pexprInner, ulOriginOpId, pdrgpexprConjuncts, pcrsReqd,
		pcrsScalarExpr, outer_refs, pmdindex, pmdrel,
		false /*fAllowPartialIndex*/, ppartcnstrIndex);
	if (NULL != pexprLogicalIndexGet)
	{
		// second child has residual predicates, create an apply of outer and inner
		// and add it to xform results
		CColRefArray *colref_array = outer_refs->Pdrgpcr(mp);
		pexprOuter->AddRef();
		CExpression *pexprIndexApply = GPOS_NEW(mp) CExpression(
			mp, PopLogicalApply(mp, colref_array), pexprOuter,
			pexprLogicalIndexGet,
			CPredicateUtils::PexprConjunction(mp, NULL /*pdrgpexpr*/));
		pxfres->Add(pexprIndexApply);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::CreateHomogeneousBitmapIndexApplyAlternatives
//
//	@doc:
//		Helper to add IndexApply expression to given xform results container
//		for homogeneous bitmap indexes.
//
//---------------------------------------------------------------------------
void
CXformJoin2IndexApply::CreateHomogeneousBitmapIndexApplyAlternatives(
	CMemoryPool *mp, ULONG ulOriginOpId, CExpression *pexprOuter,
	CExpression *pexprInner, CExpression *pexprScalar,
	CTableDescriptor *ptabdescInner, CColRefSet *outer_refs,
	CColRefSet *pcrsReqd, CXformResult *pxfres) const
{
	CLogical *popGet = CLogical::PopConvert(pexprInner->Pop());
	CExpression *pexprLogicalIndexGet = CXformUtils::PexprBitmapTableGet(
		mp, popGet, ulOriginOpId, ptabdescInner, pexprScalar, outer_refs,
		pcrsReqd);
	if (NULL != pexprLogicalIndexGet)
	{
		// second child has residual predicates, create an apply of outer and inner
		// and add it to xform results
		CColRefArray *colref_array = outer_refs->Pdrgpcr(mp);
		pexprOuter->AddRef();
		CExpression *pexprIndexApply = GPOS_NEW(mp) CExpression(
			mp, PopLogicalApply(mp, colref_array), pexprOuter,
			pexprLogicalIndexGet,
			CPredicateUtils::PexprConjunction(mp, NULL /*pdrgpexpr*/));
		pxfres->Add(pexprIndexApply);
	}
}

// Helper to add IndexApply expressions to given xform results container
// for partial indexes. For the partitions without indexes we add one
// regular inner join.
// For instance, if there are two partial indexes plus some partitions
// not covered by any index, the result of this xform on:
//
// clang-format off
// +--CLogicalInnerJoin
//   |--CLogicalGet "my_tt_agg_small", Columns: ["symbol" (0), "event_ts" (1)]
//   |--CLogicalDynamicGet "my_tq_agg_small_part", Columns: ["ets" (11), "end_ts" (15)]
//   +--CScalarBoolOp (EboolopAnd)
//     |--CScalarCmp (>=)
//     |  |--CScalarIdent "event_ts" (1)
//     |  +--CScalarIdent "ets" (11)
//     +--CScalarCmp (<)
//        |--CScalarIdent "event_ts" (1)
//        +--CScalarIdent "end_ts" (15)
//
// will look like:
//
// +--CLogicalCTEAnchor (0)
//    +--CLogicalUnionAll ["symbol" (0), "event_ts" (1),  ...]
//     |--CLogicalUnionAll ["symbol" (0), "event_ts" (1),  ...]
//     |  |--CLogicalInnerIndexApply
//     |  |  |--CLogicalCTEConsumer (0), Columns: ["symbol" (0), "event_ts" (1), "gp_segment_id" (10)]
//     |  |  |--CLogicalDynamicIndexGet   Index Name: ("my_tq_agg_small_part_ets_end_ts_ix_1"),
//     |  |  |      Columns: ["ets" (11), "end_ts" (15) "gp_segment_id" (22)]
//     |  |  |  |--CScalarBoolOp (EboolopAnd)
//     |  |  |  |  |--CScalarCmp (<=)
//     |  |  |  |  |  |--CScalarIdent "ets" (11)
//     |  |  |  |  |  +--CScalarIdent "event_ts" (1)
//     |  |  |  |  +--CScalarCmp (>)
//     |  |  |  |     |--CScalarIdent "end_ts" (15)
//     |  |  |  |     +--CScalarIdent "event_ts" (1)
//     |  |  |  +--CScalarConst (TRUE)
//     |  |  +--CScalarConst (TRUE)
//     |  +--CLogicalInnerIndexApply
//     |     |--CLogicalCTEConsumer (0), Columns: ["symbol" (35), "event_ts" (36), "gp_segment_id" (45)]
//     |     |--CLogicalDynamicIndexGet   Index Name: ("my_tq_agg_small_part_ets_end_ts_ix_2"),
//     |     |    Columns: ["ets" (46),  "end_ts" (50), "gp_segment_id" (57)]
//     |     |  |--CScalarCmp (<=)
//     |     |  |  |--CScalarIdent "ets" (46)
//     |     |  |  +--CScalarIdent "event_ts" (36)
//     |     |  +--CScalarCmp (<)
//     |     |     |--CScalarIdent "event_ts" (36)
//     |     |     +--CScalarIdent "end_ts" (50)
//     |     +--CScalarConst (TRUE)
//     +--CLogicalInnerJoin
//          |--CLogicalCTEConsumer (0), Columns: ["symbol" (58), "event_ts" (59), "gp_segment_id" (68)]
//          |--CLogicalDynamicGet "my_tq_agg_small_part" ,
//         Columns: ["ets" (69), "end_ts" (73), "gp_segment_id" (80)]
//            +--CScalarBoolOp (EboolopAnd)
//            |--CScalarCmp (>=)
//            |  |--CScalarIdent "event_ts" (59)
//            |  +--CScalarIdent "ets" (69)
//            +--CScalarCmp (<)
//               |--CScalarIdent "event_ts" (59)
//               +--CScalarIdent "end_ts" (73)
//
// clang-format on
void
CXformJoin2IndexApply::CreatePartialIndexApplyAlternatives(
	CMemoryPool *mp, ULONG ulOriginOpId, CExpression *pexprOuter,
	CExpression *pexprInner, CExpression *pexprScalar,
	CTableDescriptor *ptabdescInner, CLogicalDynamicGet *popDynamicGet,
	CXformResult *pxfres) const
{
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(NULL != ptabdescInner);
	GPOS_ASSERT(NULL != pxfres);

	if (NULL == popDynamicGet || popDynamicGet->IsPartial())
	{
		// not a dynamic get or
		// already a partial dynamic get; do not try to split further
		return;
	}

	const ULONG ulIndices = ptabdescInner->IndexCount();
	if (0 == ulIndices)
	{
		return;
	}

	CPartConstraint *ppartcnstr = popDynamicGet->Ppartcnstr();
	ppartcnstr->AddRef();

	CColRefSet *pcrsScalarExpr = NULL;
	CColRefSet *outer_refs = NULL;
	CColRefSet *pcrsReqd = NULL;
	ComputeColumnSets(mp, pexprInner, pexprScalar, &pcrsScalarExpr, &outer_refs,
					  &pcrsReqd);

	// find a candidate set of partial index combinations
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = md_accessor->RetrieveRel(ptabdescInner->MDId());

	// array of expressions in the scalar expression
	CExpressionArray *pdrgpexpr =
		CPredicateUtils::PdrgpexprConjuncts(mp, pexprScalar);

	GPOS_ASSERT(0 < pdrgpexpr->Size());

	SPartDynamicIndexGetInfoArrays *pdrgpdrgppartdig =
		CXformUtils::PdrgpdrgppartdigCandidates(
			mp, COptCtxt::PoctxtFromTLS()->Pmda(), pdrgpexpr,
			popDynamicGet->PdrgpdrgpcrPart(), pmdrel, ppartcnstr,
			popDynamicGet->PdrgpcrOutput(), pcrsReqd, pcrsScalarExpr,
			outer_refs);



	// construct alternative partial index apply plans
	const ULONG ulCandidates = pdrgpdrgppartdig->Size();
	for (ULONG ul = 0; ul < ulCandidates; ul++)
	{
		SPartDynamicIndexGetInfoArray *pdrgppartdig = (*pdrgpdrgppartdig)[ul];
		if (0 < pdrgppartdig->Size())
		{
			CreatePartialIndexApplyPlan(mp, ulOriginOpId, pexprOuter,
										pexprScalar, outer_refs, popDynamicGet,
										pdrgppartdig, pmdrel, pxfres);
		}
	}

	ppartcnstr->Release();
	pdrgpdrgppartdig->Release();
	pcrsReqd->Release();
	outer_refs->Release();
	pdrgpexpr->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::CreatePartialIndexApplyPlan
//
//	@doc:
//		Create a plan as a union of the given partial index apply candidates and
//		possibly a regular inner join with a dynamic table scan on the inner branch.
//
//---------------------------------------------------------------------------
void
CXformJoin2IndexApply::CreatePartialIndexApplyPlan(
	CMemoryPool *mp, ULONG ulOriginOpId, CExpression *pexprOuter,
	CExpression *pexprScalar, CColRefSet *outer_refs,
	CLogicalDynamicGet *popDynamicGet,
	SPartDynamicIndexGetInfoArray *pdrgppartdig, const IMDRelation *pmdrel,
	CXformResult *pxfres) const
{
	const ULONG ulPartialIndexes = pdrgppartdig->Size();
	if (0 == ulPartialIndexes)
	{
		return;
	}

	CColRefArray *pdrgpcrGet = popDynamicGet->PdrgpcrOutput();
	const ULONG ulCTEId = COptCtxt::PoctxtFromTLS()->Pcteinfo()->next_id();

	// outer references mentioned in the scan filter: we need them to generate the IndexApply
	CColRefArray *pdrgpcrOuterRefsInScan = outer_refs->Pdrgpcr(mp);

	// columns from the outer branch of the IndexApply or Join
	// we will create copies of these columns because every CTE consumer needs to have its
	// own column ids
	CColRefArray *pdrgpcrOuter = pexprOuter->DeriveOutputColumns()->Pdrgpcr(mp);

	// positions in pdrgpcrOuter of the outer references mentioned in the scan filter
	// we need them because each time we create copies of pdrgpcrOuter, we will extract the
	// subsequence corresponding to pdrgpcrOuterRefsInScan
	ULongPtrArray *pdrgpulIndexesOfRefsInScan =
		pdrgpcrOuter->IndexesOfSubsequence(pdrgpcrOuterRefsInScan);

	GPOS_ASSERT(NULL != pdrgpulIndexesOfRefsInScan);

	(void) CXformUtils::PexprAddCTEProducer(mp, ulCTEId, pdrgpcrOuter,
											pexprOuter);
	BOOL fFirst = true;

	CColRefArray *pdrgpcrOutput = NULL;
	CExpressionArray *pdrgpexprInput = GPOS_NEW(mp) CExpressionArray(mp);
	CColRef2dArray *pdrgpdrgpcrInput = GPOS_NEW(mp) CColRef2dArray(mp);

	for (ULONG ul = 0; ul < ulPartialIndexes; ul++)
	{
		SPartDynamicIndexGetInfo *ppartdig = (*pdrgppartdig)[ul];

		const IMDIndex *pmdindex = ppartdig->m_pmdindex;
		CPartConstraint *ppartcnstr = ppartdig->m_part_constraint;
		CExpressionArray *pdrgpexprIndex = ppartdig->m_pdrgpexprIndex;
		CExpressionArray *pdrgpexprResidual = ppartdig->m_pdrgpexprResidual;

		CColRefArray *pdrgpcrOuterNew = pdrgpcrOuter;
		CColRefArray *pdrgpcrIndexGet = pdrgpcrGet;
		UlongToColRefMap *colref_mapping = NULL;
		if (fFirst)
		{
			// For the first child of the union, reuse the initial columns
			// because the output schema of the union must be identical to its first child's.
			pdrgpcrIndexGet->AddRef();
		}
		else
		{
			colref_mapping = GPOS_NEW(mp) UlongToColRefMap(mp);
			pdrgpcrOuterNew = CUtils::PdrgpcrCopy(
				mp, pdrgpcrOuter, false /*fAllComputed*/, colref_mapping);
			pdrgpcrIndexGet = CUtils::PdrgpcrCopy(
				mp, pdrgpcrGet, false /*fAllComputed*/, colref_mapping);
		}

		CExpression *pexprUnionAllChild = NULL;
		if (NULL != pmdindex)
		{
			pexprUnionAllChild = PexprIndexApplyOverCTEConsumer(
				mp, ulOriginOpId, popDynamicGet, pdrgpexprIndex,
				pdrgpexprResidual, pdrgpcrIndexGet, pmdindex, pmdrel, fFirst,
				ulCTEId, ppartcnstr, outer_refs, pdrgpcrOuter, pdrgpcrOuterNew,
				pdrgpcrOuterRefsInScan, pdrgpulIndexesOfRefsInScan);
		}
		else
		{
			pexprUnionAllChild = PexprJoinOverCTEConsumer(
				mp, ulOriginOpId, popDynamicGet, ulCTEId, pexprScalar,
				pdrgpcrIndexGet, ppartcnstr, pdrgpcrOuter, pdrgpcrOuterNew);
		}

		CRefCount::SafeRelease(pdrgpcrIndexGet);

		// if we failed to create a DynamicIndexScan, we give up
		GPOS_ASSERT(NULL != pexprUnionAllChild);

		CColRefArray *pdrgpcrNew = NULL;
		if (NULL == colref_mapping)
		{
			pdrgpcrNew = pexprUnionAllChild->DeriveOutputColumns()->Pdrgpcr(mp);
		}
		else
		{
			// inner branches of the union-all needs columns in the same order as in the outer branch
			pdrgpcrNew = CUtils::PdrgpcrRemap(mp, pdrgpcrOutput, colref_mapping,
											  true /*must_exist*/);
		}

		if (fFirst)
		{
			GPOS_ASSERT(NULL != pdrgpcrNew);

			// the output columns of a UnionAll need to be exactly the input
			// columns of its first input branch
			pdrgpcrNew->AddRef();
			pdrgpcrOutput = pdrgpcrNew;
		}
		fFirst = false;

		pdrgpdrgpcrInput->Append(pdrgpcrNew);
		pdrgpexprInput->Append(pexprUnionAllChild);

		CRefCount::SafeRelease(colref_mapping);
	}

	GPOS_ASSERT(pdrgpexprInput->Size() == pdrgpdrgpcrInput->Size());
	GPOS_ASSERT(1 < pdrgpexprInput->Size());

	CExpression *pexprResult = NULL;
	if (2 <= pdrgpexprInput->Size())
	{
		// construct a new union operator
		pexprResult = GPOS_NEW(mp) CExpression(
			mp,
			GPOS_NEW(mp) CLogicalUnionAll(mp, pdrgpcrOutput, pdrgpdrgpcrInput,
										  popDynamicGet->ScanId()),
			pdrgpexprInput);
	}
	else
	{
		GPOS_ASSERT(1 == pdrgpexprInput->Size());
		pexprResult = (*pdrgpexprInput)[0];
		pexprResult->AddRef();

		// clean up
		pdrgpdrgpcrInput->Release();
		pdrgpexprInput->Release();
		pdrgpcrOutput->Release();
	}

	pdrgpcrOuterRefsInScan->Release();
	pdrgpulIndexesOfRefsInScan->Release();

	AddUnionPlanForPartialIndexes(mp, popDynamicGet, ulCTEId, pexprResult,
								  pexprScalar, pxfres);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::PexprJoinOverCTEConsumer
//
//	@doc:
//		Create an join with a CTE consumer on the inner branch,
//		with the given partition constraint
//
//---------------------------------------------------------------------------
CExpression *
CXformJoin2IndexApply::PexprJoinOverCTEConsumer(
	CMemoryPool *mp,
	ULONG,	//  ulOriginOpId
	CLogicalDynamicGet *popDynamicGet, ULONG ulCTEId, CExpression *pexprScalar,
	CColRefArray *pdrgpcrDynamicGet, CPartConstraint *ppartcnstr,
	CColRefArray *pdrgpcrOuter, CColRefArray *pdrgpcrOuterNew) const
{
	UlongToColRefMap *colref_mapping = CUtils::PhmulcrMapping(
		mp, popDynamicGet->PdrgpcrOutput(), pdrgpcrDynamicGet);

	// construct a partial dynamic get with the negated constraint
	CPartConstraint *ppartcnstrPartialDynamicGet =
		ppartcnstr->PpartcnstrCopyWithRemappedColumns(mp, colref_mapping,
													  true /*must_exist*/);

	CLogicalDynamicGet *popPartialDynamicGet =
		(CLogicalDynamicGet *) popDynamicGet->PopCopyWithRemappedColumns(
			mp, colref_mapping, true /*must_exist*/
		);
	popPartialDynamicGet->SetPartConstraint(ppartcnstrPartialDynamicGet);
	popPartialDynamicGet->SetSecondaryScanId(
		COptCtxt::PoctxtFromTLS()->UlPartIndexNextVal());
	popPartialDynamicGet->SetPartial();

	// if there are any outer references, add them to the mapping
	ULONG ulOuterPcrs = pdrgpcrOuter->Size();
	GPOS_ASSERT(ulOuterPcrs == pdrgpcrOuterNew->Size());

	for (ULONG ul = 0; ul < ulOuterPcrs; ul++)
	{
		CColRef *pcrOld = (*pdrgpcrOuter)[ul];
		CColRef *new_colref = (*pdrgpcrOuterNew)[ul];
#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif
			colref_mapping->Insert(GPOS_NEW(mp) ULONG(pcrOld->Id()),
								   new_colref);
		GPOS_ASSERT(fInserted);
	}

	CExpression *pexprJoin = GPOS_NEW(mp)
		CExpression(mp, PopLogicalJoin(mp),
					CXformUtils::PexprCTEConsumer(mp, ulCTEId, pdrgpcrOuterNew),
					GPOS_NEW(mp) CExpression(mp, popPartialDynamicGet),
					pexprScalar->PexprCopyWithRemappedColumns(
						mp, colref_mapping, true /*must_exist*/
						));
	colref_mapping->Release();

	return pexprJoin;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::PexprIndexApplyOverCTEConsumer
//
//	@doc:
//		Create an index apply with a CTE consumer on the outer branch
//		and a dynamic get on the inner one
//
//---------------------------------------------------------------------------
CExpression *
CXformJoin2IndexApply::PexprIndexApplyOverCTEConsumer(
	CMemoryPool *mp, ULONG ulOriginOpId, CLogicalDynamicGet *popDynamicGet,
	CExpressionArray *pdrgpexprIndex, CExpressionArray *pdrgpexprResidual,
	CColRefArray *pdrgpcrIndexGet, const IMDIndex *pmdindex,
	const IMDRelation *pmdrel, BOOL fFirst, ULONG ulCTEId,
	CPartConstraint *ppartcnstr, CColRefSet *outer_refs,
	CColRefArray *pdrgpcrOuter, CColRefArray *pdrgpcrOuterNew,
	CColRefArray *pdrgpcrOuterRefsInScan,
	ULongPtrArray *pdrgpulIndexesOfRefsInScan) const
{
	CExpression *pexprDynamicScan = CXformUtils::PexprPartialDynamicIndexGet(
		mp, popDynamicGet, ulOriginOpId, pdrgpexprIndex, pdrgpexprResidual,
		pdrgpcrIndexGet, pmdindex, pmdrel, ppartcnstr, outer_refs, pdrgpcrOuter,
		pdrgpcrOuterNew);

	if (NULL == pexprDynamicScan)
	{
		return NULL;
	}

	// create an apply of outer and inner and add it to the union
	CColRefArray *pdrgpcrOuterRefsInScanNew = pdrgpcrOuterRefsInScan;
	if (fFirst)
	{
		pdrgpcrOuterRefsInScanNew->AddRef();
	}
	else
	{
		pdrgpcrOuterRefsInScanNew = CXformUtils::PdrgpcrReorderedSubsequence(
			mp, pdrgpcrOuterNew, pdrgpulIndexesOfRefsInScan);
	}

	return GPOS_NEW(mp)
		CExpression(mp, PopLogicalApply(mp, pdrgpcrOuterRefsInScanNew),
					CXformUtils::PexprCTEConsumer(mp, ulCTEId, pdrgpcrOuterNew),
					pexprDynamicScan,
					CPredicateUtils::PexprConjunction(mp, NULL /*pdrgpexpr*/));
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::PexprConstructUnionAll
//
//	@doc:
//		Create a union-all with the given children. Takes ownership of all
//		arguments.
//
//---------------------------------------------------------------------------
CExpression *
CXformJoin2IndexApply::PexprConstructUnionAll(CMemoryPool *mp,
											  CColRefArray *pdrgpcrLeftSchema,
											  CColRefArray *pdrgpcrRightSchema,
											  CExpression *pexprLeftChild,
											  CExpression *pexprRightChild,
											  ULONG scan_id) const
{
	CColRef2dArray *pdrgpdrgpcrInput = GPOS_NEW(mp) CColRef2dArray(mp);
	pdrgpdrgpcrInput->Append(pdrgpcrLeftSchema);
	pdrgpdrgpcrInput->Append(pdrgpcrRightSchema);
	pdrgpcrLeftSchema->AddRef();

	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp)
			CLogicalUnionAll(mp, pdrgpcrLeftSchema, pdrgpdrgpcrInput, scan_id),
		pexprLeftChild, pexprRightChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::AddUnionPlanForPartialIndexes
//
//	@doc:
//		Constructs a CTE Anchor over the given UnionAll and adds it to the given
//		Xform result
//
//---------------------------------------------------------------------------
void
CXformJoin2IndexApply::AddUnionPlanForPartialIndexes(
	CMemoryPool *mp, CLogicalDynamicGet *popDynamicGet, ULONG ulCTEId,
	CExpression *pexprUnion, CExpression *pexprScalar,
	CXformResult *pxfres) const
{
	if (NULL == pexprUnion)
	{
		return;
	}

	// if scalar expression involves the partitioning key, keep a SELECT node
	// on top for the purposes of partition selection
	CColRef2dArray *pdrgpdrgpcrPartKeys = popDynamicGet->PdrgpdrgpcrPart();
	CExpression *pexprPredOnPartKey =
		CPredicateUtils::PexprExtractPredicatesOnPartKeys(
			mp, pexprScalar, pdrgpdrgpcrPartKeys, NULL, /*pcrsAllowedRefs*/
			true										/*fUseConstraints*/
		);

	if (NULL != pexprPredOnPartKey)
	{
		pexprUnion =
			GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalSelect(mp),
									 pexprUnion, pexprPredOnPartKey);
	}

	CExpression *pexprAnchor = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalCTEAnchor(mp, ulCTEId), pexprUnion);
	pxfres->Add(pexprAnchor);
}

// EOF
