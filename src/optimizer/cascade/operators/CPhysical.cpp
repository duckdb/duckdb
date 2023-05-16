//---------------------------------------------------------------------------
//	@filename:
//		CPhysical.cpp
//
//	@doc:
//		Implementation of basic physical operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CPhysical.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CCTEMap.h"
#include "duckdb/optimizer/cascade/base/CCTEReq.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecAny.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecHashed.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecRandom.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecReplicated.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecSingleton.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropPlan.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/CPartIndexMap.h"
#include "duckdb/optimizer/cascade/base/CReqdPropPlan.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CScalarIdent.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::CPhysical
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CPhysical::CPhysical(CMemoryPool *mp)
	: COperator(mp), m_phmrcr(NULL), m_pdrgpulpOptReqsExpanded(NULL), m_ulTotalOptRequests(1)  // by default, an operator creates a single request for each property
{
	GPOS_ASSERT(NULL != mp);
	for (ULONG ul = 0; ul < GPOPT_PLAN_PROPS; ul++)
	{
		// by default, an operator creates a single request for each property
		m_rgulOptReqs[ul] = 1;
	}
	UpdateOptRequests(0 /*ulPropIndex*/, 1 /*ulOrderReqs*/);
	m_phmrcr = GPOS_NEW(mp) ReqdColsReqToColRefSetMap(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::UpdateOptRequests
//
//	@doc:
//		Update number of requests of a given property,
//		re-compute total number of optimization requests as the product
//		of all properties requests
//
//---------------------------------------------------------------------------
void
CPhysical::UpdateOptRequests(ULONG ulPropIndex, ULONG ulRequests)
{
	GPOS_ASSERT(ulPropIndex < GPOPT_PLAN_PROPS);

	// update property requests
	m_rgulOptReqs[ulPropIndex] = ulRequests;

	// compute new value of total requests
	ULONG ulOptReqs = 1;
	for (ULONG ul = 0; ul < GPOPT_PLAN_PROPS; ul++)
	{
		ulOptReqs = ulOptReqs * m_rgulOptReqs[ul];
	}

	// update total requests
	m_ulTotalOptRequests = ulOptReqs;

	// update expanded requests
	const ULONG ulOrderRequests = UlOrderRequests();
	const ULONG ulDistrRequests = UlDistrRequests();
	const ULONG ulRewindRequests = UlRewindRequests();
	const ULONG ulPartPropagateRequests = UlPartPropagateRequests();

	CRefCount::SafeRelease(m_pdrgpulpOptReqsExpanded);
	m_pdrgpulpOptReqsExpanded = NULL;
	m_pdrgpulpOptReqsExpanded = GPOS_NEW(m_mp) UlongPtrArray(m_mp);
	for (ULONG ulOrder = 0; ulOrder < ulOrderRequests; ulOrder++)
	{
		for (ULONG ulDistr = 0; ulDistr < ulDistrRequests; ulDistr++)
		{
			for (ULONG ulRewind = 0; ulRewind < ulRewindRequests; ulRewind++)
			{
				for (ULONG ulPartPropagate = 0;
					 ulPartPropagate < ulPartPropagateRequests;
					 ulPartPropagate++)
				{
					ULONG_PTR *pulpRequest =
						GPOS_NEW_ARRAY(m_mp, ULONG_PTR, GPOPT_PLAN_PROPS);

					pulpRequest[0] = ulOrder;
					pulpRequest[1] = ulDistr;
					pulpRequest[2] = ulRewind;
					pulpRequest[3] = ulPartPropagate;

					m_pdrgpulpOptReqsExpanded->Append(pulpRequest);
				}
			}
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::LookupReqNo
//
//	@doc:
//		Map input request number to order, distribution, rewindability and
//		partition propagation requests
//
//---------------------------------------------------------------------------
void
CPhysical::LookupRequest(
	ULONG ulReqNo,				// input: request number
	ULONG *pulOrderReq,			// output: order request number
	ULONG *pulDistrReq,			// output: distribution request number
	ULONG *pulRewindReq,		// output: rewindability request number
	ULONG *pulPartPropagateReq	// output: partition propagation request number
)
{
	GPOS_ASSERT(NULL != m_pdrgpulpOptReqsExpanded);
	GPOS_ASSERT(ulReqNo < m_pdrgpulpOptReqsExpanded->Size());
	GPOS_ASSERT(NULL != pulOrderReq);
	GPOS_ASSERT(NULL != pulDistrReq);
	GPOS_ASSERT(NULL != pulRewindReq);
	GPOS_ASSERT(NULL != pulPartPropagateReq);

	ULONG_PTR *pulpRequest = (*m_pdrgpulpOptReqsExpanded)[ulReqNo];
	*pulOrderReq = (ULONG) pulpRequest[0];
	*pulDistrReq = (ULONG) pulpRequest[1];
	*pulRewindReq = (ULONG) pulpRequest[2];
	*pulPartPropagateReq = (ULONG) pulpRequest[3];
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PdpCreate
//
//	@doc:
//		Create base container of derived properties
//
//---------------------------------------------------------------------------
CDrvdProp *
CPhysical::PdpCreate(CMemoryPool *mp) const
{
	return GPOS_NEW(mp) CDrvdPropPlan();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CPhysical::PopCopyWithRemappedColumns(CMemoryPool *,	   //mp,
									  UlongToColRefMap *,  //colref_mapping,
									  BOOL				   //must_exist
)
{
	GPOS_ASSERT(!"Invalid call of CPhysical::PopCopyWithRemappedColumns");
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PrpCreate
//
//	@doc:
//		Create base container of required properties
//
//---------------------------------------------------------------------------
CReqdProp *
CPhysical::PrpCreate(CMemoryPool *mp) const
{
	return GPOS_NEW(mp) CReqdPropPlan();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::CReqdColsRequest::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CPhysical::CReqdColsRequest::HashValue(const CReqdColsRequest *prcr)
{
	GPOS_ASSERT(NULL != prcr);

	ULONG ulHash = prcr->GetColRefSet()->HashValue();
	ulHash = CombineHashes(ulHash, prcr->UlChildIndex());
	;

	return CombineHashes(ulHash, prcr->UlScalarChildIndex());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::CReqdColsRequest::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CPhysical::CReqdColsRequest::Equals(const CReqdColsRequest *prcrFst,
									const CReqdColsRequest *prcrSnd)
{
	GPOS_ASSERT(NULL != prcrFst);
	GPOS_ASSERT(NULL != prcrSnd);

	return prcrFst->UlChildIndex() == prcrSnd->UlChildIndex() &&
		   prcrFst->UlScalarChildIndex() == prcrSnd->UlScalarChildIndex() &&
		   prcrFst->GetColRefSet()->Equals(prcrSnd->GetColRefSet());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PdsCompute
//
//	@doc:
//		Compute the distribution spec given the table descriptor
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysical::PdsCompute(CMemoryPool *mp, const CTableDescriptor *ptabdesc,
					  CColRefArray *pdrgpcrOutput)
{
	CDistributionSpec *pds = NULL;

	switch (ptabdesc->GetRelDistribution())
	{
		case IMDRelation::EreldistrMasterOnly:
			pds = GPOS_NEW(mp) CDistributionSpecSingleton(
				CDistributionSpecSingleton::EstMaster);
			break;

		case IMDRelation::EreldistrRandom:
			pds = GPOS_NEW(mp) CDistributionSpecRandom();
			break;

		case IMDRelation::EreldistrHash:
		{
			const CColumnDescriptorArray *pdrgpcoldesc =
				ptabdesc->PdrgpcoldescDist();
			CColRefArray *colref_array = GPOS_NEW(mp) CColRefArray(mp);

			const ULONG size = pdrgpcoldesc->Size();
			for (ULONG ul = 0; ul < size; ul++)
			{
				CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[ul];
				ULONG ulPos =
					ptabdesc->UlPos(pcoldesc, ptabdesc->Pdrgpcoldesc());

				GPOS_ASSERT(ulPos < ptabdesc->Pdrgpcoldesc()->Size() &&
							"Column not found");

				CColRef *colref = (*pdrgpcrOutput)[ulPos];
				colref_array->Append(colref);
			}

			CExpressionArray *pdrgpexpr =
				CUtils::PdrgpexprScalarIdents(mp, colref_array);
			colref_array->Release();

			pds = GPOS_NEW(mp)
				CDistributionSpecHashed(pdrgpexpr, true /*fNullsColocated*/);
			break;
		}

		case IMDRelation::EreldistrReplicated:
			return GPOS_NEW(mp) CDistributionSpecReplicated();
			break;

		default:
			GPOS_ASSERT(!"Invalid distribution policy");
	}

	return pds;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PosPassThru
//
//	@doc:
//		Helper for a simple case of of computing child's required sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysical::PosPassThru(CMemoryPool *,		 // mp
					   CExpressionHandle &,	 // exprhdl
					   COrderSpec *posRequired,
					   ULONG  // child_index
)
{
	posRequired->AddRef();

	return posRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PdsPassThru
//
//	@doc:
//		Helper for a simple case of computing child's required distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysical::PdsPassThru(CMemoryPool *,		 // mp
					   CExpressionHandle &,	 // exprhdl
					   CDistributionSpec *pdsRequired,
					   ULONG  // child_index
)
{
	pdsRequired->AddRef();

	return pdsRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PdsSingletonExecutionOrReplicated
//
//	@doc:
//		Helper for computing child's required distribution - Singleton or Replicated
//		1. If the expression must execute on single host - require Singleton
//		2. If the expression has outer references        - require Singleton or Replicated
//		                                                   based on the optimization request
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysical::PdsRequireSingletonOrReplicated(CMemoryPool *mp,
										   CExpressionHandle &exprhdl,
										   CDistributionSpec *pdsRequired,
										   ULONG child_index, ULONG ulOptReq)
{
	GPOS_ASSERT(2 > ulOptReq);

	// if expression has to execute on a single host then we need a gather motion
	if (exprhdl.NeedsSingletonExecution())
	{
		return PdsRequireSingleton(mp, exprhdl, pdsRequired, child_index);
	}

	// if there are outer references, then we need a broadcast (or a gather)
	if (exprhdl.HasOuterRefs())
	{
		if (0 == ulOptReq)
		{
			return GPOS_NEW(mp) CDistributionSpecReplicated();
		}

		return GPOS_NEW(mp) CDistributionSpecSingleton();
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PdsUnary
//
//	@doc:
//		Helper for computing child's required distribution in unary operators
//		with a scalar child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysical::PdsUnary(CMemoryPool *mp, CExpressionHandle &exprhdl,
					CDistributionSpec *pdsRequired, ULONG child_index,
					ULONG ulOptReq)
{
	GPOS_ASSERT(0 == child_index);
	GPOS_ASSERT(2 > ulOptReq);

	// check if singleton/replicated distribution needs to be requested
	CDistributionSpec *pds = PdsRequireSingletonOrReplicated(
		mp, exprhdl, pdsRequired, child_index, ulOptReq);
	if (NULL != pds)
	{
		return pds;
	}

	// operator does not have distribution requirements, required distribution
	// will be enforced on its output
	return GPOS_NEW(mp) CDistributionSpecAny(exprhdl.Pop()->Eopid());
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PrsPassThru
//
//	@doc:
//		Helper for a simple case of of computing child's required rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysical::PrsPassThru(CMemoryPool *,		 // mp
					   CExpressionHandle &,	 // exprhdl
					   CRewindabilitySpec *prsRequired,
					   ULONG  // child_index
)
{
	prsRequired->AddRef();

	return prsRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PosDerivePassThruOuter
//
//	@doc:
//		Helper for common case of sort order derivation
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysical::PosDerivePassThruOuter(CExpressionHandle &exprhdl)
{
	COrderSpec *pos = exprhdl.Pdpplan(0 /*child_index*/)->Pos();
	pos->AddRef();

	return pos;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PdsDerivePassThruOuter
//
//	@doc:
//		Helper for common case of distribution derivation
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysical::PdsDerivePassThruOuter(CExpressionHandle &exprhdl)
{
	CDistributionSpec *pds = exprhdl.Pdpplan(0 /*child_index*/)->Pds();
	pds->AddRef();

	return pds;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PrsDerivePassThruOuter
//
//	@doc:
//		Helper for common case of rewindability derivation
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysical::PrsDerivePassThruOuter(CMemoryPool *mp, CExpressionHandle &exprhdl)
{
	CRewindabilitySpec *prs = exprhdl.Pdpplan(0 /*child_index*/)->Prs();

	// I cannot derive mark-restorable just because my child is mark-restorable.
	// However, I am rewindable.
	if (CRewindabilitySpec::ErtMarkRestore == prs->Ert())
	{
		prs = GPOS_NEW(mp)
			CRewindabilitySpec(CRewindabilitySpec::ErtRewindable, prs->Emht());
	}
	else
	{
		prs->AddRef();
	}

	return prs;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PcrsChildReqd
//
//	@doc:
//		Helper for computing required output columns of the n-th child;
//		the caller must be an operator whose ulScalarIndex-th child is a
//		scalar
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysical::PcrsChildReqd(CMemoryPool *mp, CExpressionHandle &exprhdl,
						 CColRefSet *pcrsRequired, ULONG child_index,
						 ULONG ulScalarIndex)
{
	pcrsRequired->AddRef();
	CReqdColsRequest *prcr =
		GPOS_NEW(mp) CReqdColsRequest(pcrsRequired, child_index, ulScalarIndex);
	CColRefSet *pcrs = NULL;

	// lookup required columns map first
	pcrs = m_phmrcr->Find(prcr);
	if (NULL != pcrs)
	{
		prcr->Release();
		pcrs->AddRef();
		return pcrs;
	}

	// request was not found in map -- we need to compute it
	pcrs = GPOS_NEW(mp) CColRefSet(mp, *pcrsRequired);
	if (gpos::ulong_max != ulScalarIndex)
	{
		// include used columns and exclude defined columns of scalar child
		pcrs->Union(exprhdl.DeriveUsedColumns(ulScalarIndex));
		pcrs->Exclude(exprhdl.DeriveDefinedColumns(ulScalarIndex));
	}

	// intersect computed column set with child's output columns
	pcrs->Intersection(exprhdl.DeriveOutputColumns(child_index));

	// insert request in map
	pcrs->AddRef();
#ifdef GPOS_DEBUG
	BOOL fSuccess =
#endif	// GPOS_DEBUG
		m_phmrcr->Insert(prcr, pcrs);
	GPOS_ASSERT(fSuccess);

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::FUnaryProvidesReqdCols
//
//	@doc:
//		Helper for checking if output columns of a unary operator that defines
//		no new columns include the required columns
//
//---------------------------------------------------------------------------
BOOL
CPhysical::FUnaryProvidesReqdCols(CExpressionHandle &exprhdl,
								  CColRefSet *pcrsRequired)
{
	GPOS_ASSERT(NULL != pcrsRequired);

	CColRefSet *pcrsOutput = exprhdl.DeriveOutputColumns(0 /*child_index*/);

	return pcrsOutput->ContainsAll(pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PdssMatching
//
//	@doc:
//		Compute a singleton distribution matching the given distribution
//
//---------------------------------------------------------------------------
CDistributionSpecSingleton *
CPhysical::PdssMatching(CMemoryPool *mp, CDistributionSpecSingleton *pdss)
{
	CDistributionSpecSingleton::ESegmentType est =
		CDistributionSpecSingleton::EstSegment;
	if (pdss->FOnMaster())
	{
		est = CDistributionSpecSingleton::EstMaster;
	}

	return GPOS_NEW(mp) CDistributionSpecSingleton(est);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PppsRequiredPushThru
//
//	@doc:
//		Helper for pushing required partition propagation to the child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysical::PppsRequiredPushThru(CMemoryPool *,		  // mp,
								CExpressionHandle &,  // exprhdl,
								CPartitionPropagationSpec *pppsRequired,
								ULONG  // child_index
)
{
	GPOS_ASSERT(NULL != pppsRequired);

	// required partition propagation has been initialized already: pass it down
	pppsRequired->AddRef();
	return pppsRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PcterPushThru
//
//	@doc:
//		Helper for pushing cte requirement to the child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysical::PcterPushThru(CCTEReq *pcter)
{
	GPOS_ASSERT(NULL != pcter);
	pcter->AddRef();
	return pcter;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PcmCombine
//
//	@doc:
//		Combine the derived CTE maps of the first n children
//		of the given expression handle
//
//---------------------------------------------------------------------------
CCTEMap *
CPhysical::PcmCombine(CMemoryPool *mp, CDrvdPropArray *pdrgpdpCtxt)
{
	GPOS_ASSERT(NULL != pdrgpdpCtxt);

	const ULONG size = pdrgpdpCtxt->Size();
	CCTEMap *pcmCombined = GPOS_NEW(mp) CCTEMap(mp);
	for (ULONG ul = 0; ul < size; ul++)
	{
		CCTEMap *pcmChild =
			CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[ul])->GetCostModel();

		// get the remaining requirements that have not been met by child
		CCTEMap *pcm = CCTEMap::PcmCombine(mp, *pcmCombined, *pcmChild);
		pcmCombined->Release();
		pcmCombined = pcm;
	}

	return pcmCombined;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PcterNAry
//
//	@doc:
//		Helper for computing cte requirement for the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysical::PcterNAry(CMemoryPool *mp, CExpressionHandle &exprhdl,
					 CCTEReq *pcter, ULONG child_index,
					 CDrvdPropArray *pdrgpdpCtxt) const
{
	GPOS_ASSERT(NULL != pcter);

	if (EceoLeftToRight == Eceo())
	{
		ULONG ulLastNonScalarChild = exprhdl.UlLastNonScalarChild();
		if (gpos::ulong_max != ulLastNonScalarChild &&
			child_index < ulLastNonScalarChild)
		{
			return pcter->PcterAllOptional(mp);
		}
	}
	else
	{
		GPOS_ASSERT(EceoRightToLeft == Eceo());

		ULONG ulFirstNonScalarChild = exprhdl.UlFirstNonScalarChild();
		if (gpos::ulong_max != ulFirstNonScalarChild &&
			child_index > ulFirstNonScalarChild)
		{
			return pcter->PcterAllOptional(mp);
		}
	}

	CCTEMap *pcmCombined = PcmCombine(mp, pdrgpdpCtxt);

	// pass the remaining requirements that have not been resolved
	CCTEReq *pcterUnresolved = pcter->PcterUnresolved(mp, pcmCombined);
	pcmCombined->Release();

	return pcterUnresolved;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PppsRequiredPushThruNAry
//
//	@doc:
//		Helper for pushing required partition propagation to the children of
//		an n-ary operator
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysical::PppsRequiredPushThruNAry(CMemoryPool *mp, CExpressionHandle &exprhdl,
									CPartitionPropagationSpec *pppsReqd,
									ULONG child_index)
{
	GPOS_ASSERT(NULL != pppsReqd);


	CPartIndexMap *ppimReqd = pppsReqd->Ppim();
	CPartFilterMap *ppfmReqd = pppsReqd->Ppfm();

	ULongPtrArray *pdrgpul = ppimReqd->PdrgpulScanIds(mp);

	CPartIndexMap *ppimResult = GPOS_NEW(mp) CPartIndexMap(mp);
	CPartFilterMap *ppfmResult = GPOS_NEW(mp) CPartFilterMap(mp);

	const ULONG ulPartIndexIds = pdrgpul->Size();
	const ULONG arity = exprhdl.UlNonScalarChildren();

	// iterate over required part index ids and decide which ones to push to the outer
	// and which to the inner side of the n-ary op
	for (ULONG ul = 0; ul < ulPartIndexIds; ul++)
	{
		ULONG part_idx_id = *((*pdrgpul)[ul]);
		GPOS_ASSERT(ppimReqd->Contains(part_idx_id));

		CBitSet *pbsPartConsumer = GPOS_NEW(mp) CBitSet(mp);
		for (ULONG ulChildIdx = 0; ulChildIdx < arity; ulChildIdx++)
		{
			if (exprhdl.DerivePartitionInfo(ulChildIdx)
					->FContainsScanId(part_idx_id))
			{
				(void) pbsPartConsumer->ExchangeSet(ulChildIdx);
			}
		}

		if (arity == pbsPartConsumer->Size() &&
			COperator::EopPhysicalSequence == exprhdl.Pop()->Eopid() &&
			(*(exprhdl.Pgexpr()))[0]->FHasCTEProducer())
		{
			GPOS_ASSERT(2 == arity);

			// this is a part index id that comes from both sides of a sequence
			// with a CTE producer on the outer side, so pretend that part index
			// id is not defined the inner sides
			pbsPartConsumer->ExchangeClear(1);
		}

		if (!FCanPushPartReqToChild(pbsPartConsumer, child_index))
		{
			// clean up
			pbsPartConsumer->Release();

			continue;
		}

		// clean up
		pbsPartConsumer->Release();

		CPartKeysArray *pdrgppartkeys =
			exprhdl.DerivePartitionInfo(child_index)
				->PdrgppartkeysByScanId(part_idx_id);
		GPOS_ASSERT(NULL != pdrgppartkeys);
		pdrgppartkeys->AddRef();

		// push requirements to child node
		ppimResult->AddRequiredPartPropagation(
			ppimReqd, part_idx_id, CPartIndexMap::EppraPreservePropagators,
			pdrgppartkeys);

		// check if there is a filter on the part index id and propagate that further down
		if (ppfmReqd->FContainsScanId(part_idx_id))
		{
			CExpression *pexpr = ppfmReqd->Pexpr(part_idx_id);
			// if the current child is inner child and the predicate is IsNull check and the parent is outer join,
			// don't push IsNull check predicate to the partition filter.
			// for all the other cases, push the filter down.
			if (!(1 == child_index && CUtils::FScalarNullTest(pexpr) &&
				  CUtils::FPhysicalOuterJoin(exprhdl.Pop())))
			{
				pexpr->AddRef();
				ppfmResult->AddPartFilter(mp, part_idx_id, pexpr,
										  NULL /*stats */);
			}
		}
	}

	pdrgpul->Release();

	return GPOS_NEW(mp) CPartitionPropagationSpec(ppimResult, ppfmResult);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::FCanPushPartReqToChild
//
//	@doc:
//		Check whether we can push a part table requirement to a given child, given
// 		the knowledge of where the part index id is defined
//
//---------------------------------------------------------------------------
BOOL
CPhysical::FCanPushPartReqToChild(CBitSet *pbsPartConsumer, ULONG child_index)
{
	GPOS_ASSERT(NULL != pbsPartConsumer);

	// if part index id comes from more that one child, we cannot push request to just one child
	if (1 < pbsPartConsumer->Size())
	{
		return false;
	}

	// child where the part index is defined should be the same child being processed
	return (pbsPartConsumer->Get(child_index));
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PppsRequiredPushThruUnresolvedUnary
//
//	@doc:
//		Helper function for pushing unresolved partition propagation in unary
//		operators
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysical::PppsRequiredPushThruUnresolvedUnary(
	CMemoryPool *mp, CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	EPropogatePartConstraint eppcPropogate, CColRefSet *filter_colrefs)
{
	GPOS_ASSERT(NULL != pppsRequired);

	CPartInfo *ppartinfo = exprhdl.DerivePartitionInfo(0);

	CPartIndexMap *ppimReqd = pppsRequired->Ppim();
	CPartFilterMap *ppfmReqd = pppsRequired->Ppfm();

	ULongPtrArray *pdrgpul = ppimReqd->PdrgpulScanIds(mp);

	CPartIndexMap *ppimResult = GPOS_NEW(mp) CPartIndexMap(mp);
	CPartFilterMap *ppfmResult = GPOS_NEW(mp) CPartFilterMap(mp);

	const ULONG ulPartIndexIds = pdrgpul->Size();

	// iterate over required part index ids and decide which ones to push through
	for (ULONG ul = 0; ul < ulPartIndexIds; ul++)
	{
		ULONG part_idx_id = *((*pdrgpul)[ul]);
		GPOS_ASSERT(ppimReqd->Contains(part_idx_id));

		// if part index id is defined in child, push it to the child
		if (ppartinfo->FContainsScanId(part_idx_id))
		{
			// push requirements to child node
			ppimResult->AddRequiredPartPropagation(
				ppimReqd, part_idx_id, CPartIndexMap::EppraPreservePropagators);
			if (CPhysical::EppcAllowed == eppcPropogate)
			{
				// for some logical operators such as limit while we push the part index map, we cannot push the constraints
				// since they are NOT semantically equivalent. So only push the constraints when the operator asks this
				// utility function to do so
				(void) ppfmResult->FCopyPartFilter(mp, part_idx_id, ppfmReqd,
												   filter_colrefs);
			}
		}
	}

	pdrgpul->Release();

	return GPOS_NEW(mp) CPartitionPropagationSpec(ppimResult, ppfmResult);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PpimDeriveCombineRelational
//
//	@doc:
//		Common case of common case of combining partition index maps
//		of all logical children
//
//---------------------------------------------------------------------------
CPartIndexMap *
CPhysical::PpimDeriveCombineRelational(CMemoryPool *mp,
									   CExpressionHandle &exprhdl)
{
	GPOS_ASSERT(0 < exprhdl.Arity());

	CPartIndexMap *ppim = GPOS_NEW(mp) CPartIndexMap(mp);
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (!exprhdl.FScalarChild(ul))
		{
			CPartIndexMap *ppimChild = exprhdl.Pdpplan(ul)->Ppim();
			GPOS_ASSERT(NULL != ppimChild);

			CPartIndexMap *ppimCombined =
				CPartIndexMap::PpimCombine(mp, *ppim, *ppimChild);
			ppim->Release();
			ppim = ppimCombined;
		}
	}

	return ppim;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PpimPassThruOuter
//
//	@doc:
//		Common case of common case of passing through partition index map
//
//---------------------------------------------------------------------------
CPartIndexMap *
CPhysical::PpimPassThruOuter(CExpressionHandle &exprhdl)
{
	CPartIndexMap *ppim = exprhdl.Pdpplan(0 /*child_index*/)->Ppim();
	GPOS_ASSERT(NULL != ppim);

	ppim->AddRef();

	return ppim;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PpfmPassThruOuter
//
//	@doc:
//		Common case of common case of passing through partition filter map
//
//---------------------------------------------------------------------------
CPartFilterMap *
CPhysical::PpfmPassThruOuter(CExpressionHandle &exprhdl)
{
	CPartFilterMap *ppfm = exprhdl.Pdpplan(0 /*child_index*/)->Ppfm();
	GPOS_ASSERT(NULL != ppfm);

	ppfm->AddRef();

	return ppfm;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PpfmDeriveCombineRelational
//
//	@doc:
//		Combine derived part filter maps of relational children
//
//---------------------------------------------------------------------------
CPartFilterMap *
CPhysical::PpfmDeriveCombineRelational(CMemoryPool *mp,
									   CExpressionHandle &exprhdl)
{
	CPartFilterMap *ppfmCombined = GPOS_NEW(mp) CPartFilterMap(mp);
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (!exprhdl.FScalarChild(ul))
		{
			CPartFilterMap *ppfm = exprhdl.Pdpplan(ul)->Ppfm();
			GPOS_ASSERT(NULL != ppfm);
			ppfmCombined->CopyPartFilterMap(mp, ppfm);
		}
	}

	return ppfmCombined;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PcmDerive
//
//	@doc:
//		Common case of combining cte maps of all logical children
//
//---------------------------------------------------------------------------
CCTEMap *
CPhysical::PcmDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
	GPOS_ASSERT(0 < exprhdl.Arity());

	CCTEMap *pcm = GPOS_NEW(mp) CCTEMap(mp);
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (!exprhdl.FScalarChild(ul))
		{
			CCTEMap *pcmChild = exprhdl.Pdpplan(ul)->GetCostModel();
			GPOS_ASSERT(NULL != pcmChild);

			CCTEMap *pcmCombined = CCTEMap::PcmCombine(mp, *pcm, *pcmChild);
			pcm->Release();
			pcm = pcmCombined;
		}
	}

	return pcm;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::FProvidesReqdCTEs
//
//	@doc:
//		Check if required CTEs are included in derived CTE map
//
//---------------------------------------------------------------------------
BOOL
CPhysical::FProvidesReqdCTEs(CExpressionHandle &exprhdl,
							 const CCTEReq *pcter) const
{
	CCTEMap *pcmDrvd = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->GetCostModel();
	GPOS_ASSERT(NULL != pcmDrvd);
	return pcmDrvd->FSatisfies(pcter);
}


CEnfdProp::EPropEnforcingType
CPhysical::EpetDistribution(CExpressionHandle &exprhdl,
							const CEnfdDistribution *ped) const
{
	GPOS_ASSERT(NULL != ped);

	// get distribution delivered by the physical node
	CDistributionSpec *pds = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pds();
	if (ped->FCompatible(pds))
	{
		// required distribution is already provided
		return CEnfdProp::EpetUnnecessary;
	}

	// required distribution will be enforced on Assert's output
	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::EpetPartitionPropagation
//
//	@doc:
//		Compute the enforcing type for the operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysical::EpetPartitionPropagation(CExpressionHandle &exprhdl,
									const CEnfdPartitionPropagation *pepp) const
{
	CPartIndexMap *ppimReqd = pepp->PppsRequired()->Ppim();
	if (!ppimReqd->FContainsUnresolved())
	{
		// no unresolved partition consumers left
		return CEnfdProp::EpetUnnecessary;
	}

	CPartIndexMap *ppimDrvd = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Ppim();
	GPOS_ASSERT(NULL != ppimDrvd);

	BOOL fInScope = pepp->FInScope(m_mp, ppimDrvd);
	BOOL fResolved = pepp->FResolved(m_mp, ppimDrvd);

	if (fResolved)
	{
		// all required partition consumers are resolved
		return CEnfdProp::EpetUnnecessary;
	}

	if (!fInScope)
	{
		// some partition consumers are not in scope of the operator: need to enforce these on top
		return CEnfdProp::EpetRequired;
	}


	// all partition resolvers are in scope of the operator: do not enforce them on top
	return CEnfdProp::EpetProhibited;
}

// Generate a singleton distribution spec request
CDistributionSpec *
CPhysical::PdsRequireSingleton(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   CDistributionSpec *pds, ULONG child_index)
{
	if (CDistributionSpec::EdtSingleton == pds->Edt())
	{
		return PdsPassThru(mp, exprhdl, pds, child_index);
	}

	return GPOS_NEW(mp) CDistributionSpecSingleton();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::GetSkew
//
//	@doc:
//		Helper to compute skew estimate based on given stats and
//		distribution spec
//
//---------------------------------------------------------------------------
CDouble
CPhysical::GetSkew(IStatistics *stats, CDistributionSpec *pds)
{
	CDouble dSkew = 1.0;
	if (CDistributionSpec::EdtHashed == pds->Edt())
	{
		CDistributionSpecHashed *pdshashed =
			CDistributionSpecHashed::PdsConvert(pds);
		const CExpressionArray *pdrgpexpr = pdshashed->Pdrgpexpr();
		const ULONG size = pdrgpexpr->Size();
		for (ULONG ul = 0; ul < size; ul++)
		{
			CExpression *pexpr = (*pdrgpexpr)[ul];
			if (COperator::EopScalarIdent == pexpr->Pop()->Eopid())
			{
				// consider only hashed distribution direct columns for now
				CScalarIdent *popScId = CScalarIdent::PopConvert(pexpr->Pop());
				ULONG colid = popScId->Pcr()->Id();
				CDouble dSkewCol = stats->GetSkew(colid);
				if (dSkewCol > dSkew)
				{
					dSkew = dSkewCol;
				}
			}
		}
	}

	return CDouble(dSkew);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::FChildrenHaveCompatibleDistributions
//
//	@doc:
//		Returns true iff the delivered distributions of the children are
//		compatible among themselves.
//
//---------------------------------------------------------------------------
BOOL
CPhysical::FCompatibleChildrenDistributions(
	const CExpressionHandle &exprhdl) const
{
	GPOS_ASSERT(exprhdl.Pop() == this);
	BOOL fSingletonOrUniversalChild = false;
	BOOL fNotSingletonOrUniversalDistributedChild = false;
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (!exprhdl.FScalarChild(ul))
		{
			CDrvdPropPlan *pdpplanChild = exprhdl.Pdpplan(ul);

			// an operator cannot have a singleton or universal distributed child
			// and one distributed on multiple nodes
			// this assumption is safe for all current operators, but it can be
			// too conservative: we could allow for instance the following cases
			// * LeftOuterJoin (universal, distributed)
			// * AntiSemiJoin  (universal, distributed)
			// These cases can be enabled if considered necessary by overriding
			// this function.
			if (CDistributionSpec::EdtUniversal == pdpplanChild->Pds()->Edt() ||
				pdpplanChild->Pds()->FSingletonOrStrictSingleton())
			{
				fSingletonOrUniversalChild = true;
			}
			else
			{
				fNotSingletonOrUniversalDistributedChild = true;
			}
			if (fSingletonOrUniversalChild &&
				fNotSingletonOrUniversalDistributedChild)
			{
				return false;
			}
		}
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysical::FUnaryUsesDefinedColumns
//
//	@doc:
//		Return true if the given column set includes any of the columns defined
//		by the unary node, as given by the handle
//
//---------------------------------------------------------------------------
BOOL
CPhysical::FUnaryUsesDefinedColumns(CColRefSet *pcrs,
									CExpressionHandle &exprhdl)
{
	GPOS_ASSERT(NULL != pcrs);
	GPOS_ASSERT(2 == exprhdl.Arity() && "Not a unary operator");

	if (0 == pcrs->Size())
	{
		return false;
	}

	return !pcrs->IsDisjoint(exprhdl.DeriveDefinedColumns(1));
}

CEnfdDistribution::EDistributionMatching
CPhysical::Edm(CReqdPropPlan *, ULONG, CDrvdPropArray *, ULONG)
{
	// by default, request distribution satisfaction
	return CEnfdDistribution::EdmSatisfy;
}

CEnfdOrder::EOrderMatching
CPhysical::Eom(CReqdPropPlan *, ULONG, CDrvdPropArray *, ULONG)
{
	// request satisfaction by default
	return CEnfdOrder::EomSatisfy;
}

CEnfdRewindability::ERewindabilityMatching
CPhysical::Erm(CReqdPropPlan *, ULONG, CDrvdPropArray *, ULONG)
{
	// request satisfaction by default
	return CEnfdRewindability::ErmSatisfy;
}