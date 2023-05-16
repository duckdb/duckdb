//---------------------------------------------------------------------------
//	@filename:
//		CPhysicalPartitionSelector.cpp
//
//	@doc:
//		Implementation of physical partition selector
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CPhysicalPartitionSelector.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecAny.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CPredicateUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::CPhysicalPartitionSelector
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalPartitionSelector::CPhysicalPartitionSelector(
	CMemoryPool *mp, ULONG scan_id, IMDId *mdid, CColRef2dArray *pdrgpdrgpcr,
	UlongToPartConstraintMap *ppartcnstrmap, CPartConstraint *ppartcnstr,
	UlongToExprMap *phmulexprEqPredicates, UlongToExprMap *phmulexprPredicates,
	CExpression *pexprResidual)
	: CPhysical(mp),
	  m_scan_id(scan_id),
	  m_mdid(mdid),
	  m_pdrgpdrgpcr(pdrgpdrgpcr),
	  m_ppartcnstrmap(ppartcnstrmap),
	  m_part_constraint(ppartcnstr),
	  m_phmulexprEqPredicates(phmulexprEqPredicates),
	  m_phmulexprPredicates(phmulexprPredicates),
	  m_pexprResidual(pexprResidual)
{
	GPOS_ASSERT(0 < scan_id);
	GPOS_ASSERT(mdid->IsValid());
	GPOS_ASSERT(NULL != pdrgpdrgpcr);
	GPOS_ASSERT(0 < pdrgpdrgpcr->Size());
	GPOS_ASSERT(NULL != ppartcnstrmap);
	GPOS_ASSERT(NULL != ppartcnstr);
	GPOS_ASSERT(NULL != phmulexprEqPredicates);
	GPOS_ASSERT(NULL != phmulexprPredicates);

	m_pexprCombinedPredicate = PexprCombinedPartPred(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::CPhysicalPartitionSelector
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalPartitionSelector::CPhysicalPartitionSelector(
	CMemoryPool *mp, IMDId *mdid, UlongToExprMap *phmulexprEqPredicates)
	: CPhysical(mp),
	  m_scan_id(0),
	  m_mdid(mdid),
	  m_pdrgpdrgpcr(NULL),
	  m_ppartcnstrmap(NULL),
	  m_part_constraint(NULL),
	  m_phmulexprEqPredicates(phmulexprEqPredicates),
	  m_phmulexprPredicates(NULL),
	  m_pexprResidual(NULL),
	  m_pexprCombinedPredicate(NULL)
{
	GPOS_ASSERT(mdid->IsValid());
	GPOS_ASSERT(NULL != phmulexprEqPredicates);

	m_phmulexprPredicates = GPOS_NEW(mp) UlongToExprMap(mp);
	m_pexprCombinedPredicate = PexprCombinedPartPred(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::~CPhysicalPartitionSelector
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalPartitionSelector::~CPhysicalPartitionSelector()
{
	CRefCount::SafeRelease(m_pdrgpdrgpcr);
	CRefCount::SafeRelease(m_part_constraint);
	CRefCount::SafeRelease(m_ppartcnstrmap);
	m_phmulexprPredicates->Release();
	m_mdid->Release();
	m_phmulexprEqPredicates->Release();
	CRefCount::SafeRelease(m_pexprResidual);
	CRefCount::SafeRelease(m_pexprCombinedPredicate);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::FMatchExprMaps
//
//	@doc:
//		Check whether two expression maps match
//
//---------------------------------------------------------------------------
BOOL
CPhysicalPartitionSelector::FMatchExprMaps(UlongToExprMap *phmulexprFst,
										   UlongToExprMap *phmulexprSnd)
{
	GPOS_ASSERT(NULL != phmulexprFst);
	GPOS_ASSERT(NULL != phmulexprSnd);

	const ULONG ulEntries = phmulexprFst->Size();
	if (ulEntries != phmulexprSnd->Size())
	{
		return false;
	}

	UlongToExprMapIter hmulei(phmulexprFst);

	while (hmulei.Advance())
	{
		ULONG ulKey = *(hmulei.Key());
		const CExpression *pexprFst = hmulei.Value();
		CExpression *pexprSnd = phmulexprSnd->Find(&ulKey);
		if (!CUtils::Equals(pexprFst, pexprSnd))
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::FMatchPartCnstr
//
//	@doc:
//		Match part constraints
//
//---------------------------------------------------------------------------
BOOL
CPhysicalPartitionSelector::FMatchPartCnstr(
	UlongToPartConstraintMap *ppartcnstrmap) const
{
	if (NULL == m_ppartcnstrmap || NULL == ppartcnstrmap)
	{
		return NULL == m_ppartcnstrmap && NULL == ppartcnstrmap;
	}

	return m_ppartcnstrmap->Size() == ppartcnstrmap->Size() &&
		   FSubsetPartCnstr(ppartcnstrmap, m_ppartcnstrmap);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::FSubsetPartCnstr
//
//	@doc:
//		Check if first part constraint map is a subset of the second one
//
//---------------------------------------------------------------------------
BOOL
CPhysicalPartitionSelector::FSubsetPartCnstr(
	UlongToPartConstraintMap *ppartcnstrmapFst,
	UlongToPartConstraintMap *ppartcnstrmapSnd)
{
	GPOS_ASSERT(NULL != ppartcnstrmapFst);
	GPOS_ASSERT(NULL != ppartcnstrmapSnd);
	if (ppartcnstrmapFst->Size() > ppartcnstrmapSnd->Size())
	{
		return false;
	}

	UlongToPartConstraintMapIter partcnstriter(ppartcnstrmapFst);

	while (partcnstriter.Advance())
	{
		ULONG ulKey = *(partcnstriter.Key());
		CPartConstraint *ppartcnstr = ppartcnstrmapSnd->Find(&ulKey);

		if (NULL == ppartcnstr ||
			!partcnstriter.Value()->FEquivalent(ppartcnstr))
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::FHasFilter
//
//	@doc:
//		Check whether this operator has a partition selection filter
//
//---------------------------------------------------------------------------
BOOL
CPhysicalPartitionSelector::FHasFilter() const
{
	return (NULL != m_pexprResidual || 0 < m_phmulexprEqPredicates->Size() ||
			0 < m_phmulexprPredicates->Size());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalPartitionSelector::Matches(COperator *pop) const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}

	CPhysicalPartitionSelector *popPartSelector =
		CPhysicalPartitionSelector::PopConvert(pop);

	BOOL fScanIdCmp = popPartSelector->ScanId() == m_scan_id;
	BOOL fMdidCmp = popPartSelector->MDId()->Equals(MDId());
	BOOL fPartCnstrMapCmp = FMatchPartCnstr(popPartSelector->m_ppartcnstrmap);
	BOOL fColRefCmp =
		CColRef::Equals(popPartSelector->Pdrgpdrgpcr(), m_pdrgpdrgpcr);
	BOOL fPartCnstrEquiv =
		popPartSelector->m_part_constraint->FEquivalent(m_part_constraint);
	BOOL fEqPredCmp = FMatchExprMaps(popPartSelector->m_phmulexprEqPredicates,
									 m_phmulexprEqPredicates);
	BOOL fPredCmp = FMatchExprMaps(popPartSelector->m_phmulexprPredicates,
								   m_phmulexprPredicates);
	BOOL fResPredCmp =
		CUtils::Equals(popPartSelector->m_pexprResidual, m_pexprResidual);

	return fScanIdCmp && fMdidCmp && fPartCnstrMapCmp && fColRefCmp &&
		   fPartCnstrEquiv && fEqPredCmp && fResPredCmp && fPredCmp;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::HashValue
//
//	@doc:
//		Hash operator
//
//---------------------------------------------------------------------------
ULONG
CPhysicalPartitionSelector::HashValue() const
{
	return gpos::CombineHashes(
		Eopid(), gpos::CombineHashes(m_scan_id, MDId()->HashValue()));
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PexprEqFilter
//
//	@doc:
//		Return the equality filter expression for the given level
//
//---------------------------------------------------------------------------
CExpression *
CPhysicalPartitionSelector::PexprEqFilter(ULONG ulPartLevel) const
{
	return m_phmulexprEqPredicates->Find(&ulPartLevel);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PexprFilter
//
//	@doc:
//		Return the non-equality filter expression for the given level
//
//---------------------------------------------------------------------------
CExpression *
CPhysicalPartitionSelector::PexprFilter(ULONG ulPartLevel) const
{
	return m_phmulexprPredicates->Find(&ulPartLevel);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PexprPartPred
//
//	@doc:
//		Return the partition selection predicate for the given level
//
//---------------------------------------------------------------------------
CExpression *
CPhysicalPartitionSelector::PexprPartPred(CMemoryPool *mp,
										  ULONG ulPartLevel) const
{
	GPOS_ASSERT(ulPartLevel < UlPartLevels());

	CExpression *pexpr = PexprEqFilter(ulPartLevel);
	if (NULL != pexpr)
	{
		// we have one side of an equality predicate - need to construct the
		// whole predicate
		GPOS_ASSERT(NULL == m_phmulexprPredicates->Find(&ulPartLevel));
		pexpr->AddRef();
		if (NULL != m_pdrgpdrgpcr)
		{
			CColRef *pcrPartKey = (*(*m_pdrgpdrgpcr)[ulPartLevel])[0];
			return CUtils::PexprScalarEqCmp(mp, pcrPartKey, pexpr);
		}
		else
		{
			return pexpr;
		}
	}

	pexpr = m_phmulexprPredicates->Find(&ulPartLevel);
	if (NULL != pexpr)
	{
		pexpr->AddRef();
	}

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PexprCombinedPartPred
//
//	@doc:
//		Return a single combined partition selection predicate
//
//---------------------------------------------------------------------------
CExpression *
CPhysicalPartitionSelector::PexprCombinedPartPred(CMemoryPool *mp) const
{
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);

	const ULONG ulLevels = UlPartLevels();
	for (ULONG ul = 0; ul < ulLevels; ul++)
	{
		CExpression *pexpr = PexprPartPred(mp, ul);
		if (NULL != pexpr)
		{
			pdrgpexpr->Append(pexpr);
		}
	}

	if (NULL != m_pexprResidual)
	{
		m_pexprResidual->AddRef();
		pdrgpexpr->Append(m_pexprResidual);
	}

	return CPredicateUtils::PexprConjunction(mp, pdrgpexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::UlPartLevels
//
//	@doc:
//		Number of partitioning levels
//
//---------------------------------------------------------------------------
ULONG
CPhysicalPartitionSelector::UlPartLevels() const
{
	if (NULL != m_pdrgpdrgpcr)
	{
		return m_pdrgpdrgpcr->Size();
	}

	return m_phmulexprEqPredicates->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PpfmDerive
//
//	@doc:
//		Derive partition filter map
//
//---------------------------------------------------------------------------
CPartFilterMap *
CPhysicalPartitionSelector::PpfmDerive(CMemoryPool *mp,
									   CExpressionHandle &exprhdl) const
{
	if (!FHasFilter())
	{
		return PpfmPassThruOuter(exprhdl);
	}

	CPartFilterMap *ppfm = PpfmDeriveCombineRelational(mp, exprhdl);
	IStatistics *stats = exprhdl.Pstats();
	GPOS_ASSERT(NULL != stats);
	m_pexprCombinedPredicate->AddRef();
	stats->AddRef();
	ppfm->AddPartFilter(mp, m_scan_id, m_pexprCombinedPredicate, stats);
	return ppfm;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalPartitionSelector::PcrsRequired(CMemoryPool *mp,
										 CExpressionHandle &exprhdl,
										 CColRefSet *pcrsInput,
										 ULONG child_index,
										 CDrvdPropArray *,	// pdrgpdpCtxt
										 ULONG				// ulOptReq
)
{
	GPOS_ASSERT(
		0 == child_index &&
		"Required properties can only be computed on the relational child");

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *pcrsInput);
	pcrs->Union(m_pexprCombinedPredicate->DeriveUsedColumns());
	pcrs->Intersection(exprhdl.DeriveOutputColumns(child_index));

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalPartitionSelector::PosRequired(CMemoryPool *mp,
										CExpressionHandle &exprhdl,
										COrderSpec *posRequired,
										ULONG child_index,
										CDrvdPropArray *,  // pdrgpdpCtxt
										ULONG			   // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	return PosPassThru(mp, exprhdl, posRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalPartitionSelector::PdsRequired(CMemoryPool *mp,
										CExpressionHandle &exprhdl,
										CDistributionSpec *pdsInput,
										ULONG child_index,
										CDrvdPropArray *,  // pdrgpdpCtxt
										ULONG			   // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	CPartInfo *ppartinfo = exprhdl.DerivePartitionInfo();
	BOOL fCovered = ppartinfo->FContainsScanId(m_scan_id);

	if (fCovered)
	{
		// if partition consumer is defined below, do not pass distribution
		// requirements down as this will cause the consumer and enforcer to be
		// in separate slices
		return GPOS_NEW(mp) CDistributionSpecAny(this->Eopid());
	}

	return PdsPassThru(mp, exprhdl, pdsInput, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalPartitionSelector::PrsRequired(CMemoryPool *mp,
										CExpressionHandle &exprhdl,
										CRewindabilitySpec *prsRequired,
										ULONG child_index,
										CDrvdPropArray *,  // pdrgpdpCtxt
										ULONG			   // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	return PrsPassThru(mp, exprhdl, prsRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalPartitionSelector::PppsRequired(
	CMemoryPool *mp, CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG
#ifdef GPOS_DEBUG
		child_index
#endif	// GPOS_DEBUG
	,
	CDrvdPropArray *,  //pdrgpdpCtxt,
	ULONG			   //ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);
	GPOS_ASSERT(NULL != pppsRequired);

	CPartIndexMap *ppimInput = pppsRequired->Ppim();
	CPartFilterMap *ppfmInput = pppsRequired->Ppfm();

	ULongPtrArray *pdrgpulInputScanIds = ppimInput->PdrgpulScanIds(mp);

	CPartIndexMap *ppim = GPOS_NEW(mp) CPartIndexMap(mp);
	CPartFilterMap *ppfm = GPOS_NEW(mp) CPartFilterMap(mp);

	CPartInfo *ppartinfo = exprhdl.DerivePartitionInfo(0);

	const ULONG ulScanIds = pdrgpulInputScanIds->Size();

	for (ULONG ul = 0; ul < ulScanIds; ul++)
	{
		ULONG scan_id = *((*pdrgpulInputScanIds)[ul]);
		ULONG ulExpectedPropagators = ppimInput->UlExpectedPropagators(scan_id);

		if (scan_id == m_scan_id)
		{
			// partition propagation resolved - do not need to require from children
			continue;
		}

		if (!ppartinfo->FContainsScanId(scan_id) &&
			ppartinfo->FContainsScanId(m_scan_id))
		{
			// dynamic scan for the required id not defined below, but the current one is: do not push request down
			continue;
		}

		IMDId *mdid = ppimInput->GetRelMdId(scan_id);
		CPartKeysArray *pdrgppartkeys = ppimInput->Pdrgppartkeys(scan_id);
		UlongToPartConstraintMap *ppartcnstrmap =
			ppimInput->Ppartcnstrmap(scan_id);
		CPartConstraint *ppartcnstr = ppimInput->PpartcnstrRel(scan_id);
		CPartIndexMap::EPartIndexManipulator epim = ppimInput->Epim(scan_id);
		mdid->AddRef();
		pdrgppartkeys->AddRef();
		ppartcnstrmap->AddRef();
		ppartcnstr->AddRef();

		ppim->Insert(scan_id, ppartcnstrmap, epim, ulExpectedPropagators, mdid,
					 pdrgppartkeys, ppartcnstr);
		(void) ppfm->FCopyPartFilter(m_mp, scan_id, ppfmInput, NULL);
	}

	// cleanup
	pdrgpulInputScanIds->Release();

	return GPOS_NEW(mp) CPartitionPropagationSpec(ppim, ppfm);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalPartitionSelector::PcteRequired(CMemoryPool *,		   //mp,
										 CExpressionHandle &,  //exprhdl,
										 CCTEReq *pcter,
										 ULONG
#ifdef GPOS_DEBUG
											 child_index
#endif
										 ,
										 CDrvdPropArray *,	//pdrgpdpCtxt,
										 ULONG				//ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);
	return PcterPushThru(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalPartitionSelector::FProvidesReqdCols(CExpressionHandle &exprhdl,
											  CColRefSet *pcrsRequired,
											  ULONG	 // ulOptReq
) const
{
	return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalPartitionSelector::PosDerive(CMemoryPool *,  // mp
									  CExpressionHandle &exprhdl) const
{
	return PosDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalPartitionSelector::PdsDerive(CMemoryPool *,  // mp
									  CExpressionHandle &exprhdl) const
{
	return PdsDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PpimDerive
//
//	@doc:
//		Derive partition index map
//
//---------------------------------------------------------------------------
CPartIndexMap *
CPhysicalPartitionSelector::PpimDerive(CMemoryPool *mp,
									   CExpressionHandle &exprhdl,
									   CDrvdPropCtxt *pdpctxt) const
{
	GPOS_ASSERT(NULL != pdpctxt);

	CDrvdPropPlan *pdpplan = exprhdl.Pdpplan(0 /*child_index*/);
	CPartIndexMap *ppimInput = pdpplan->Ppim();
	GPOS_ASSERT(NULL != ppimInput);

	ULONG ulExpectedPartitionSelectors =
		CDrvdPropCtxtPlan::PdpctxtplanConvert(pdpctxt)
			->UlExpectedPartitionSelectors();

	CPartIndexMap *ppim = ppimInput->PpimPartitionSelector(
		mp, m_scan_id, ulExpectedPartitionSelectors);
	if (!ppim->Contains(m_scan_id))
	{
		// the consumer of this scan id does not come from the child, i.e. it
		// is on the other side of a join
		MDId()->AddRef();
		m_pdrgpdrgpcr->AddRef();
		m_ppartcnstrmap->AddRef();
		m_part_constraint->AddRef();

		CPartKeysArray *pdrgppartkeys = GPOS_NEW(mp) CPartKeysArray(mp);
		pdrgppartkeys->Append(GPOS_NEW(mp) CPartKeys(m_pdrgpdrgpcr));

		ppim->Insert(m_scan_id, m_ppartcnstrmap, CPartIndexMap::EpimPropagator,
					 0 /*ulExpectedPropagators*/, MDId(), pdrgppartkeys,
					 m_part_constraint);
	}

	return ppim;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalPartitionSelector::PrsDerive(CMemoryPool *mp,
									  CExpressionHandle &exprhdl) const
{
	CPartInfo *ppartinfo = exprhdl.DerivePartitionInfo(0);
	BOOL staticPartitionSelector = ppartinfo->FContainsScanId(this->ScanId());
	if (!staticPartitionSelector)
	{
		// Currently the executor function ExecRescanPartitionSelector() expects
		// that a dynamic partition selector is not rescannable. So, prevent
		// Orca from picking such a plan.
		CRewindabilitySpec *prs = exprhdl.Pdpplan(0 /*child_index*/)->Prs();
		return GPOS_NEW(mp)
			CRewindabilitySpec(CRewindabilitySpec::ErtNone, prs->Emht());
	}
	return PrsDerivePassThruOuter(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalPartitionSelector::EpetDistribution(CExpressionHandle &exprhdl,
											 const CEnfdDistribution *ped) const
{
	CDrvdPropPlan *pdpplan = exprhdl.Pdpplan(0 /* child_index */);

	if (ped->FCompatible(pdpplan->Pds()))
	{
		// required distribution established by the operator
		return CEnfdProp::EpetUnnecessary;
	}

	CPartIndexMap *ppimDrvd = pdpplan->Ppim();
	if (!ppimDrvd->Contains(m_scan_id))
	{
		// part consumer is defined above: prohibit adding a motion on top of the
		// part resolver as this will create two slices
		return CEnfdProp::EpetProhibited;
	}

	GPOS_ASSERT(CPartIndexMap::EpimConsumer == ppimDrvd->Epim(m_scan_id));

	// part consumer found below: enforce distribution on top of part resolver
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalPartitionSelector::EpetRewindability(
	CExpressionHandle &exprhdl, const CEnfdRewindability *per) const
{
	// get rewindability delivered by the node
	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();
	if (per->FCompatible(prs))
	{
		// required rewindability is already provided
		return CEnfdProp::EpetUnnecessary;
	}

	// always force spool to be on top of filter
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalPartitionSelector::EpetOrder(CExpressionHandle &,	// exprhdl,
									  const CEnfdOrder *	// ped
) const
{
	return CEnfdProp::EpetOptional;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalPartitionSelector::OsPrint(IOstream &os) const
{
	os << SzId() << ", Scan Id: " << m_scan_id << ", Part Table: ";
	MDId()->OsPrint(os);

	return os;
}