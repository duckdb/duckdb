//---------------------------------------------------------------------------
//	@filename:
//		CPhysicalSort.cpp
//
//	@doc:
//		Implementation of physical sort operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CPhysicalSort.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CCTEMap.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::CPhysicalSort
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalSort::CPhysicalSort(CMemoryPool *mp, COrderSpec *pos)
	: CPhysical(mp),
	  m_pos(pos),  // caller must add-ref pos
	  m_pcrsSort(NULL)
{
	GPOS_ASSERT(NULL != pos);

	m_pcrsSort = Pos()->PcrsUsed(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::~CPhysicalSort
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalSort::~CPhysicalSort()
{
	m_pos->Release();
	m_pcrsSort->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::Matches
//
//	@doc:
//		Match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalSort::Matches(COperator *pop) const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}

	CPhysicalSort *popSort = CPhysicalSort::PopConvert(pop);
	return m_pos->Matches(popSort->Pos());
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalSort::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
							CColRefSet *pcrsRequired, ULONG child_index,
							CDrvdPropArray *,  // pdrgpdpCtxt
							ULONG			   // ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *m_pcrsSort);
	pcrs->Union(pcrsRequired);
	CColRefSet *pcrsChildReqd =
		PcrsChildReqd(mp, exprhdl, pcrs, child_index, gpos::ulong_max);
	pcrs->Release();

	return pcrsChildReqd;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalSort::PosRequired(CMemoryPool *mp,
						   CExpressionHandle &,	 // exprhdl
						   COrderSpec *,		 // posRequired
						   ULONG
#ifdef GPOS_DEBUG
							   child_index
#endif	// GPOS_DEBUG
						   ,
						   CDrvdPropArray *,  // pdrgpdpCtxt
						   ULONG			  // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	// sort operator is order-establishing and does not require child to deliver
	// any sort order; we return an empty sort order as child requirement
	return GPOS_NEW(mp) COrderSpec(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalSort::PdsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
						   CDistributionSpec *pdsRequired, ULONG child_index,
						   CDrvdPropArray *,  // pdrgpdpCtxt
						   ULONG			  // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	return PdsPassThru(mp, exprhdl, pdsRequired, child_index);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalSort::PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
						   CRewindabilitySpec *,  //prsRequired,
						   ULONG
#ifdef GPOS_DEBUG
							   child_index
#endif	// GPOPS_DEBUG
						   ,
						   CDrvdPropArray *,  // pdrgpdpCtxt
						   ULONG			  // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	// Sort establishes rewindability on its own. It does not require motion
	// hazard handling since it is inherently blocking. However, if it contains
	// outer refs in its subtree, a Rescannable request should be sent, so that
	// an appropriate enforcer is added for any non-rescannable ops below (e.g
	// the subtree contains a Filter with outer refs on top of a Motion op, a
	// Spool op needs to be added above the Motion).
	// NB: This logic should be implemented in any materializing ops (e.g Sort & Spool)
	if (exprhdl.HasOuterRefs(0))
	{
		return GPOS_NEW(mp)
			CRewindabilitySpec(CRewindabilitySpec::ErtRescannable,
							   CRewindabilitySpec::EmhtNoMotion);
	}
	else
	{
		return GPOS_NEW(mp) CRewindabilitySpec(
			CRewindabilitySpec::ErtNone, CRewindabilitySpec::EmhtNoMotion);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalSort::PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
							CPartitionPropagationSpec *pppsRequired,
							ULONG
#ifdef GPOS_DEBUG
								child_index
#endif
							,
							CDrvdPropArray *,  //pdrgpdpCtxt,
							ULONG			   //ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);
	GPOS_ASSERT(NULL != pppsRequired);

	return CPhysical::PppsRequiredPushThruUnresolvedUnary(
		mp, exprhdl, pppsRequired, CPhysical::EppcAllowed, NULL);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalSort::PcteRequired(CMemoryPool *,		  //mp,
							CExpressionHandle &,  //exprhdl,
							CCTEReq *pcter,
							ULONG
#ifdef GPOS_DEBUG
								child_index
#endif
							,
							CDrvdPropArray *,  //pdrgpdpCtxt,
							ULONG			   //ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);
	return PcterPushThru(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalSort::FProvidesReqdCols(CExpressionHandle &exprhdl,
								 CColRefSet *pcrsRequired,
								 ULONG	// ulOptReq
) const
{
	return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalSort::PosDerive(CMemoryPool *,		  // mp
						 CExpressionHandle &  // exprhdl
) const
{
	m_pos->AddRef();
	return m_pos;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalSort::PdsDerive(CMemoryPool *,	 // mp
						 CExpressionHandle &exprhdl) const
{
	return PdsDerivePassThruOuter(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalSort::PrsDerive(CMemoryPool *mp,
						 CExpressionHandle &  // exprhdl
) const
{
	// rewindability of output is always true
	return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtMarkRestore,
										   CRewindabilitySpec::EmhtNoMotion);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalSort::EpetOrder(CExpressionHandle &,  // exprhdl
						 const CEnfdOrder *peo) const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	if (peo->FCompatible(m_pos))
	{
		// required order is already established by sort operator
		return CEnfdProp::EpetUnnecessary;
	}

	// required order is incompatible with the order established by the
	// sort operator, prohibit adding another sort operator on top
	return CEnfdProp::EpetProhibited;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalSort::EpetDistribution(CExpressionHandle & /*exprhdl*/,
								const CEnfdDistribution *
#ifdef GPOS_DEBUG
									ped
#endif	// GPOS_DEBUG
) const
{
	GPOS_ASSERT(NULL != ped);

	// distribution enforcers have already been added
	return CEnfdProp::EpetUnnecessary;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalSort::EpetRewindability(CExpressionHandle &,		 // exprhdl
								 const CEnfdRewindability *	 // per
) const
{
	// no need for enforcing rewindability on output
	return CEnfdProp::EpetUnnecessary;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalSort::OsPrint(IOstream &os) const
{
	os << SzId() << "  ";
	return Pos()->OsPrint(os);
}